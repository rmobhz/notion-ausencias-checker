import json
import os
from datetime import date, timedelta
import requests

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
DATABASE_ID_EQUIPE_GCMD = os.getenv("DATABASE_ID_EQUIPE_GCMD")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
GIST_ID = os.environ["SNAPSHOT_GIST_ID"]
GITHUB_TOKEN = os.environ["GH_TOKEN_GIST"]  # token com escopo 'gist', separado do GITHUB_TOKEN padrão
SNAPSHOT_FILENAME = "snapshot_responsaveis.json"

# A partir de 2025-09-03 o Notion separou "database" (o contêiner) de
# "data source" (onde as linhas realmente vivem). Databases que ganharam
# mais de uma fonte de dados passam a rejeitar consultas feitas no
# endpoint antigo /v1/databases/{id}/query com 400 Bad Request — foi
# exatamente isso que começou a acontecer aqui. A partir desta versão,
# toda consulta precisa ir para /v1/data_sources/{data_source_id}/query.
NOTION_VERSION = "2025-09-03"

DIAS_A_FRENTE = 30  # janela de verificação
NOME_PROPRIEDADE_DATA = "Veiculação inst"

# As 3 propriedades tipo Pessoa a monitorar no Calendário Editorial
PROPRIEDADES_PESSOAS = ["Responsável", "Apoio", "Editor(a) imagem/vídeo"]

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

GIST_HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
}

SLACK_HEADERS = {
    "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
    "Content-Type": "application/json; charset=utf-8",
}

_DATA_SOURCE_CACHE: dict[str, str] = {}


def checar_resposta_notion(resp: requests.Response) -> None:
    """Como raise_for_status() sozinho esconde o motivo, imprime o corpo
    de erro que o Notion sempre devolve antes de propagar a exceção."""
    if not resp.ok:
        try:
            detalhe = resp.json()
        except ValueError:
            detalhe = resp.text
        print(f"Erro Notion {resp.status_code} em {resp.url}: {detalhe}")
    resp.raise_for_status()


def carregar_snapshot() -> dict:
    """Lê o snapshot salvo no Gist secreto (não no repositório)."""
    resp = requests.get(f"https://api.github.com/gists/{GIST_ID}", headers=GIST_HEADERS)
    resp.raise_for_status()
    arquivos = resp.json()["files"]
    if SNAPSHOT_FILENAME not in arquivos:
        return {}
    conteudo = arquivos[SNAPSHOT_FILENAME]["content"]
    return json.loads(conteudo) if conteudo.strip() else {}


def salvar_snapshot(snapshot: dict) -> None:
    """Sobrescreve o conteúdo do Gist secreto com o novo snapshot."""
    payload = {
        "files": {
            SNAPSHOT_FILENAME: {
                "content": json.dumps(snapshot, indent=2, ensure_ascii=False)
            }
        }
    }
    resp = requests.patch(
        f"https://api.github.com/gists/{GIST_ID}", headers=GIST_HEADERS, json=payload
    )
    resp.raise_for_status()


def obter_data_source_id(database_id: str) -> str:
    """
    Resolve o data_source_id de uma database (necessário desde a API
    2025-09-03). Resultado é cacheado em memória pra não repetir a
    chamada várias vezes na mesma execução.
    """
    if database_id in _DATA_SOURCE_CACHE:
        return _DATA_SOURCE_CACHE[database_id]

    resp = requests.get(
        f"https://api.notion.com/v1/databases/{database_id}", headers=HEADERS
    )
    checar_resposta_notion(resp)
    data_sources = resp.json().get("data_sources", [])
    if not data_sources:
        raise RuntimeError(f"Nenhuma data source encontrada para a database {database_id}")
    if len(data_sources) > 1:
        nomes = ", ".join(ds.get("name", "(sem nome)") for ds in data_sources)
        print(
            f"Aviso: a database {database_id} tem múltiplas data sources ({nomes}); "
            f"usando a primeira: '{data_sources[0].get('name')}'."
        )

    data_source_id = data_sources[0]["id"]
    _DATA_SOURCE_CACHE[database_id] = data_source_id
    return data_source_id


def notion_query_data_source(data_source_id: str, base_payload: dict | None = None) -> list[dict]:
    """Query genérica com paginação sobre uma data source."""
    paginas = []
    payload = dict(base_payload or {})
    while True:
        resp = requests.post(
            f"https://api.notion.com/v1/data_sources/{data_source_id}/query",
            headers=HEADERS,
            json=payload,
        )
        checar_resposta_notion(resp)
        data = resp.json()
        paginas.extend(data["results"])
        if not data.get("has_more"):
            break
        payload["start_cursor"] = data["next_cursor"]
    return paginas


def obter_schema_data_source(data_source_id: str) -> dict:
    resp = requests.get(
        f"https://api.notion.com/v1/data_sources/{data_source_id}", headers=HEADERS
    )
    checar_resposta_notion(resp)
    return resp.json().get("properties", {})


def validar_propriedades(data_source_id: str, nomes_esperados: list[str]) -> None:
    """
    Confere, antes de consultar, que todas as propriedades esperadas ainda
    existem com esse nome exato na data source. Evita tanto o 400 (query
    com filtro numa propriedade inexistente) quanto um problema mais sutil:
    se uma das PROPRIEDADES_PESSOAS for renomeada, o código original não
    quebraria — ele simplesmente leria "ninguém está nesse papel" pra toda
    página, e isso dispararia notificação de remoção falsa para todo mundo
    que estava lá antes da renomeação. Melhor falhar alto e claro aqui.
    """
    schema = obter_schema_data_source(data_source_id)
    faltando = [n for n in nomes_esperados if n not in schema]
    if faltando:
        disponiveis = ", ".join(sorted(schema.keys()))
        raise RuntimeError(
            f"Propriedade(s) não encontrada(s) na data source {data_source_id}: "
            f"{faltando}. Disponíveis: {disponiveis}"
        )


def buscar_paginas_calendario() -> list[dict]:
    """
    Busca páginas do Calendário Editorial cuja 'Veiculação' esteja entre
    hoje e hoje + DIAS_A_FRENTE.

    IMPORTANTE: main() só compara/notifica remoção para páginas retornadas
    aqui. Uma tarefa que sai da janela só porque o tempo passou (sem edição
    real das propriedades de pessoas) simplesmente não aparece mais nesta
    lista — não entra no loop de comparação, então não gera notificação,
    apenas some do snapshot na próxima gravação. Não trocar essa lógica por
    uma comparação via união de chaves (antigo ∪ novo): isso reintroduziria
    o falso positivo.
    """
    hoje = date.today().isoformat()
    limite = (date.today() + timedelta(days=DIAS_A_FRENTE)).isoformat()
    payload = {
        "filter": {
            "and": [
                {"property": NOME_PROPRIEDADE_DATA, "date": {"on_or_after": hoje}},
                {"property": NOME_PROPRIEDADE_DATA, "date": {"on_or_before": limite}},
            ]
        }
    }
    data_source_id = obter_data_source_id(DATABASE_ID)
    return notion_query_data_source(data_source_id, payload)


def extrair_pessoas_por_papel(pagina: dict) -> dict[str, list[str]]:
    """Retorna {papel: [ids ordenados]} para cada uma das PROPRIEDADES_PESSOAS."""
    resultado = {}
    for papel in PROPRIEDADES_PESSOAS:
        prop = pagina["properties"].get(papel, {})
        pessoas = prop.get("people", [])
        resultado[papel] = sorted(p["id"] for p in pessoas)
    return resultado


# =========================
# EQUIPE | GCMD (People -> email)
# =========================
def load_team_user_map() -> dict[str, str]:
    data_source_id = obter_data_source_id(DATABASE_ID_EQUIPE_GCMD)
    pages = notion_query_data_source(data_source_id, {"page_size": 100})
    user_map = {}
    for p in pages:
        people_prop = p.get("properties", {}).get("Usuário no Notion")
        email_prop = p.get("properties", {}).get("E-mail")
        if not people_prop or people_prop.get("type") != "people":
            continue
        if not email_prop or email_prop.get("type") != "email":
            continue
        email = email_prop.get("email")
        if not email:
            continue
        for person in people_prop.get("people", []):
            uid = person.get("id")
            if uid:
                user_map[uid] = email.lower()
    return user_map


def resolver_slack_id(email: str, cache: dict[str, str | None]) -> str | None:
    """Resolve um e-mail para o member ID do Slack via users.lookupByEmail, com cache."""
    if email in cache:
        return cache[email]
    resp = requests.get(
        "https://slack.com/api/users.lookupByEmail",
        headers=SLACK_HEADERS,
        params={"email": email},
    )
    resp.raise_for_status()
    data = resp.json()
    slack_id = data["user"]["id"] if data.get("ok") else None
    if not data.get("ok"):
        print(f"Falha ao resolver e-mail {email} no Slack: {data.get('error')}")
    cache[email] = slack_id
    return slack_id


def notificar_remocao_slack(slack_id: str, titulo: str, papel: str, url_pagina: str) -> None:
    """Envia uma DM no Slack para a pessoa removida, linkando a tarefa."""
    body = {
        "channel": slack_id,
        "text": (
            f'Opa! 👋 Notei uma mudança no Calendário Editorial: você foi removido(a) de '
            f'*"{papel}"* em <{url_pagina}|{titulo}>.'
        ),
    }
    resp = requests.post(
        "https://slack.com/api/chat.postMessage", headers=SLACK_HEADERS, json=body
    )
    resp.raise_for_status()
    resultado = resp.json()
    if not resultado.get("ok"):
        print(f"Falha ao enviar Slack para {slack_id}: {resultado.get('error')}")


def titulo_da_pagina(pagina: dict) -> str:
    for prop in pagina["properties"].values():
        if prop["type"] == "title" and prop["title"]:
            return "".join(t["plain_text"] for t in prop["title"])
    return "(sem título)"


def main() -> None:
    data_source_calendario = obter_data_source_id(DATABASE_ID)
    validar_propriedades(
        data_source_calendario, [NOME_PROPRIEDADE_DATA] + PROPRIEDADES_PESSOAS
    )

    snapshot_antigo = carregar_snapshot()
    snapshot_novo = {}

    notion_para_email = load_team_user_map()
    cache_slack: dict[str, str | None] = {}

    for pagina in buscar_paginas_calendario():
        page_id = pagina["id"]
        atual = extrair_pessoas_por_papel(pagina)
        anterior = snapshot_antigo.get(page_id, {})

        titulo = None  # calculado só se precisar, para economizar chamadas
        for papel, lista_atual in atual.items():
            lista_anterior = anterior.get(papel, [])
            removidos = set(lista_anterior) - set(lista_atual)
            if not removidos:
                continue

            if titulo is None:
                titulo = titulo_da_pagina(pagina)

            for user_id in removidos:
                email = notion_para_email.get(user_id)
                if not email:
                    print(f"Sem e-mail mapeado para usuário Notion {user_id} — pulando aviso.")
                    continue
                slack_id = resolver_slack_id(email, cache_slack)
                if not slack_id:
                    print(f"Sem Slack ID para {email} — pulando aviso.")
                    continue
                notificar_remocao_slack(slack_id, titulo, papel, pagina["url"])
                print(f"Notificado via Slack: '{titulo}' / {papel} -> {email}")

        snapshot_novo[page_id] = atual

    salvar_snapshot(snapshot_novo)


if __name__ == "__main__":
    main()
