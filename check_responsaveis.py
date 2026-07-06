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
NOTION_VERSION = "2022-06-28"  # ajuste para a versão que seu script já usa
DIAS_A_FRENTE = 30  # janela de verificação
NOME_PROPRIEDADE_DATA = "Veiculação"

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


def notion_query_database(database_id: str, base_payload: dict | None = None) -> list[dict]:
    """Query genérica com paginação — usada tanto pro calendário quanto pela equipe."""
    paginas = []
    payload = dict(base_payload or {})
    while True:
        resp = requests.post(
            f"https://api.notion.com/v1/databases/{database_id}/query",
            headers=HEADERS,
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()
        paginas.extend(data["results"])
        if not data.get("has_more"):
            break
        payload["start_cursor"] = data["next_cursor"]
    return paginas


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
    return notion_query_database(DATABASE_ID, payload)


def extrair_pessoas_por_papel(pagina: dict) -> dict[str, list[str]]:
    """Retorna {papel: [ids ordenados]} para cada uma das PROPRIEDADES_PESSOAS."""
    resultado = {}
    for papel in PROPRIEDADES_PESSOAS:
        prop = pagina["properties"].get(papel, {})
        pessoas = prop.get("people", [])
        resultado[papel] = sorted(p["id"] for p in pessoas)
    return resultado


# =========================
# EQUIPE | GCMD (People -> email) — adaptado do script de recorrência
# =========================
def load_team_user_map() -> dict[str, str]:
    pages = notion_query_database(DATABASE_ID_EQUIPE_GCMD, {"page_size": 100})
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
