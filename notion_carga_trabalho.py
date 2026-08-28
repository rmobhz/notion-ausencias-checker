"""
Popula a base "Carga de trabalho" no Notion com base em:
  - "Equipe | GCMD"  -> pessoas ativas + usuário Notion (people)
  - "Tarefas GCMD"   -> tarefas com propriedade "Responsável" (people) e "Status"

Para cada pessoa ATIVA em "Equipe | GCMD" com "Usuário no Notion" preenchido:
  1. Conta as tarefas em "Tarefas GCMD" cujo "Responsável" é esse usuário e
     cujo "Status" é diferente de "Concluído" -> "Tarefas abertas".
  2. Soma a propriedade "Peso" dessas mesmas tarefas -> "Peso total"
     (funciona com "Peso" do tipo number, formula ou rollup).
  3. Se já existir uma página em "Carga de trabalho" relacionada a essa
     pessoa (via a propriedade de relação "Equipe | GCMD"), atualiza os
     dois números. Se não existir, CRIA a página (Nome + relação + números).

Pessoas inativas (Status != "Ativo") são ignoradas.

"Carga" e "Situação" são fórmulas/rollups no próprio Notion e não são
escritas pelo script.

Variáveis de ambiente esperadas:
  NOTION_API_KEY                 - token de integração interna do Notion
  DATABASE_ID_EQUIPE_GCMD       - ID da base "Equipe | GCMD"
  DATABASE_ID_TAREFAS_GCMD      - ID da base "Tarefas GCMD"
  DATABASE_ID_CARGA_TRABALHO    - ID da base "Carga de trabalho"

Nomes de propriedades (ajuste aqui se algo mudar no Notion):
"""

import os
import sys
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import requests

NOTION_API_KEY = os.environ["NOTION_API_KEY"]
EQUIPE_DB_ID = os.environ["DATABASE_ID_EQUIPE_GCMD"]
TAREFAS_DB_ID = os.environ["DATABASE_ID_TAREFAS_GCMD"]
CARGA_DB_ID = os.environ["DATABASE_ID_CARGA_TRABALHO"]

NOTION_VERSION = "2022-06-28"
BASE_URL = "https://api.notion.com/v1"
HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# --- nomes de propriedades ---------------------------------------------
EQUIPE_PROP_NOME = "Nome"                          # title, na base Equipe | GCMD
EQUIPE_PROP_STATUS = "Status"                      # status/select, na base Equipe | GCMD
EQUIPE_PROP_USUARIO_NOTION = "Usuário no Notion"   # people, na base Equipe | GCMD
STATUS_ATIVO = "Ativo"

CARGA_PROP_NOME = "Nome"                           # title, na base Carga de trabalho
CARGA_PROP_RELACAO_EQUIPE = "Equipe | GCMD"        # relation, na base Carga de trabalho
CARGA_PROP_TAREFAS_ABERTAS = "Tarefas abertas"     # number
CARGA_PROP_PESO_TOTAL = "Peso total"               # number

TAREFAS_PROP_RESPONSAVEL = "Responsável"           # people, na base Tarefas GCMD
TAREFAS_PROP_STATUS = "Status"                     # status/select
TAREFAS_PROP_PESO = "Peso"                          # number, formula ou rollup
STATUS_CONCLUIDO = "Concluído"

TIMEZONE = "America/Sao_Paulo"

MAX_RETRIES = 3


def _request(method: str, path: str, **kwargs) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.request(method, url, headers=HEADERS, **kwargs)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", "1"))
            time.sleep(wait)
            continue
        if resp.status_code >= 500 and attempt < MAX_RETRIES:
            time.sleep(2 * attempt)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()
    return resp.json()


def query_database_all(database_id: str, payload: dict[str, Any] | None = None) -> list[dict]:
    """Retorna todas as páginas de uma base, paginando automaticamente."""
    payload = dict(payload or {})
    results: list[dict] = []
    while True:
        data = _request("POST", f"/databases/{database_id}/query", json=payload)
        results.extend(data["results"])
        if not data.get("has_more"):
            break
        payload["start_cursor"] = data["next_cursor"]
    return results


def get_relation_ids(page: dict, prop_name: str) -> list[str]:
    prop = page["properties"].get(prop_name, {})
    return [item["id"] for item in prop.get("relation", [])]


def get_people_ids(page: dict, prop_name: str) -> list[str]:
    prop = page["properties"].get(prop_name, {})
    return [person["id"] for person in prop.get("people", [])]


def get_status_name(page: dict, prop_name: str) -> str | None:
    prop = page["properties"].get(prop_name, {})
    if prop.get("type") == "status" and prop.get("status"):
        return prop["status"]["name"]
    if prop.get("type") == "select" and prop.get("select"):
        return prop["select"]["name"]
    return None


def get_title(page: dict, prop_name: str, fallback: str = "") -> str:
    prop = page["properties"].get(prop_name, {})
    title = prop.get("title", [])
    return title[0]["plain_text"] if title else fallback


def get_number(page: dict, prop_name: str) -> float:
    """Lê um número mesmo quando a propriedade é number, formula ou rollup."""
    prop = page["properties"].get(prop_name, {})
    ptype = prop.get("type")

    if ptype == "number":
        return prop.get("number") or 0

    if ptype == "formula":
        formula = prop.get("formula") or {}
        if formula.get("type") == "number":
            return formula.get("number") or 0
        return 0

    if ptype == "rollup":
        rollup = prop.get("rollup") or {}
        if rollup.get("type") == "number":
            return rollup.get("number") or 0
        return 0

    return 0


def build_equipe_records() -> list[dict[str, Any]]:
    """Lista de pessoas ATIVAS em Equipe | GCMD com usuário Notion vinculado."""
    pages = query_database_all(EQUIPE_DB_ID)
    records = []
    for page in pages:
        status = get_status_name(page, EQUIPE_PROP_STATUS)
        if status != STATUS_ATIVO:
            continue
        user_ids = get_people_ids(page, EQUIPE_PROP_USUARIO_NOTION)
        records.append(
            {
                "page_id": page["id"],
                "nome": get_title(page, EQUIPE_PROP_NOME, fallback=page["id"]),
                "notion_user_id": user_ids[0] if user_ids else None,
            }
        )
    return records


def build_carga_by_equipe_id() -> dict[str, str]:
    """equipe_page_id -> carga_page_id, a partir das páginas já existentes em Carga de trabalho."""
    pages = query_database_all(CARGA_DB_ID)
    mapping: dict[str, str] = {}
    for page in pages:
        equipe_ids = get_relation_ids(page, CARGA_PROP_RELACAO_EQUIPE)
        if equipe_ids:
            mapping[equipe_ids[0]] = page["id"]
    return mapping


def count_open_tasks(notion_user_id: str) -> tuple[int, float]:
    """Retorna (tarefas_abertas, peso_total) para um usuário Notion."""
    payload = {
        "filter": {
            "and": [
                {
                    "property": TAREFAS_PROP_RESPONSAVEL,
                    "people": {"contains": notion_user_id},
                },
                {
                    "property": TAREFAS_PROP_STATUS,
                    "status": {"does_not_equal": STATUS_CONCLUIDO},
                },
            ]
        }
    }
    tasks = query_database_all(TAREFAS_DB_ID, payload)
    total_peso = sum(get_number(t, TAREFAS_PROP_PESO) for t in tasks)
    return len(tasks), total_peso


def update_carga_page(page_id: str, tarefas_abertas: int, peso_total: float) -> None:
    payload = {
        "properties": {
            CARGA_PROP_TAREFAS_ABERTAS: {"number": tarefas_abertas},
            CARGA_PROP_PESO_TOTAL: {"number": peso_total},
        }
    }
    _request("PATCH", f"/pages/{page_id}", json=payload)


def create_carga_page(equipe_page_id: str, nome: str, tarefas_abertas: int, peso_total: float) -> str:
    payload = {
        "parent": {"database_id": CARGA_DB_ID},
        "properties": {
            CARGA_PROP_NOME: {"title": [{"type": "text", "text": {"content": nome}}]},
            CARGA_PROP_RELACAO_EQUIPE: {"relation": [{"id": equipe_page_id}]},
            CARGA_PROP_TAREFAS_ABERTAS: {"number": tarefas_abertas},
            CARGA_PROP_PESO_TOTAL: {"number": peso_total},
        },
    }
    data = _request("POST", "/pages", json=payload)
    return data["id"]


def notion_page_url(page_id: str) -> str:
    return f"https://www.notion.so/{page_id.replace('-', '')}"


def update_database_description(last_run_str: str) -> None:
    """Reescreve a descrição da base 'Carga de trabalho' com um timestamp da última execução,
    mantendo links clicáveis para as bases de origem."""
    payload = {
        "description": [
            {
                "type": "text",
                "text": {
                    "content": "Dados automáticos, atualizados 2x ao dia via script externo "
                    "a partir das bases "
                },
            },
            {
                "type": "text",
                "text": {
                    "content": "Equipe GCMD",
                    "link": {"url": notion_page_url(EQUIPE_DB_ID)},
                },
                "annotations": {"color": "brown_background"},
            },
            {"type": "text", "text": {"content": " e "}},
            {
                "type": "text",
                "text": {
                    "content": "Tarefas GCMD",
                    "link": {"url": notion_page_url(TAREFAS_DB_ID)},
                },
                "annotations": {"color": "brown_background"},
            },
            {
                "type": "text",
                "text": {"content": f".\nÚltima atualização: {last_run_str}."},
            },
        ]
    }
    _request("PATCH", f"/databases/{CARGA_DB_ID}", json=payload)


def main() -> int:
    print("Carregando pessoas ativas de Equipe | GCMD...")
    equipe_records = build_equipe_records()
    print(f"  {len(equipe_records)} pessoas ativas.")

    print("Carregando páginas existentes de Carga de trabalho...")
    carga_by_equipe_id = build_carga_by_equipe_id()
    print(f"  {len(carga_by_equipe_id)} páginas já existentes.")

    updated, created, skipped = 0, 0, 0
    for record in equipe_records:
        nome = record["nome"]
        notion_user_id = record["notion_user_id"]

        if not notion_user_id:
            print(f"  [pular] {nome}: sem 'Usuário no Notion' preenchido")
            skipped += 1
            continue

        tarefas_abertas, peso_total = count_open_tasks(notion_user_id)
        carga_page_id = carga_by_equipe_id.get(record["page_id"])

        if carga_page_id:
            update_carga_page(carga_page_id, tarefas_abertas, peso_total)
            print(f"  [ok] {nome}: {tarefas_abertas} tarefas abertas, peso total {peso_total}")
            updated += 1
        else:
            new_id = create_carga_page(record["page_id"], nome, tarefas_abertas, peso_total)
            print(f"  [criado] {nome} ({new_id}): {tarefas_abertas} tarefas abertas, peso total {peso_total}")
            created += 1

    print(f"Concluído: {updated} atualizadas, {created} criadas, {skipped} puladas.")

    now = datetime.now(ZoneInfo(TIMEZONE))
    last_run_str = now.strftime("%d/%m/%Y às %Hh%M")
    update_database_description(last_run_str)
    print(f"Descrição da base atualizada: última atualização {last_run_str}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
