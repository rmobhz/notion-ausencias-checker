"""
Popula a base "Carga de trabalho" no Notion com base em:
  - "Equipe | GCMD"  -> mapeia página da pessoa <-> usuário Notion (people)
  - "Tarefas GCMD"   -> tarefas com propriedade "Responsável" (people) e "Status"

Para cada página já existente em "Carga de trabalho" (relacionada a uma pessoa
em "Equipe | GCMD" via a propriedade de relação "Equipe | GCMD"):
  1. Descobre o usuário Notion correspondente à pessoa.
  2. Conta as tarefas em "Tarefas GCMD" cujo "Responsável" é esse usuário e
     cujo "Status" é diferente de "Concluído" -> "Tarefas abertas".
  3. Soma a propriedade numérica "Peso" dessas mesmas tarefas -> "Peso total".
  4. Atualiza a página em "Carga de trabalho" com os dois números.

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
from typing import Any

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
EQUIPE_PROP_USUARIO_NOTION = "Usuário no Notion"   # people, na base Equipe | GCMD

CARGA_PROP_RELACAO_EQUIPE = "Equipe | GCMD"        # relation, na base Carga de trabalho
CARGA_PROP_TAREFAS_ABERTAS = "Tarefas abertas"     # number
CARGA_PROP_PESO_TOTAL = "Peso total"               # number

TAREFAS_PROP_RESPONSAVEL = "Responsável"           # people, na base Tarefas GCMD
TAREFAS_PROP_STATUS = "Status"                     # status/select
TAREFAS_PROP_PESO = "Peso"                          # number
STATUS_CONCLUIDO = "Concluído"

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


def get_number(page: dict, prop_name: str) -> float:
    prop = page["properties"].get(prop_name, {})
    return prop.get("number") or 0


def build_equipe_user_map() -> dict[str, str]:
    """equipe_page_id -> notion_user_id (primeiro usuário em 'Usuário no Notion')."""
    pages = query_database_all(EQUIPE_DB_ID)
    mapping: dict[str, str] = {}
    for page in pages:
        user_ids = get_people_ids(page, EQUIPE_PROP_USUARIO_NOTION)
        if user_ids:
            mapping[page["id"]] = user_ids[0]
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


def main() -> int:
    print("Carregando mapa Equipe | GCMD -> usuário Notion...")
    equipe_user_map = build_equipe_user_map()
    print(f"  {len(equipe_user_map)} pessoas com usuário Notion vinculado.")

    print("Carregando páginas de Carga de trabalho...")
    carga_pages = query_database_all(CARGA_DB_ID)
    print(f"  {len(carga_pages)} páginas encontradas.")

    updated, skipped = 0, 0
    for page in carga_pages:
        equipe_ids = get_relation_ids(page, CARGA_PROP_RELACAO_EQUIPE)
        title_prop = next(
            (p for p in page["properties"].values() if p.get("type") == "title"), None
        )
        nome = (
            title_prop["title"][0]["plain_text"]
            if title_prop and title_prop["title"]
            else page["id"]
        )

        if not equipe_ids:
            print(f"  [pular] {nome}: sem relação com Equipe | GCMD")
            skipped += 1
            continue

        notion_user_id = equipe_user_map.get(equipe_ids[0])
        if not notion_user_id:
            print(f"  [pular] {nome}: pessoa sem 'Usuário no Notion' preenchido")
            skipped += 1
            continue

        tarefas_abertas, peso_total = count_open_tasks(notion_user_id)
        update_carga_page(page["id"], tarefas_abertas, peso_total)
        print(f"  [ok] {nome}: {tarefas_abertas} tarefas abertas, peso total {peso_total}")
        updated += 1

    print(f"Concluído: {updated} atualizadas, {skipped} puladas.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
