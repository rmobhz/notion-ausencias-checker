import os
import time
import requests
from typing import Dict, Any, List

# ======================================================
# ENV
# ======================================================
NOTION_API_KEY = os.getenv("NOTION_API_KEY")

DATABASE_ID_REUNIOES = os.getenv("DATABASE_ID_REUNIOES")
DATABASE_ID_REUNIOES_ESPELHO = os.getenv("DATABASE_ID_REUNIOES_ESPELHO")

# Checkbox na ORIGEM
PROP_PUBLICAR = "Publicar intersetorialmente?"

# Relation no ESPELHO apontando para a origem
PROP_RELACAO = "Origem"

# ======================================================
# 🔧 MAPA DE PROPRIEDADES COPIADAS (EDITE AQUI)
# chave   = nome na BASE ORIGEM
# valor   = nome correspondente na BASE ESPELHO
# ======================================================
PROPS_MAP = {
    "Evento": "Evento",
    "Data": "Data",
    "Local": "Local",
    "Status": "Status",
    "Participantes": "Participantes",
}

# ======================================================
# NOTION API
# ======================================================
NOTION_VERSION = "2022-06-28"
BASE_URL = "https://api.notion.com/v1"

HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# ======================================================
# HELPERS
# ======================================================
def notion_post(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.post(f"{BASE_URL}{path}", headers=HEADERS, json=payload)
    r.raise_for_status()
    return r.json()

def notion_patch(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.patch(f"{BASE_URL}{path}", headers=HEADERS, json=payload)
    r.raise_for_status()
    return r.json()

def query_all(database_id: str, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    results = []
    cursor = None
    while True:
        if cursor:
            payload["start_cursor"] = cursor
        data = notion_post(f"/databases/{database_id}/query", payload)
        results.extend(data["results"])
        if not data.get("has_more"):
            break
        cursor = data["next_cursor"]
        time.sleep(0.2)
    return results

def get_title(page: Dict[str, Any]) -> str:
    for prop in page["properties"].values():
        if prop["type"] == "title":
            return "".join(t["plain_text"] for t in prop["title"])
    return "(Sem título)"

def copy_property(prop: Dict[str, Any]) -> Dict[str, Any]:
    """
    Retorna o payload correto da propriedade,
    preservando o tipo original.
    """
    prop_type = prop["type"]
    return {prop_type: prop[prop_type]}

# ======================================================
# CORE
# ======================================================
def main():
    # 1️⃣ Buscar reuniões marcadas para publicar
    reunioes = query_all(
        DATABASE_ID_REUNIOES,
        {
            "filter": {
                "property": PROP_PUBLICAR,
                "checkbox": {"equals": True},
            }
        },
    )

    print(f"🔍 Reuniões para publicar: {len(reunioes)}")

    for r in reunioes:
        origem_id = r["id"]

        # 2️⃣ Verificar se já existe espelho
        espelhos = query_all(
            DATABASE_ID_REUNIOES_ESPELHO,
            {
                "filter": {
                    "property": PROP_RELACAO,
                    "relation": {"contains": origem_id},
                }
            },
        )

        # 3️⃣ Montar propriedades copiadas
        propriedades = {
            PROP_RELACAO: {"relation": [{"id": origem_id}]}
        }

        for origem_prop, espelho_prop in PROPS_MAP.items():
            prop_data = r["properties"].get(origem_prop)
            if prop_data:
                propriedades[espelho_prop] = copy_property(prop_data)

        # 4️⃣ Criar ou atualizar espelho
        if not espelhos:
            payload = {
                "parent": {"database_id": DATABASE_ID_REUNIOES_ESPELHO},
                "properties": propriedades,
            }
            notion_post("/pages", payload)
            print(f"➕ Criado espelho: {get_title(r)}")
        else:
            espelho_id = espelhos[0]["id"]
            notion_patch(
                f"/pages/{espelho_id}",
                {"properties": propriedades},
            )
            print(f"🔄 Atualizado espelho: {get_title(r)}")

        time.sleep(0.2)

    print("✅ Sincronização concluída")

if __name__ == "__main__":
    main()
