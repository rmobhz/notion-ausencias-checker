import os
import time
import json
import requests
from typing import Dict, Any, List

# ======================================================
# ENV
# ======================================================
NOTION_API_KEY = os.getenv("NOTION_API_KEY")

DATABASE_ID_REUNIOES = os.getenv("DATABASE_ID_REUNIOES")
DATABASE_ID_REUNIOES_ESPELHO = os.getenv("DATABASE_ID_REUNIOES_ESPELHO")

# Relation no ESPELHO apontando para a origem
PROP_RELACAO = os.getenv("PROP_RELACAO", "Origem")

# Nome da propriedade de título na base ESPELHO (no Notion costuma ser "Name")
PROP_TITULO_ESPELHO = os.getenv("PROP_TITULO_ESPELHO", "Name")

# Se você quiser usar uma prop específica da origem como "Evento" para título
# (precisa ser do tipo Title na origem). Se vazio, usa o título padrão do item.
ORIGEM_TITLE_PROP = os.getenv("ORIGEM_TITLE_PROP", "Evento").strip()

# ======================================================
# 🔧 MAPA DE PROPRIEDADES COPIADAS (EDITE AQUI)
# origem -> espelho (nomes EXATOS no Notion)
# ======================================================
PROPS_MAP = {
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
# HTTP helpers (com log detalhado)
# ======================================================
def _raise_with_details(r: requests.Response, context: str, payload: Dict[str, Any] | None = None) -> None:
    try:
        details = r.json()
    except Exception:
        details = {"raw_text": r.text}

    print("\n❌ ERRO Notion API")
    print(f"Contexto: {context}")
    print(f"Status: {r.status_code}")
    if payload is not None:
        print("Payload:")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    print("Detalhes:")
    print(json.dumps(details, ensure_ascii=False, indent=2))
    print()
    r.raise_for_status()

def notion_get(path: str, context: str) -> Dict[str, Any]:
    r = requests.get(f"{BASE_URL}{path}", headers=HEADERS, timeout=60)
    if not r.ok:
        _raise_with_details(r, f"GET {path} | {context}")
    return r.json()

def notion_post(path: str, payload: Dict[str, Any], context: str) -> Dict[str, Any]:
    r = requests.post(f"{BASE_URL}{path}", headers=HEADERS, json=payload, timeout=60)
    if not r.ok:
        _raise_with_details(r, f"POST {path} | {context}", payload)
    return r.json()

def notion_patch(path: str, payload: Dict[str, Any], context: str) -> Dict[str, Any]:
    r = requests.patch(f"{BASE_URL}{path}", headers=HEADERS, json=payload, timeout=60)
    if not r.ok:
        _raise_with_details(r, f"PATCH {path} | {context}", payload)
    return r.json()

# ======================================================
# Notion helpers
# ======================================================
def query_all(database_id: str, payload: Dict[str, Any], context: str) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    cursor = None

    while True:
        body = dict(payload)
        if cursor:
            body["start_cursor"] = cursor

        data = notion_post(f"/databases/{database_id}/query", body, context=context)
        results.extend(data.get("results", []))

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
        time.sleep(0.2)

    return results

def list_properties(database_id: str) -> Dict[str, str]:
    db = notion_get(f"/databases/{database_id}", context="List properties")
    props = db.get("properties", {})
    return {name: props[name].get("type") for name in props.keys()}

def extract_plain_text_title_from_page(page: Dict[str, Any]) -> str:
    for prop in page.get("properties", {}).values():
        if prop.get("type") == "title":
            parts = prop.get("title", [])
            return "".join(p.get("plain_text", "") for p in parts).strip()
    return "(Sem título)"

def extract_text_from_title_prop(page: Dict[str, Any], prop_name: str) -> str:
    prop = page.get("properties", {}).get(prop_name)
    if not prop or prop.get("type") != "title":
        return ""
    parts = prop.get("title", [])
    return "".join(p.get("plain_text", "") for p in parts).strip()

def copy_property(prop: Dict[str, Any]) -> Dict[str, Any]:
    t = prop.get("type")
    # mantém o payload original do tipo
    return {t: prop.get(t)}

# ======================================================
# Core
# ======================================================
def main():
    missing = []
    if not NOTION_API_KEY: missing.append("NOTION_API_KEY")
    if not DATABASE_ID_REUNIOES: missing.append("DATABASE_ID_REUNIOES")
    if not DATABASE_ID_REUNIOES_ESPELHO: missing.append("DATABASE_ID_REUNIOES_ESPELHO")
    if missing:
        raise RuntimeError(f"Faltando env vars: {', '.join(missing)}")

    # (Opcional, mas útil) validar que as props existem — ajuda a evitar 400
    origem_props = list_properties(DATABASE_ID_REUNIOES)
    espelho_props = list_properties(DATABASE_ID_REUNIOES_ESPELHO)

    # valida relation e title do espelho
    if PROP_RELACAO not in espelho_props:
        print(f'\n❌ A relation "{PROP_RELACAO}" não existe na base ESPELHO.')
        print("Propriedades ESPELHO:")
        for k, v in sorted(espelho_props.items()):
            print(f" - {k} -> {v}")
        raise RuntimeError("Crie a relation no espelho ou ajuste PROP_RELACAO.")

    if PROP_TITULO_ESPELHO not in espelho_props:
        print(f'\n❌ A propriedade de título "{PROP_TITULO_ESPELHO}" não existe na base ESPELHO.')
        print("Propriedades ESPELHO:")
        for k, v in sorted(espelho_props.items()):
            print(f" - {k} -> {v}")
        raise RuntimeError("Ajuste PROP_TITULO_ESPELHO para o nome exato do title no espelho.")

    # valida props do mapa
    for o, e in PROPS_MAP.items():
        if o not in origem_props:
            print(f'\n⚠️ Origem não tem a prop "{o}" (mapa). Ela será ignorada.')
        if e not in espelho_props:
            print(f'\n⚠️ Espelho não tem a prop "{e}" (mapa). Ela será ignorada.')

    # 1) Buscar TODAS as reuniões da origem
    reunioes = query_all(
        DATABASE_ID_REUNIOES,
        {"page_size": 100},
        context="Query ORIGEM (todas as páginas)",
    )
    print(f"🔍 Total de reuniões na origem: {len(reunioes)}")

    created = 0
    updated = 0

    for r in reunioes:
        origem_id = r["id"]

        # 2) Verificar espelho existente pela relation
        espelhos = query_all(
            DATABASE_ID_REUNIOES_ESPELHO,
            {
                "filter": {
                    "property": PROP_RELACAO,
                    "relation": {"contains": origem_id},
                },
                "page_size": 1,
            },
            context="Query ESPELHO (relation contains origem_id)",
        )

        # 3) Título do espelho
        title_text = ""
        if ORIGEM_TITLE_PROP:
            title_text = extract_text_from_title_prop(r, ORIGEM_TITLE_PROP)
        if not title_text:
            title_text = extract_plain_text_title_from_page(r)

        # 4) Montar propriedades do espelho
        props_out: Dict[str, Any] = {
            PROP_RELACAO: {"relation": [{"id": origem_id}]},
            PROP_TITULO_ESPELHO: {"title": [{"text": {"content": title_text}}]},
        }

        for origem_prop, espelho_prop in PROPS_MAP.items():
            prop_data = r["properties"].get(origem_prop)
            if prop_data and (espelho_prop in espelho_props):
                props_out[espelho_prop] = copy_property(prop_data)

        # 5) Criar ou atualizar
        if not espelhos:
            notion_post(
                "/pages",
                {
                    "parent": {"database_id": DATABASE_ID_REUNIOES_ESPELHO},
                    "properties": props_out,
                },
                context=f"Criar espelho (origem_id={origem_id})",
            )
            created += 1
            print(f"➕ Criado: {title_text}")
        else:
            espelho_id = espelhos[0]["id"]
            notion_patch(
                f"/pages/{espelho_id}",
                {"properties": props_out},
                context=f"Atualizar espelho (espelho_id={espelho_id} origem_id={origem_id})",
            )
            updated += 1
            print(f"🔄 Atualizado: {title_text}")

        time.sleep(0.2)

    print(f"✅ Concluído | Criados: {created} | Atualizados: {updated}")

if __name__ == "__main__":
    main()
