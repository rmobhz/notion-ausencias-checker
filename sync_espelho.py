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

PROP_RELACAO = os.getenv("PROP_RELACAO", "Origem")
PROP_TITULO_ESPELHO = os.getenv("PROP_TITULO_ESPELHO", "Evento")
ORIGEM_TITLE_PROP = os.getenv("ORIGEM_TITLE_PROP", "").strip()  # opcional

# ======================================================
# 🔧 MAPA DE PROPRIEDADES COPIADAS (EDITE AQUI)
# origem -> espelho
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
# HTTP helpers
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

# ✅ CORREÇÃO AQUI: normalizar status/select/multi_select por "name"
def normalize_property_for_write(prop: Dict[str, Any]) -> Dict[str, Any]:
    t = prop.get("type")

    # Status: enviar somente name (nunca id)
    if t == "status":
        status_obj = prop.get("status")
        if not status_obj:
            return {"status": None}
        name = status_obj.get("name")
        return {"status": {"name": name}} if name else {"status": None}

    # Select: enviar somente name
    if t == "select":
        sel = prop.get("select")
        if not sel:
            return {"select": None}
        name = sel.get("name")
        return {"select": {"name": name}} if name else {"select": None}

    # Multi-select: lista por name
    if t == "multi_select":
        arr = prop.get("multi_select", []) or []
        return {"multi_select": [{"name": o.get("name")} for o in arr if o.get("name")]}

    # People: pode copiar direto
    if t == "people":
        return {"people": prop.get("people", []) or []}

    # Date: copiar direto (pode ser null)
    if t == "date":
        return {"date": prop.get("date")}

    # Rich text: copiar direto
    if t == "rich_text":
        return {"rich_text": prop.get("rich_text", []) or []}

    # Number, checkbox, url, email, phone, etc.:
    if t in ("number", "checkbox", "url", "email", "phone_number"):
        return {t: prop.get(t)}

    # Fallback: tenta copiar o conteúdo cru do tipo
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

    origem_props = list_properties(DATABASE_ID_REUNIOES)
    espelho_props = list_properties(DATABASE_ID_REUNIOES_ESPELHO)

    if PROP_RELACAO not in espelho_props:
        print(f'\n❌ A relation "{PROP_RELACAO}" não existe na base ESPELHO.')
        print("Propriedades ESPELHO:")
        for k, v in sorted(espelho_props.items()):
            print(f" - {k} -> {v}")
        raise RuntimeError("Crie a relation no espelho ou ajuste PROP_RELACAO.")

    if PROP_TITULO_ESPELHO not in espelho_props or espelho_props[PROP_TITULO_ESPELHO] != "title":
        print(f'\n❌ A propriedade de título "{PROP_TITULO_ESPELHO}" não existe (ou não é title) na base ESPELHO.')
        print("Propriedades ESPELHO:")
        for k, v in sorted(espelho_props.items()):
            print(f" - {k} -> {v}")
        raise RuntimeError("Ajuste PROP_TITULO_ESPELHO para o nome exato do title no espelho.")

    # 1) Buscar todas as reuniões da origem
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

        # 2) Buscar espelho existente
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

        # 3) Título
        title_text = ""
        if ORIGEM_TITLE_PROP:
            title_text = extract_text_from_title_prop(r, ORIGEM_TITLE_PROP)
        if not title_text:
            title_text = extract_plain_text_title_from_page(r)

        props_out: Dict[str, Any] = {
            PROP_RELACAO: {"relation": [{"id": origem_id}]},
            PROP_TITULO_ESPELHO: {"title": [{"text": {"content": title_text}}]},
        }

        # 4) Copiar props permitidas
        for origem_prop, espelho_prop in PROPS_MAP.items():
            if espelho_prop not in espelho_props:
                continue
            prop_data = r["properties"].get(origem_prop)
            if prop_data:
                props_out[espelho_prop] = normalize_property_for_write(prop_data)

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
        else:
            espelho_id = espelhos[0]["id"]
            notion_patch(
                f"/pages/{espelho_id}",
                {"properties": props_out},
                context=f"Atualizar espelho (espelho_id={espelho_id} origem_id={origem_id})",
            )
            updated += 1

        time.sleep(0.2)

    print(f"✅ Concluído | Criados: {created} | Atualizados: {updated}")

if __name__ == "__main__":
    main()
