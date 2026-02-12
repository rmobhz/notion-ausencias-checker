import os
import time
import json
import requests
from typing import Dict, Any, List, Optional

# ======================================================
# ✅ CONFIG ÚNICA (edite aqui)
# ======================================================

# Para adicionar novas bases espelho no futuro, duplique um bloco em MIRRORS.
# - env_origem / env_espelho: nomes das env vars com os IDs
# - title_prop_espelho: nome da coluna title no espelho
# - relation_prop_espelho: nome da relation no espelho que aponta pra origem
# - copy_props: lista de propriedades a copiar 1:1 (mesmo nome na origem e espelho)
# - transforms: transformações especiais (ex.: people -> rich_text)
MIRRORS = [
    {
        "name": "Reuniões",
        "env_origem": "DATABASE_ID_REUNIOES",
        "env_espelho": "DATABASE_ID_REUNIOES_ESPELHO",

        # propriedades no ESPELHO
        "title_prop_espelho": "Evento",     # title no espelho
        "relation_prop_espelho": "Origem",  # relation no espelho -> origem

        # ✅ props a copiar facilmente (mesmo nome na origem e espelho)
        "copy_props": ["Data", "Local", "Status"],

        # ✅ transformações especiais
        "transforms": {
            # Participantes: origem people -> espelho rich_text (privacidade)
            "Participantes": {
                "mode": "people_to_text",
                "target_prop": "Participantes",
                "separator": ", ",
                # se quiser expor só setores em vez de nomes, você pode ajustar depois
            }
        },

        # Se na origem o title se chama "Evento", ele já será usado automaticamente.
        # Se não quiser depender disso, você pode setar uma prop title explícita aqui:
        "title_prop_origem": "Evento",
    }
]

# Rate limit leve
SLEEP_BETWEEN_REQUESTS = 0.2

# ======================================================
# NOTION API
# ======================================================
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
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
def _raise_with_details(r: requests.Response, context: str, payload: Optional[Dict[str, Any]] = None) -> None:
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
        time.sleep(SLEEP_BETWEEN_REQUESTS)
    return results

def list_properties(database_id: str) -> Dict[str, str]:
    db = notion_get(f"/databases/{database_id}", context="List properties")
    props = db.get("properties", {})
    return {name: props[name].get("type") for name in props.keys()}

def get_status_options(database_id: str, status_prop_name: str) -> List[str]:
    db = notion_get(f"/databases/{database_id}", context="Get status options")
    prop = db.get("properties", {}).get(status_prop_name)
    if not prop or prop.get("type") != "status":
        return []
    options = prop.get("status", {}).get("options", []) or []
    return [o.get("name") for o in options if o.get("name")]

def extract_title(page: Dict[str, Any], title_prop_origem: str) -> str:
    if title_prop_origem:
        prop = page.get("properties", {}).get(title_prop_origem)
        if prop and prop.get("type") == "title":
            parts = prop.get("title", []) or []
            txt = "".join(p.get("plain_text", "") for p in parts).strip()
            if txt:
                return txt

    # fallback: primeiro title encontrado
    for prop in page.get("properties", {}).values():
        if prop.get("type") == "title":
            parts = prop.get("title", []) or []
            return "".join(p.get("plain_text", "") for p in parts).strip()

    return "(Sem título)"

def to_rich_text(text: str) -> List[Dict[str, Any]]:
    if not text:
        return []
    return [{"type": "text", "text": {"content": text}}]

def people_to_text(prop_people: Dict[str, Any], sep: str = ", ") -> str:
    arr = prop_people.get("people", []) or []
    names = []
    for p in arr:
        name = p.get("name")
        if name:
            names.append(name)
    return sep.join(names)

def normalize_for_write(prop: Dict[str, Any], target_type: str, status_options_espelho: List[str]) -> Optional[Dict[str, Any]]:
    """
    Converte o valor da origem para um payload válido no espelho.
    Só cobre os tipos que você está usando agora (date, rich_text, status).
    """
    src_type = prop.get("type")

    # date -> date
    if target_type == "date" and src_type == "date":
        return {"date": prop.get("date")}

    # rich_text -> rich_text
    if target_type == "rich_text" and src_type == "rich_text":
        return {"rich_text": prop.get("rich_text", []) or []}

    # status -> status (por name; se não existir no espelho, não preenche)
    if target_type == "status" and src_type == "status":
        name = (prop.get("status") or {}).get("name")
        if not name:
            return {"status": None}
        if status_options_espelho and name not in status_options_espelho:
            return {"status": None}
        return {"status": {"name": name}}

    # people não é copiado aqui (tratado por transforms)
    return None

# ======================================================
# Sync (1 mirror)
# ======================================================
def sync_mirror(cfg: Dict[str, Any]) -> None:
    origem_id = os.getenv(cfg["env_origem"])
    espelho_id = os.getenv(cfg["env_espelho"])

    if not origem_id or not espelho_id:
        raise RuntimeError(f'Faltando env vars para "{cfg["name"]}": {cfg["env_origem"]} e/ou {cfg["env_espelho"]}')

    rel_prop = cfg["relation_prop_espelho"]
    title_prop_espelho = cfg["title_prop_espelho"]
    title_prop_origem = cfg.get("title_prop_origem", "")

    origem_props = list_properties(origem_id)
    espelho_props = list_properties(espelho_id)

    # valida essenciais
    if rel_prop not in espelho_props or espelho_props[rel_prop] != "relation":
        raise RuntimeError(f'[{cfg["name"]}] A relation "{rel_prop}" não existe (ou não é relation) no ESPELHO.')

    if title_prop_espelho not in espelho_props or espelho_props[title_prop_espelho] != "title":
        raise RuntimeError(f'[{cfg["name"]}] O title "{title_prop_espelho}" não existe (ou não é title) no ESPELHO.')

    # pré-carrega opções do status (se existir no espelho)
    status_options = []
    if "Status" in espelho_props and espelho_props["Status"] == "status":
        status_options = get_status_options(espelho_id, "Status")

    # busca tudo da origem
    pages = query_all(origem_id, {"page_size": 100}, context=f'Query ORIGEM ({cfg["name"]})')
    print(f'🔍 [{cfg["name"]}] Total na origem: {len(pages)}')

    created = 0
    updated = 0

    for p in pages:
        pid = p["id"]

        # encontra espelho existente
        existing = query_all(
            espelho_id,
            {"filter": {"property": rel_prop, "relation": {"contains": pid}}, "page_size": 1},
            context=f'Query ESPELHO ({cfg["name"]}) contains origem_id',
        )

        # monta props
        titulo = extract_title(p, title_prop_origem)

        props_out: Dict[str, Any] = {
            rel_prop: {"relation": [{"id": pid}]},
            title_prop_espelho: {"title": [{"text": {"content": titulo}}]},
        }

        # copia props 1:1 (com normalização por tipo)
        for prop_name in cfg.get("copy_props", []):
            if prop_name not in origem_props:
                continue
            if prop_name not in espelho_props:
                continue

            src_prop = p["properties"].get(prop_name)
            if not src_prop:
                continue

            normalized = normalize_for_write(
                src_prop,
                target_type=espelho_props[prop_name],
                status_options_espelho=status_options,
            )
            if normalized is not None:
                props_out[prop_name] = normalized

        # transforms (ex.: people -> text)
        transforms = cfg.get("transforms", {}) or {}
        for origem_prop_name, tcfg in transforms.items():
            mode = tcfg.get("mode")
            target_prop = tcfg.get("target_prop", origem_prop_name)

            if origem_prop_name not in origem_props:
                continue
            if target_prop not in espelho_props:
                continue

            src_prop = p["properties"].get(origem_prop_name)
            if not src_prop:
                continue

            if mode == "people_to_text":
                # origem people -> espelho rich_text
                if src_prop.get("type") != "people":
                    continue
                if espelho_props[target_prop] != "rich_text":
                    raise RuntimeError(f'[{cfg["name"]}] Transform people_to_text exige "{target_prop}" como rich_text no espelho.')

                sep = tcfg.get("separator", ", ")
                texto = people_to_text(src_prop, sep=sep)
                props_out[target_prop] = {"rich_text": to_rich_text(texto)}

        # cria/atualiza
        if not existing:
            notion_post(
                "/pages",
                {"parent": {"database_id": espelho_id}, "properties": props_out},
                context=f'Criar espelho ({cfg["name"]}) origem_id={pid}',
            )
            created += 1
        else:
            mid = existing[0]["id"]
            notion_patch(
                f"/pages/{mid}",
                {"properties": props_out},
                context=f'Atualizar espelho ({cfg["name"]}) espelho_id={mid} origem_id={pid}',
            )
            updated += 1

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    print(f'✅ [{cfg["name"]}] Concluído | Criados: {created} | Atualizados: {updated}')

# ======================================================
# MAIN
# ======================================================
def main():
    if not NOTION_API_KEY:
        raise RuntimeError("Faltando NOTION_API_KEY.")

    for cfg in MIRRORS:
        sync_mirror(cfg)

if __name__ == "__main__":
    main()
