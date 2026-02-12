import os
import time
import json
import random
import requests
from typing import Dict, Any, List, Optional

# ======================================================
# ✅ CONFIG ÚNICA (edite aqui)
# ======================================================

STATE_DIR = ".state"
STATE_FILE = os.path.join(STATE_DIR, "mirror_sync_state.json")

# Lotes por execução (seu workflow roda a cada 15 min)
BATCH_SIZE = int(os.getenv("MIRROR_BATCH_SIZE", "60"))
CLEANUP_BATCH_SIZE = int(os.getenv("MIRROR_CLEANUP_BATCH_SIZE", "80"))

# Pausa mínima entre requests + retries para 429/5xx
BASE_SLEEP = float(os.getenv("MIRROR_BASE_SLEEP", "0.15"))
MAX_RETRIES = int(os.getenv("MIRROR_MAX_RETRIES", "8"))

MIRRORS = [
    {
        "name": "Reuniões",
        "env_origem": "DATABASE_ID_REUNIOES",
        "env_espelho": "DATABASE_ID_REUNIOES_ESPELHO",

        # Props no ESPELHO
        "relation_prop_espelho": "Origem",   # relation no espelho -> origem
        "title_prop_espelho": "Evento",      # title no espelho

        # Title na ORIGEM (se existir). Se não existir, usa o primeiro title encontrado.
        "title_prop_origem": "Evento",

        # ✅ Props copiadas 1:1 (mesmo nome origem/espelho) — fácil editar
        "copy_props": ["Data", "Local", "Status"],

        # ✅ Transformações especiais
        "transforms": {
            # Origem: Participantes (people) -> Espelho: Participantes (público) (texto)
            "Participantes": {
                "mode": "people_to_public_text",
                "target_prop": "Participantes (público)",

                # "count" (recomendado e privativo): "2 participantes"
                # "names_or_count": tenta nomes; se não der, usa contagem
                # "names": só nomes (pode ficar vazio dependendo do Notion/permissões)
                "people_public_mode": "names_or_count",

                "separator": ", ",
                "label_singular": "participante",
                "label_plural": "participantes",
            }
        },

        # Limpeza: arquivar no espelho itens cuja origem foi apagada
        "cleanup_orphans": True,
    }
]

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
# STATE
# ======================================================
def load_state() -> Dict[str, Any]:
    os.makedirs(STATE_DIR, exist_ok=True)
    if not os.path.exists(STATE_FILE):
        return {}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state: Dict[str, Any]) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def mirror_key(cfg: Dict[str, Any], origem_db: str, espelho_db: str) -> str:
    return f'{cfg["name"]}::{origem_db}::{espelho_db}'

# ======================================================
# HTTP (retry/backoff)
# ======================================================
def _parse_json_safe(r: requests.Response) -> Dict[str, Any]:
    try:
        return r.json()
    except Exception:
        return {"raw_text": r.text}

def request_with_retry(method: str, path: str, payload: Optional[Dict[str, Any]], context: str) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}"

    for attempt in range(MAX_RETRIES + 1):
        if method == "GET":
            r = requests.get(url, headers=HEADERS, timeout=60)
        elif method == "POST":
            r = requests.post(url, headers=HEADERS, json=payload, timeout=60)
        elif method == "PATCH":
            r = requests.patch(url, headers=HEADERS, json=payload, timeout=60)
        else:
            raise ValueError("Unsupported method")

        # retry on rate limit / server errors
        if r.status_code == 429 or (500 <= r.status_code <= 599):
            wait = min(60, (2 ** attempt)) + random.uniform(0, 0.6)
            ra = r.headers.get("Retry-After")
            if ra:
                try:
                    wait = max(wait, float(ra))
                except Exception:
                    pass
            print(f"⚠️ {r.status_code} em {context}. Tentativa {attempt+1}/{MAX_RETRIES+1}. Aguardando {wait:.1f}s.")
            time.sleep(wait)
            continue

        if not r.ok:
            details = _parse_json_safe(r)
            print("\n❌ ERRO Notion API")
            print(f"Contexto: {context}")
            print(f"Status: {r.status_code}")
            if payload is not None:
                print("Payload:")
                print(json.dumps(payload, ensure_ascii=False, indent=2))
            print("Detalhes:")
            print(json.dumps(details, ensure_ascii=False, indent=2))
            r.raise_for_status()

        time.sleep(BASE_SLEEP)
        return r.json()

    raise RuntimeError(f"Falhou após retries: {context}")

def notion_get(path: str, context: str) -> Dict[str, Any]:
    return request_with_retry("GET", path, None, context)

def notion_post(path: str, payload: Dict[str, Any], context: str) -> Dict[str, Any]:
    return request_with_retry("POST", path, payload, context)

def notion_patch(path: str, payload: Dict[str, Any], context: str) -> Dict[str, Any]:
    return request_with_retry("PATCH", path, payload, context)

# ======================================================
# Notion helpers
# ======================================================
def query_database_page(database_id: str, payload: Dict[str, Any], context: str) -> Dict[str, Any]:
    return notion_post(f"/databases/{database_id}/query", payload, context=context)

def query_all(database_id: str, payload: Dict[str, Any], context: str) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    cursor = None
    while True:
        body = dict(payload)
        if cursor:
            body["start_cursor"] = cursor
        data = query_database_page(database_id, body, context=context)
        results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return results

def list_properties(database_id: str) -> Dict[str, str]:
    db = notion_get(f"/databases/{database_id}", context="List properties")
    props = db.get("properties", {})
    return {name: props[name].get("type") for name in props.keys()}

def get_database_schema(database_id: str) -> Dict[str, Any]:
    return notion_get(f"/databases/{database_id}", context="Get database schema")

def get_status_options(database_id: str, status_prop_name: str) -> List[str]:
    db = get_database_schema(database_id)
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

    for prop in page.get("properties", {}).values():
        if prop.get("type") == "title":
            parts = prop.get("title", []) or []
            return "".join(p.get("plain_text", "") for p in parts).strip()

    return "(Sem título)"

def to_rich_text(text: str) -> List[Dict[str, Any]]:
    if not text:
        return []
    return [{"type": "text", "text": {"content": text}}]

def normalize_for_write(prop: Dict[str, Any], target_type: str, status_options_espelho: List[str]) -> Optional[Dict[str, Any]]:
    src_type = prop.get("type")

    if target_type == "date" and src_type == "date":
        return {"date": prop.get("date")}

    if target_type == "rich_text" and src_type == "rich_text":
        return {"rich_text": prop.get("rich_text", []) or []}

    if target_type == "status" and src_type == "status":
        name = (prop.get("status") or {}).get("name")
        if not name:
            return {"status": None}
        if status_options_espelho and name not in status_options_espelho:
            # não trava a sync
            return {"status": None}
        return {"status": {"name": name}}

    # (se quiser adicionar select/multi_select depois, é aqui)
    return None

def find_existing_mirror(espelho_db: str, rel_prop: str, origem_page_id: str, cfg_name: str) -> Optional[str]:
    res = query_all(
        espelho_db,
        {"filter": {"property": rel_prop, "relation": {"contains": origem_page_id}}, "page_size": 1},
        context=f"Find existing mirror ({cfg_name})",
    )
    if res:
        return res[0]["id"]
    return None

# ======================================================
# ✅ Robust People fetch (fallback)
# ======================================================
def get_property_id_from_schema(schema: Dict[str, Any], prop_name: str) -> Optional[str]:
    prop = (schema.get("properties") or {}).get(prop_name)
    if not prop:
        return None
    return prop.get("id")

def fetch_people_property_item(page_id: str, prop_id: str) -> List[Dict[str, Any]]:
    """
    Busca o valor completo de uma propriedade People via:
    GET /pages/{page_id}/properties/{property_id}
    (paginado)
    Retorna lista de dicts com pelo menos id/name quando existirem.
    """
    people: List[Dict[str, Any]] = []
    cursor = None

    while True:
        path = f"/pages/{page_id}/properties/{prop_id}"
        if cursor:
            path += f"?start_cursor={cursor}"

        data = notion_get(path, context=f"Get page property item (people) page={page_id}")
        results = data.get("results", []) or []

        # Estruturas podem variar; extraímos id/name quando existirem
        for it in results:
            uid = it.get("id")
            name = it.get("name")
            if uid or name:
                people.append({"id": uid, "name": name})

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    return people

def make_participants_public_text(
    people_items: List[Dict[str, Any]],
    mode: str,
    sep: str,
    singular: str,
    plural: str
) -> str:
    """
    mode:
      - count: "2 participantes"
      - names_or_count: tenta nomes; se não der, usa contagem
      - names: só nomes
    """
    count = len([p for p in people_items if p.get("id") or p.get("name")])

    if mode == "count":
        if count == 0:
            return ""
        return f"{count} {singular if count == 1 else plural}"

    names = [p.get("name") for p in people_items if p.get("name")]
    names = [n for n in names if n]

    if mode == "names":
        return sep.join(names)

    # names_or_count
    if names:
        return sep.join(names)
    if count == 0:
        return ""
    return f"{count} {singular if count == 1 else plural}"

# ======================================================
# Sync with batching + cleanup
# ======================================================
def sync_mirror(cfg: Dict[str, Any], state: Dict[str, Any]) -> None:
    origem_db = os.getenv(cfg["env_origem"])
    espelho_db = os.getenv(cfg["env_espelho"])
    if not origem_db or not espelho_db:
        raise RuntimeError(f'Faltando env vars para "{cfg["name"]}": {cfg["env_origem"]} e/ou {cfg["env_espelho"]}')

    key = mirror_key(cfg, origem_db, espelho_db)
    s = state.get(key, {})
    cursor = s.get("cursor")
    phase = s.get("phase", "sync")  # "sync" ou "cleanup"
    cleanup_cursor = s.get("cleanup_cursor")

    rel_prop = cfg["relation_prop_espelho"]
    title_prop_espelho = cfg["title_prop_espelho"]
    title_prop_origem = cfg.get("title_prop_origem", "")

    origem_props = list_properties(origem_db)
    espelho_props = list_properties(espelho_db)

    if rel_prop not in espelho_props or espelho_props[rel_prop] != "relation":
        raise RuntimeError(f'[{cfg["name"]}] Relation "{rel_prop}" não existe (ou não é relation) no espelho.')
    if title_prop_espelho not in espelho_props or espelho_props[title_prop_espelho] != "title":
        raise RuntimeError(f'[{cfg["name"]}] Title "{title_prop_espelho}" não existe (ou não é title) no espelho.')

    # schema da origem para conseguir property_id do People (fallback)
    origem_schema = get_database_schema(origem_db)
    participants_prop_id = None
    # Descobre a prop source do transform (a chave do dict)
    if cfg.get("transforms"):
        # se tiver "Participantes" no transforms, pega o id no schema
        if "Participantes" in cfg["transforms"]:
            participants_prop_id = get_property_id_from_schema(origem_schema, "Participantes")

    status_options = []
    if "Status" in espelho_props and espelho_props["Status"] == "status":
        status_options = get_status_options(espelho_db, "Status")

    # -------------------------
    # PHASE 1: SYNC
    # -------------------------
    if phase == "sync":
        processed = 0
        created = 0
        updated = 0

        while processed < BATCH_SIZE:
            payload = {"page_size": 100}
            if cursor:
                payload["start_cursor"] = cursor

            page = query_database_page(origem_db, payload, context=f"Query origem page ({cfg['name']})")
            items = page.get("results", [])
            cursor = page.get("next_cursor") if page.get("has_more") else None

            if not items:
                break

            for p in items:
                if processed >= BATCH_SIZE:
                    break

                origem_page_id = p["id"]
                mirror_id = find_existing_mirror(espelho_db, rel_prop, origem_page_id, cfg["name"])

                titulo = extract_title(p, title_prop_origem)

                props_out: Dict[str, Any] = {
                    rel_prop: {"relation": [{"id": origem_page_id}]},
                    title_prop_espelho: {"title": [{"text": {"content": titulo}}]},
                }

                # Copiar props 1:1
                for prop_name in cfg.get("copy_props", []):
                    if prop_name not in origem_props or prop_name not in espelho_props:
                        continue
                    src_prop = p["properties"].get(prop_name)
                    if not src_prop:
                        continue

                    normalized = normalize_for_write(src_prop, espelho_props[prop_name], status_options)
                    if normalized is not None:
                        props_out[prop_name] = normalized

                # Transforms
                transforms = cfg.get("transforms", {}) or {}
                for origem_prop_name, tcfg in transforms.items():
                    mode = tcfg.get("mode")
                    target_prop = tcfg.get("target_prop", origem_prop_name)

                    if origem_prop_name not in origem_props or target_prop not in espelho_props:
                        continue

                    src_prop = p["properties"].get(origem_prop_name)
                    if not src_prop:
                        continue

                    if mode == "people_to_public_text":
                        if src_prop.get("type") != "people":
                            continue
                        if espelho_props[target_prop] != "rich_text":
                            continue

                        # tenta people do query
                        people_items: List[Dict[str, Any]] = []
                        people_arr = src_prop.get("people", []) or []
                        for u in people_arr:
                            people_items.append({"id": u.get("id"), "name": u.get("name")})

                        # fallback: se veio vazio/incompleto, busca via property item
                        if (not people_items or all((not x.get("id") and not x.get("name")) for x in people_items)) and participants_prop_id:
                            try:
                                people_items = fetch_people_property_item(origem_page_id, participants_prop_id)
                            except Exception:
                                # não trava o sync
                                people_items = []

                        people_mode = tcfg.get("people_public_mode", "count")
                        sep = tcfg.get("separator", ", ")
                        singular = tcfg.get("label_singular", "participante")
                        plural = tcfg.get("label_plural", "participantes")

                        texto = make_participants_public_text(
                            people_items=people_items,
                            mode=people_mode,
                            sep=sep,
                            singular=singular,
                            plural=plural,
                        )

                        props_out[target_prop] = {"rich_text": to_rich_text(texto)}

                # Criar / Atualizar
                if mirror_id is None:
                    notion_post(
                        "/pages",
                        {"parent": {"database_id": espelho_db}, "properties": props_out},
                        context=f"Criar espelho ({cfg['name']}) origem_id={origem_page_id}",
                    )
                    created += 1
                else:
                    notion_patch(
                        f"/pages/{mirror_id}",
                        {"properties": props_out},
                        context=f"Atualizar espelho ({cfg['name']}) espelho_id={mirror_id} origem_id={origem_page_id}",
                    )
                    updated += 1

                processed += 1

            if cursor is None:
                break

        print(f"✅ [{cfg['name']}] Sync batch | Processados: {processed} | Criados: {created} | Atualizados: {updated} | Próximo cursor: {cursor}")

        s["cursor"] = cursor
        if cursor is None and cfg.get("cleanup_orphans"):
            s["phase"] = "cleanup"
            s["cleanup_cursor"] = None
        state[key] = s
        save_state(state)
        return

    # -------------------------
    # PHASE 2: CLEANUP (órfãos)
    # -------------------------
    if phase == "cleanup":
        # Carrega IDs da origem (para saber o que ainda existe)
        all_origin = query_all(origem_db, {"page_size": 100}, context=f"Load all origin ids ({cfg['name']})")
        origin_ids = {p["id"] for p in all_origin}

        processed = 0
        archived = 0
        cursor = cleanup_cursor

        while processed < CLEANUP_BATCH_SIZE:
            payload = {"page_size": 100}
            if cursor:
                payload["start_cursor"] = cursor

            page = query_database_page(espelho_db, payload, context=f"Query espelho page (cleanup {cfg['name']})")
            items = page.get("results", [])
            cursor = page.get("next_cursor") if page.get("has_more") else None

            if not items:
                break

            for m in items:
                if processed >= CLEANUP_BATCH_SIZE:
                    break

                rel = m.get("properties", {}).get(rel_prop)
                rel_list = (rel or {}).get("relation", []) or []
                if not rel_list:
                    processed += 1
                    continue

                oid = rel_list[0].get("id")
                if oid and oid not in origin_ids:
                    notion_patch(
                        f"/pages/{m['id']}",
                        {"archived": True},
                        context=f"Arquivar órfão ({cfg['name']}) espelho_id={m['id']} origin_id={oid}",
                    )
                    archived += 1

                processed += 1

            if cursor is None:
                break

        print(f"🧹 [{cfg['name']}] Cleanup batch | Verificados: {processed} | Órfãos arquivados: {archived} | Próximo cursor: {cursor}")

        s["cleanup_cursor"] = cursor
        if cursor is None:
            # terminou limpeza: volta pro sync do início pra pegar novas mudanças
            s["phase"] = "sync"
            s["cursor"] = None
            s["cleanup_cursor"] = None

        state[key] = s
        save_state(state)
        return

def main():
    if not NOTION_API_KEY:
        raise RuntimeError("Faltando NOTION_API_KEY.")

    state = load_state()

    for cfg in MIRRORS:
        sync_mirror(cfg, state)

if __name__ == "__main__":
    main()
