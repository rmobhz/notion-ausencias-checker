import os
import time
import json
import random
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

# =============================
# CONFIG
# =============================
STATE_DIR = ".state"
STATE_FILE = os.path.join(STATE_DIR, "mirror_state.json")
INDEX_FILE = os.path.join(STATE_DIR, "mirror_index.json")

BASE_SLEEP = float(os.getenv("MIRROR_BASE_SLEEP", "0.15"))
MAX_RETRIES = int(os.getenv("MIRROR_MAX_RETRIES", "8"))
CLEANUP_EVERY_HOURS = int(os.getenv("MIRROR_CLEANUP_EVERY_HOURS", "24"))
MAX_CHANGED_PER_RUN = int(os.getenv("MIRROR_MAX_CHANGED_PER_RUN", "0"))

# salva estado/índice a cada N itens processados (além de salvar após create)
SAVE_EVERY_N = int(os.getenv("MIRROR_SAVE_EVERY_N", "10"))

MIRRORS = [
    {
        "name": "Reuniões",
        "env_origem": "DATABASE_ID_REUNIOES",
        "env_espelho": "DATABASE_ID_REUNIOES_ESPELHO",

        "relation_prop_espelho": "Origem",
        "title_prop_origem": "Evento",
        "title_prop_espelho": "Evento",

        # fácil editar
        "copy_props": ["Data", "Local", "Status"],

        # Participantes agora é o MESMO nome no espelho, e você quer nomes
        "transforms": {
            "Participantes": {
                "mode": "people_to_public_text",
                "target_prop": "Participantes",
                "people_public_mode": "names",   # <— nomes separados por vírgula
                "separator": ", ",
                # fallback caso não consiga nome (ex. guest sem permissão)
                "fallback_mode": "count",
                "label_singular": "participante",
                "label_plural": "participantes",
            }
        },

        "cleanup_orphans": True,
    }
]

# =============================
# NOTION API
# =============================
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_VERSION = "2022-06-28"
BASE_URL = "https://api.notion.com/v1"
HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# =============================
# Helpers: keys/state/index
# =============================
def mirror_key(cfg: Dict[str, Any], origem_db: str, espelho_db: str) -> str:
    return f'{cfg.get("name","mirror")}::{origem_db}::{espelho_db}'

def _load_json(path: str, default: Any) -> Any:
    os.makedirs(STATE_DIR, exist_ok=True)
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _save_json(path: str, data: Any) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _parse_iso(dt: Optional[str]) -> Optional[datetime]:
    if not dt:
        return None
    try:
        return datetime.fromisoformat(dt.replace("Z", "+00:00"))
    except Exception:
        return None

# =============================
# HTTP with retry/backoff
# =============================
def _parse_json_safe(r: requests.Response) -> Dict[str, Any]:
    try:
        return r.json()
    except Exception:
        return {"raw_text": r.text}

def request_with_retry_url(method: str, url: str, payload: Optional[Dict[str, Any]], context: str) -> Dict[str, Any]:
    for attempt in range(MAX_RETRIES + 1):
        if method == "GET":
            r = requests.get(url, headers=HEADERS, timeout=60)
        elif method == "POST":
            r = requests.post(url, headers=HEADERS, json=payload, timeout=60)
        elif method == "PATCH":
            r = requests.patch(url, headers=HEADERS, json=payload, timeout=60)
        else:
            raise ValueError("Unsupported method")

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

def request_with_retry(method: str, path: str, payload: Optional[Dict[str, Any]], context: str) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}"
    return request_with_retry_url(method, url, payload, context)

def notion_get(path: str, context: str) -> Dict[str, Any]:
    return request_with_retry("GET", path, None, context)

def notion_post(path: str, payload: Dict[str, Any], context: str) -> Dict[str, Any]:
    return request_with_retry("POST", path, payload, context)

def notion_patch(path: str, payload: Dict[str, Any], context: str) -> Dict[str, Any]:
    return request_with_retry("PATCH", path, payload, context)

# =============================
# Notion helpers
# =============================
def get_database_schema(database_id: str) -> Dict[str, Any]:
    return notion_get(f"/databases/{database_id}", context="Get database schema")

def list_properties(database_id: str) -> Dict[str, str]:
    db = get_database_schema(database_id)
    props = db.get("properties", {})
    return {name: props[name].get("type") for name in props.keys()}

def get_property_id_from_schema(schema: Dict[str, Any], prop_name: str) -> Optional[str]:
    prop = (schema.get("properties") or {}).get(prop_name)
    if not prop:
        return None
    return prop.get("id")

def get_status_options(database_id: str, status_prop_name: str) -> List[str]:
    db = get_database_schema(database_id)
    prop = db.get("properties", {}).get(status_prop_name)
    if not prop or prop.get("type") != "status":
        return []
    options = prop.get("status", {}).get("options", []) or []
    return [o.get("name") for o in options if o.get("name")]

def query_database(database_id: str, payload: Dict[str, Any], context: str) -> Dict[str, Any]:
    return notion_post(f"/databases/{database_id}/query", payload, context=context)

def query_all(database_id: str, payload: Dict[str, Any], context: str) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    cursor = None
    while True:
        body = dict(payload)
        if cursor:
            body["start_cursor"] = cursor
        data = query_database(database_id, body, context=context)
        results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return results

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

def rich_text_plain_text(rt: List[Dict[str, Any]]) -> str:
    # transforma qualquer rich_text (com mentions etc.) em texto simples seguro
    return "".join(x.get("plain_text", "") for x in (rt or [])).strip()

def normalize_for_write(prop: Dict[str, Any], target_type: str, status_options_espelho: List[str]) -> Optional[Dict[str, Any]]:
    src_type = prop.get("type")

    if target_type == "date" and src_type == "date":
        return {"date": prop.get("date")}

    if target_type == "rich_text" and src_type == "rich_text":
        # ✅ NÃO copiar estrutura (mentions, etc.). Sempre reduzir a plain_text.
        txt = rich_text_plain_text(prop.get("rich_text", []) or [])
        return {"rich_text": to_rich_text(txt)}

    if target_type == "status" and src_type == "status":
        name = (prop.get("status") or {}).get("name")
        if not name:
            return {"status": None}
        if status_options_espelho and name not in status_options_espelho:
            return {"status": None}
        return {"status": {"name": name}}

    return None

# =============================
# People fallback (next_url pagination)
# =============================
def fetch_people_property_item(page_id: str, prop_id: str) -> List[Dict[str, Any]]:
    people: List[Dict[str, Any]] = []
    url = f"{BASE_URL}/pages/{page_id}/properties/{prop_id}"

    for _ in range(1000):
        data = request_with_retry_url("GET", url, None, context=f"Get page property item (people) page={page_id}")

        results = data.get("results", []) or []
        for it in results:
            uid = it.get("id")
            name = it.get("name")
            if uid or name:
                people.append({"id": uid, "name": name})

        if not data.get("has_more"):
            break

        next_url = data.get("next_url")
        if not next_url:
            break
        url = next_url

    return people

def make_participants_text(
    people_items: List[Dict[str, Any]],
    mode: str,
    sep: str,
    fallback_mode: str,
    singular: str,
    plural: str
) -> str:
    names = [p.get("name") for p in people_items if p.get("name")]
    names = [n for n in names if n]
    count = len([p for p in people_items if p.get("id") or p.get("name")])

    if mode == "names":
        if names:
            return sep.join(names)
        # se não tiver nomes (limitação de permissão), cai no fallback
        mode = fallback_mode

    if mode == "count":
        if count == 0:
            return ""
        return f"{count} {singular if count == 1 else plural}"

    # names_or_count
    if names:
        return sep.join(names)
    if count == 0:
        return ""
    return f"{count} {singular if count == 1 else plural}"

# =============================
# Lookup no espelho (anti-duplicação)
# =============================
def find_mirror_by_relation(espelho_db: str, rel_prop: str, origem_page_id: str) -> Optional[str]:
    payload = {
        "page_size": 1,
        "filter": {
            "property": rel_prop,
            "relation": {"contains": origem_page_id}
        }
    }
    data = query_database(espelho_db, payload, context=f"Lookup espelho by relation origem_id={origem_page_id}")
    results = data.get("results", []) or []
    if results:
        return results[0]["id"]
    return None

# =============================
# Incremental sync
# =============================
def incremental_sync(cfg: Dict[str, Any], state: Dict[str, Any], index: Dict[str, Any]) -> None:
    origem_db = os.getenv(cfg["env_origem"])
    espelho_db = os.getenv(cfg["env_espelho"])
    if not origem_db or not espelho_db:
        raise RuntimeError(f'Faltando env vars para "{cfg["name"]}"')

    k = mirror_key(cfg, origem_db, espelho_db)
    mirror_state = state.get(k, {})
    last_sync = mirror_state.get("last_sync_time")
    last_sync_dt = _parse_iso(last_sync)

    origem_schema = get_database_schema(origem_db)
    origem_props_types = {n: (origem_schema.get("properties") or {}).get(n, {}).get("type") for n in (origem_schema.get("properties") or {}).keys()}
    espelho_props = list_properties(espelho_db)

    rel_prop = cfg["relation_prop_espelho"]
    title_prop_espelho = cfg["title_prop_espelho"]
    title_prop_origem = cfg.get("title_prop_origem", "")

    if rel_prop not in espelho_props or espelho_props[rel_prop] != "relation":
        raise RuntimeError(f'[{cfg["name"]}] Relation "{rel_prop}" inválida no espelho.')
    if title_prop_espelho not in espelho_props or espelho_props[title_prop_espelho] != "title":
        raise RuntimeError(f'[{cfg["name"]}] Title "{title_prop_espelho}" inválido no espelho.')

    status_options = []
    if "Status" in espelho_props and espelho_props["Status"] == "status":
        status_options = get_status_options(espelho_db, "Status")

    # preparar fallback para people
    participants_prop_id = None
    transforms = cfg.get("transforms", {}) or {}
    if "Participantes" in transforms:
        participants_prop_id = get_property_id_from_schema(origem_schema, "Participantes")

    # query changed
    filter_obj = None
    if last_sync_dt:
        filter_obj = {"timestamp": "last_edited_time", "last_edited_time": {"after": last_sync_dt.isoformat()}}

    payload = {
        "page_size": 100,
        "sorts": [{"timestamp": "last_edited_time", "direction": "ascending"}],
    }
    if filter_obj:
        payload["filter"] = filter_obj

    changed: List[Dict[str, Any]] = []
    cursor = None
    while True:
        body = dict(payload)
        if cursor:
            body["start_cursor"] = cursor
        data = query_database(origem_db, body, context=f"Query changed ({cfg['name']})")
        changed.extend(data.get("results", []))
        if MAX_CHANGED_PER_RUN and len(changed) >= MAX_CHANGED_PER_RUN:
            changed = changed[:MAX_CHANGED_PER_RUN]
            break
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    print(f"🔄 [{cfg['name']}] last_sync={last_sync or 'None'} | changed={len(changed)}")

    idx = index.setdefault(k, {})  # origem_id -> espelho_id
    created = 0
    updated = 0

    processed = 0
    newest_edited: Optional[str] = None

    for p in changed:
        origem_page_id = p["id"]
        let = p.get("last_edited_time")
        if let:
            newest_edited = let

        titulo = extract_title(p, title_prop_origem)

        props_out: Dict[str, Any] = {
            rel_prop: {"relation": [{"id": origem_page_id}]},
            title_prop_espelho: {"title": [{"text": {"content": titulo}}]},
        }

        # copy props 1:1 (com sanitização de rich_text)
        for prop_name in cfg.get("copy_props", []):
            if prop_name not in origem_props_types or prop_name not in espelho_props:
                continue
            src_prop = p["properties"].get(prop_name)
            if not src_prop:
                continue
            normalized = normalize_for_write(src_prop, espelho_props[prop_name], status_options)
            if normalized is not None:
                props_out[prop_name] = normalized

        # transforms (Participantes)
        for origem_prop_name, tcfg in transforms.items():
            mode = tcfg.get("mode")
            target_prop = tcfg.get("target_prop", origem_prop_name)

            if origem_prop_name not in origem_props_types:
                continue
            if target_prop not in espelho_props:
                continue
            if espelho_props[target_prop] != "rich_text":
                continue

            src_prop = p["properties"].get(origem_prop_name)
            if not src_prop:
                continue

            if mode == "people_to_public_text":
                if src_prop.get("type") != "people":
                    continue

                # do query
                people_items: List[Dict[str, Any]] = []
                for u in (src_prop.get("people", []) or []):
                    people_items.append({"id": u.get("id"), "name": u.get("name")})

                # fallback property item
                q_count = len([x for x in people_items if x.get("id") or x.get("name")])
                if q_count == 0 and participants_prop_id:
                    try:
                        people_items = fetch_people_property_item(origem_page_id, participants_prop_id)
                    except Exception:
                        people_items = []

                texto = make_participants_text(
                    people_items=people_items,
                    mode=tcfg.get("people_public_mode", "names_or_count"),
                    sep=tcfg.get("separator", ", "),
                    fallback_mode=tcfg.get("fallback_mode", "count"),
                    singular=tcfg.get("label_singular", "participante"),
                    plural=tcfg.get("label_plural", "participantes"),
                )
                props_out[target_prop] = {"rich_text": to_rich_text(texto)}

        # anti-dup: resolve mirror_id pelo índice; se não tiver, procura no espelho
        mirror_id = idx.get(origem_page_id)
        if not mirror_id:
            mirror_id = find_mirror_by_relation(espelho_db, rel_prop, origem_page_id)
            if mirror_id:
                idx[origem_page_id] = mirror_id
                _save_json(INDEX_FILE, index)  # salva imediatamente

        try:
            if mirror_id:
                notion_patch(
                    f"/pages/{mirror_id}",
                    {"properties": props_out},
                    context=f"Incremental update ({cfg['name']}) espelho_id={mirror_id}",
                )
                updated += 1
            else:
                created_page = notion_post(
                    "/pages",
                    {"parent": {"database_id": espelho_db}, "properties": props_out},
                    context=f"Incremental create ({cfg['name']}) origem_id={origem_page_id}",
                )
                idx[origem_page_id] = created_page["id"]
                created += 1

                # ✅ salva índice imediatamente para evitar duplicação em caso de cancelamento
                _save_json(INDEX_FILE, index)

        except Exception as e:
            # não derruba a execução inteira por um item ruim
            print(f"⚠️ [{cfg['name']}] Falha ao sincronizar origem_id={origem_page_id}. Pulando item. Erro: {e}")
            continue

        processed += 1
        if processed % SAVE_EVERY_N == 0:
            # salva progresso parcial
            if newest_edited:
                mirror_state["last_sync_time"] = newest_edited
                state[k] = mirror_state
                _save_json(STATE_FILE, state)
            _save_json(INDEX_FILE, index)

    # salva no final
    if newest_edited:
        mirror_state["last_sync_time"] = newest_edited
        state[k] = mirror_state
        _save_json(STATE_FILE, state)
        _save_json(INDEX_FILE, index)

    print(f"✅ [{cfg['name']}] Incremental | Criados={created} | Atualizados={updated} | last_sync_time={state.get(k, {}).get('last_sync_time')}")

def cleanup_orphans(cfg: Dict[str, Any], state: Dict[str, Any], index: Dict[str, Any]) -> None:
    if not cfg.get("cleanup_orphans"):
        return

    origem_db = os.getenv(cfg["env_origem"])
    espelho_db = os.getenv(cfg["env_espelho"])
    if not origem_db or not espelho_db:
        return

    k = mirror_key(cfg, origem_db, espelho_db)
    mirror_state = state.get(k, {})
    last_cleanup = _parse_iso(mirror_state.get("last_cleanup_time"))

    if last_cleanup and datetime.now(timezone.utc) - last_cleanup < timedelta(hours=CLEANUP_EVERY_HOURS):
        return

    print(f"🧹 [{cfg['name']}] Rodando cleanup de órfãos...")

    origin_pages = query_all(origem_db, {"page_size": 100}, context=f"Load all origin ids ({cfg['name']})")
    origin_ids = {p["id"] for p in origin_pages}

    rel_prop = cfg["relation_prop_espelho"]
    cursor = None
    checked = 0
    archived = 0

    while True:
        payload = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor

        data = query_database(espelho_db, payload, context=f"Query espelho cleanup ({cfg['name']})")
        items = data.get("results", [])
        cursor = data.get("next_cursor") if data.get("has_more") else None

        for m in items:
            rel = m.get("properties", {}).get(rel_prop)
            rel_list = (rel or {}).get("relation", []) or []
            if not rel_list:
                checked += 1
                continue
            oid = rel_list[0].get("id")
            if oid and oid not in origin_ids:
                notion_patch(
                    f"/pages/{m['id']}",
                    {"archived": True},
                    context=f"Archive orphan ({cfg['name']}) espelho_id={m['id']}",
                )
                archived += 1
            checked += 1
            if checked % 80 == 0:
                time.sleep(1.5)

        if not cursor:
            break

    mirror_state["last_cleanup_time"] = _now_iso()
    state[k] = mirror_state
    _save_json(STATE_FILE, state)

    # limpar index de ids que não existem mais
    idx = index.get(k, {})
    to_del = [oid for oid in idx.keys() if oid not in origin_ids]
    for oid in to_del:
        del idx[oid]
    index[k] = idx
    _save_json(INDEX_FILE, index)

    print(f"🧹 [{cfg['name']}] Cleanup | Verificados={checked} | Arquivados={archived} | Index removidos={len(to_del)}")

def main():
    if not NOTION_API_KEY:
        raise RuntimeError("Faltando NOTION_API_KEY.")

    state = _load_json(STATE_FILE, {})
    index = _load_json(INDEX_FILE, {})

    for cfg in MIRRORS:
        incremental_sync(cfg, state, index)
        cleanup_orphans(cfg, state, index)

if __name__ == "__main__":
    main()
