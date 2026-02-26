#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import random
import requests
from typing import Any, Dict, List, Optional

# =========================
# CONFIG
# =========================
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_VERSION = "2022-06-28"
BASE_URL = "https://api.notion.com/v1"

STATE_DIR = ".state"

# Seleção do que roda (por padrão, só YouTube)
RUN_REUNIOES = os.getenv("RUN_REUNIOES", "0").strip() == "1"
RUN_YOUTUBE = os.getenv("RUN_YOUTUBE", "1").strip() == "1"
RUN_CALENDARIOEDITORIAL = os.getenv("RUN_CALENDARIOEDITORIAL", "0").strip() == "1"
RUN_TAREFAS_GCMD = os.getenv("RUN_TAREFAS_GCMD", "0").strip() == "1"
RUN_CONTROLE_DEMANDAS = os.getenv("RUN_CONTROLE_DEMANDAS", "0").strip() == "1"

# Segurança / teste
MIRROR_DRY_RUN = os.getenv("MIRROR_DRY_RUN", "0").strip() == "1"
MIRROR_LIMIT = int(os.getenv("MIRROR_LIMIT", "0").strip())

# Execução
MIRROR_FORCE_FULL_SYNC = os.getenv("MIRROR_FORCE_FULL_SYNC", "1").strip() == "1"
MIRROR_INCREMENTAL = os.getenv("MIRROR_INCREMENTAL", "0").strip() == "1"
MIRROR_UPDATE_ARCHIVED = os.getenv("MIRROR_UPDATE_ARCHIVED", "1").strip() == "1"

MIRROR_SLEEP_MS = int(os.getenv("MIRROR_SLEEP_MS", "150").strip())
SLEEP_SEC = max(0, MIRROR_SLEEP_MS) / 1000.0

# A partir de 2026
DATE_FROM = "2026-01-01"

# Retry para instabilidades (Cloudflare/Notion)
RETRY_STATUS = {429, 500, 502, 503, 504}


# =========================
# HTTP helpers (com retry)
# =========================
def notion_headers() -> Dict[str, str]:
    if not NOTION_API_KEY:
        raise RuntimeError("Faltou NOTION_API_KEY no env.")
    return {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def request_with_retry(method: str, url: str, payload: Optional[Dict[str, Any]] = None, max_attempts: int = 6) -> Dict[str, Any]:
    for attempt in range(1, max_attempts + 1):
        try:
            if method == "GET":
                r = requests.get(url, headers=notion_headers(), timeout=60)
            elif method == "POST":
                r = requests.post(url, headers=notion_headers(), json=payload, timeout=60)
            elif method == "PATCH":
                r = requests.patch(url, headers=notion_headers(), json=payload, timeout=60)
            else:
                raise RuntimeError(f"Método inválido: {method}")

            if r.status_code < 400:
                return r.json()

            if r.status_code in RETRY_STATUS:
                retry_after = r.headers.get("Retry-After")
                if retry_after:
                    sleep_s = float(retry_after)
                else:
                    sleep_s = min(30.0, (2 ** (attempt - 1)) * 0.8 + random.random())

                print(f"↩️  retry {attempt}/{max_attempts} {method} {r.status_code} em {sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue

            raise RuntimeError(f"HTTP {r.status_code} {method} {url}\n{r.text}")

        except requests.RequestException as e:
            sleep_s = min(30.0, (2 ** (attempt - 1)) * 0.8 + random.random())
            print(f"↩️  retry {attempt}/{max_attempts} {method} exception em {sleep_s:.1f}s: {e}")
            time.sleep(sleep_s)

    raise RuntimeError(f"Falhou após {max_attempts} tentativas: {method} {url}")


def http_get(url: str) -> Dict[str, Any]:
    return request_with_retry("GET", url)


def http_post(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return request_with_retry("POST", url, payload=payload)


def http_patch(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return request_with_retry("PATCH", url, payload=payload)


# =========================
# Resolver DB ID a partir de PAGE ID (child_database)
# =========================
def resolve_child_database_from_page(page_id: str) -> str:
    start_cursor: Optional[str] = None
    while True:
        url = f"{BASE_URL}/blocks/{page_id}/children?page_size=100"
        if start_cursor:
            url += f"&start_cursor={start_cursor}"

        data = http_get(url)
        for b in data.get("results", []):
            if b.get("type") == "child_database":
                return b["id"]

        if data.get("has_more"):
            start_cursor = data.get("next_cursor")
            time.sleep(SLEEP_SEC)
        else:
            break

    raise RuntimeError(
        f"O ID {page_id} parece ser uma página, mas não encontrei nenhum child_database dentro dela. "
        f"Dica: abra a base e use 'Open as page' + Copy link para obter o database_id."
    )


def resolve_database_id(maybe_db_or_page_id: str, label: str) -> str:
    try:
        _ = http_get(f"{BASE_URL}/databases/{maybe_db_or_page_id}")
        return maybe_db_or_page_id
    except Exception:
        pass

    db_id = resolve_child_database_from_page(maybe_db_or_page_id)
    print(f"ℹ️ [{label}] ID informado era PAGE; resolvido para DATABASE: {db_id}")
    return db_id


# =========================
# Estado local por espelho
# =========================
def state_path(mirror_name: str) -> str:
    safe = "".join(c for c in mirror_name.lower() if c.isalnum() or c in ("-", "_"))
    return os.path.join(STATE_DIR, f"mirror_{safe}.json")


def load_state(mirror_name: str) -> Dict[str, Any]:
    os.makedirs(STATE_DIR, exist_ok=True)
    path = state_path(mirror_name)
    if not os.path.exists(path):
        return {"mappings": {}, "last_sync_time": None}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(mirror_name: str, state: Dict[str, Any]) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    path = state_path(mirror_name)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# =========================
# Utilitários Notion props + icon
# =========================
READ_ONLY_TYPES = {
    "formula",
    "rollup",
    "created_time",
    "last_edited_time",
    "created_by",
    "last_edited_by",
    "unique_id",
    "button",
}


def is_archived(page: Dict[str, Any]) -> bool:
    return bool(page.get("archived", False))


def plain_text_from_rich(prop: Dict[str, Any]) -> str:
    return "".join([p.get("plain_text", "") for p in prop.get("rich_text", [])])


def plain_text_from_title(prop: Dict[str, Any]) -> str:
    return "".join([p.get("plain_text", "") for p in prop.get("title", [])])


def plain_text_from_people(prop: Dict[str, Any]) -> str:
    ppl = prop.get("people", []) or []
    names = [p.get("name", "").strip() for p in ppl]
    names = [n for n in names if n]
    return ", ".join(names)


def sanitize_icon(icon: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not icon:
        return None

    t = icon.get("type")
    if t == "emoji" and icon.get("emoji"):
        return {"type": "emoji", "emoji": icon["emoji"]}

    if t == "external" and icon.get("external", {}).get("url"):
        return {"type": "external", "external": {"url": icon["external"]["url"]}}

    if t == "file":
        url = (icon.get("file") or {}).get("url")
        if url:
            return {"type": "external", "external": {"url": url}}

    return None

MAX_NOTION_TEXT = 1900  # margem contra contagem diferente (unicode/emojis)

def rich_text_chunks(text: str, max_len: int = MAX_NOTION_TEXT) -> List[Dict[str, Any]]:
    """
    Divide um texto grande em vários blocos rich_text.
    Notion valida limite ~2000 por bloco, mas pode contar diferente com unicode.
    Usamos 1900 para evitar 400.
    """
    if not text:
        return []
    chunks = []
    start = 0
    while start < len(text):
        part = text[start:start + max_len]
        chunks.append({"type": "text", "text": {"content": part}})
        start += max_len
    return chunks


def build_property_payload(prop: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not prop:
        return None
    t = prop.get("type")
    if not t or t in READ_ONLY_TYPES:
        return None

    if t == "title":
        text = plain_text_from_title(prop)
        return {"title": [{"type": "text", "text": {"content": text}}]}

    if t == "rich_text":
        text = plain_text_from_rich(prop)
        return {"rich_text": rich_text_chunks(text)} if text else {"rich_text": []}

    if t == "number":
        return {"number": prop.get("number")}

    if t == "select":
        sel = prop.get("select")
        # ✅ Escrever por NOME (IDs não batem entre DBs)
        return {"select": {"name": sel.get("name")}} if sel and sel.get("name") else {"select": None}

    if t == "multi_select":
        ms = prop.get("multi_select", []) or []
        # ✅ Escrever por NOME (IDs não batem entre DBs)
        return {"multi_select": [{"name": o.get("name")} for o in ms if o.get("name")]}

    if t == "status":
        st = prop.get("status")
        # status também aceita name, mas o destino precisa ter a opção configurada (recomendado)
        return {"status": {"name": st.get("name")}} if st and st.get("name") else {"status": None}

    if t == "date":
        return {"date": prop.get("date")}

    if t == "checkbox":
        return {"checkbox": bool(prop.get("checkbox", False))}

    if t == "url":
        return {"url": prop.get("url")}

    if t == "email":
        return {"email": prop.get("email")}

    if t == "phone_number":
        return {"phone_number": prop.get("phone_number")}

    if t == "people":
        ppl = prop.get("people", [])
        return {"people": [{"id": p["id"]} for p in ppl if p.get("id")]}

    if t == "relation":
        rel = prop.get("relation", [])
        return {"relation": [{"id": r["id"]} for r in rel if r.get("id")]}

    if t == "files":
        return {"files": prop.get("files", [])}

    return None


def get_database_schema(db_id: str) -> Dict[str, Any]:
    return http_get(f"{BASE_URL}/databases/{db_id}")


def build_dest_properties(
    source_page: Dict[str, Any],
    include_only_props: Optional[List[str]],
    *,
    mirror_db_schema: Optional[Dict[str, Any]] = None,
    force_origin_relation_prop: Optional[str] = None,
) -> Dict[str, Any]:
    src_props = source_page.get("properties", {}) or {}
    dest: Dict[str, Any] = {}

    dest_prop_types: Dict[str, str] = {}
    if mirror_db_schema:
        for pname, pdef in (mirror_db_schema.get("properties", {}) or {}).items():
            ptype = pdef.get("type")
            if ptype:
                dest_prop_types[pname] = ptype

    for prop_name, prop in src_props.items():
        if include_only_props is not None and prop_name not in include_only_props:
            continue

        # Conversão people -> rich_text (quando o espelho tem texto)
        if prop.get("type") == "people" and dest_prop_types.get(prop_name) == "rich_text":
            txt = plain_text_from_people(prop)
            dest[prop_name] = {"rich_text": [{"type": "text", "text": {"content": txt}}]} if txt else {"rich_text": []}
            continue

        payload = build_property_payload(prop)
        if payload is not None:
            dest[prop_name] = payload

    # Força Origem relation -> source_id
    if include_only_props is not None and force_origin_relation_prop:
        if force_origin_relation_prop in include_only_props:
            if dest_prop_types.get(force_origin_relation_prop) == "relation":
                dest[force_origin_relation_prop] = {"relation": [{"id": source_page["id"]}]}

    return dest


# =========================
# Query com filtro de data (>= DATE_FROM)
# =========================
def query_database_pages(
    db_id: str,
    date_property_name: Optional[str],
    date_from: str,
    incremental_after: Optional[str],
    sorts: Optional[List[Dict[str, Any]]],
    *,
    extra_filters: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/databases/{db_id}/query"

    filters: List[Dict[str, Any]] = []
    if extra_filters:
        filters.extend(extra_filters)

    if date_property_name:
        filters.append({"property": date_property_name, "date": {"on_or_after": date_from}})

    if incremental_after:
        filters.append({"timestamp": "last_edited_time", "last_edited_time": {"after": incremental_after}})

    payload: Dict[str, Any] = {"page_size": 100}
    if filters:
        payload["filter"] = {"and": filters} if len(filters) > 1 else filters[0]
    if sorts:
        payload["sorts"] = sorts

    results: List[Dict[str, Any]] = []
    next_cursor: Optional[str] = None

    while True:
        if next_cursor:
            payload["start_cursor"] = next_cursor
        else:
            payload.pop("start_cursor", None)

        data = http_post(url, payload)
        results.extend(data.get("results", []))

        if data.get("has_more"):
            next_cursor = data.get("next_cursor")
            time.sleep(SLEEP_SEC)
        else:
            break

    return results


# =========================
# Upsert (com icon)
# =========================
def create_page_in_mirror(
    mirror_db_id: str,
    properties: Dict[str, Any],
    *,
    icon: Optional[Dict[str, Any]] = None,
) -> str:
    url = f"{BASE_URL}/pages"
    payload: Dict[str, Any] = {"parent": {"database_id": mirror_db_id}, "properties": properties}
    if icon:
        payload["icon"] = icon
    data = http_post(url, payload)
    return data["id"]


def update_page_in_mirror(
    page_id: str,
    properties: Dict[str, Any],
    *,
    icon: Optional[Dict[str, Any]] = None,
) -> None:
    url = f"{BASE_URL}/pages/{page_id}"
    payload: Dict[str, Any] = {"properties": properties}
    if icon:
        payload["icon"] = icon
    http_patch(url, payload)


def now_iso_z() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def mirror_database(
    name: str,
    source_db_id: str,
    mirror_db_id: str,
    *,
    include_only_props: Optional[List[str]],
    date_property_name: Optional[str],
    date_from: str,
    sort_by_date: bool = True,
    force_origin_relation_prop: Optional[str] = None,
    extra_filters: Optional[List[Dict[str, Any]]] = None,
) -> None:
    state = load_state(name)
    mappings: Dict[str, str] = state.get("mappings", {}) or {}

    incremental_after = None
    if MIRROR_INCREMENTAL and not MIRROR_FORCE_FULL_SYNC:
        incremental_after = state.get("last_sync_time")

    sorts = None
    if sort_by_date and date_property_name:
        sorts = [{"property": date_property_name, "direction": "ascending"}]

    mirror_schema = get_database_schema(mirror_db_id)

    pages = query_database_pages(
        db_id=source_db_id,
        date_property_name=date_property_name,
        date_from=date_from,
        incremental_after=incremental_after,
        sorts=sorts,
        extra_filters=extra_filters,
    )

    mode = "DRY_RUN" if MIRROR_DRY_RUN else ("FULL" if MIRROR_FORCE_FULL_SYNC or not MIRROR_INCREMENTAL else "INCR")
    print(
        f"🔄 [{name}] mode={mode}"
        f" | origem_itens={len(pages)} | from={date_from} | date_prop={date_property_name or 'N/A'}"
        + (f" | incremental_after={incremental_after}" if incremental_after else "")
    )

    if MIRROR_LIMIT and MIRROR_LIMIT > 0:
        pages = pages[:MIRROR_LIMIT]
        print(f"🧪 [{name}] MIRROR_LIMIT ativo: processando somente {len(pages)} itens")

    created = 0
    updated = 0
    skipped_archived = 0
    errors = 0

    for i, page in enumerate(pages, start=1):
        source_id = page["id"]

        if is_archived(page) and not MIRROR_UPDATE_ARCHIVED:
            skipped_archived += 1
            continue

        try:
            props = build_dest_properties(
                page,
                include_only_props=include_only_props,
                mirror_db_schema=mirror_schema,
                force_origin_relation_prop=force_origin_relation_prop,
            )

            icon = sanitize_icon(page.get("icon"))

            if MIRROR_DRY_RUN:
                continue

            if source_id in mappings:
                update_page_in_mirror(mappings[source_id], props, icon=icon)
                updated += 1
            else:
                mirror_id = create_page_in_mirror(mirror_db_id, props, icon=icon)
                mappings[source_id] = mirror_id
                created += 1

            if i % 25 == 0:
                state["mappings"] = mappings
                save_state(name, state)
                print(
                    f"  ... [{name}] {i}/{len(pages)} "
                    f"(Criados={created} | Atualizados={updated} | SkippedArchived={skipped_archived} | Erros={errors})"
                )

            time.sleep(SLEEP_SEC)

        except Exception as e:
            errors += 1
            print(f"❌ [{name}] erro {i}/{len(pages)} source_id={source_id}: {e}")

    if MIRROR_DRY_RUN:
        print(f"✅ [{name}] DRY_RUN finalizado | Itens analisados={len(pages)} | SkippedArchived={skipped_archived} | Erros={errors}")
        return

    state["mappings"] = mappings
    if MIRROR_INCREMENTAL and not MIRROR_FORCE_FULL_SYNC:
        state["last_sync_time"] = now_iso_z()
    save_state(name, state)

    print(f"✅ [{name}] concluído | Criados={created} | Atualizados={updated} | SkippedArchived={skipped_archived} | Erros={errors}")


# =========================
# MAIN
# =========================
def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Faltou {name} no env.")
    return v


def main() -> None:
    require_env("NOTION_API_KEY")

    # -------------------------
    # REUNIÕES (mantém como está)
    # -------------------------
    if RUN_REUNIOES:
        src = require_env("DATABASE_ID_REUNIOES")
        dst = require_env("DATABASE_ID_REUNIOES_ESPELHO")
        REUNIOES_DATE_PROP = os.getenv("REUNIOES_DATE_PROP", "Data")

        src = resolve_database_id(src, "Reuniões/origem")
        dst = resolve_database_id(dst, "Reuniões/espelho")

        mirror_database(
            name="Reuniões",
            source_db_id=src,
            mirror_db_id=dst,
            include_only_props=["Evento", "Data", "Local", "Status", "Participantes", "Origem"],
            date_property_name=REUNIOES_DATE_PROP,
            date_from=DATE_FROM,
            sort_by_date=True,
            force_origin_relation_prop="Origem",
        )
    else:
        print("⏭️ [Reuniões] Ignorado (RUN_REUNIOES=0)")

    # -------------------------
    # YOUTUBE (mantém como está no seu fluxo atual)
    # -------------------------
    if RUN_YOUTUBE:
        src = require_env("DATABASE_ID_YOUTUBE")
        dst = require_env("DATABASE_ID_YOUTUBE_ESPELHO")

        src = resolve_database_id(src, "YouTube/origem")
        dst = resolve_database_id(dst, "YouTube/espelho")

        youtube_platform_filter = [
            {"property": "Plataforma", "multi_select": {"contains": "YouTube"}}
        ]

        mirror_database(
            name="YouTube",
            source_db_id=src,
            mirror_db_id=dst,
            include_only_props=[
                "Título",
                "Veiculação - YouTube",
                "Status - YouTube",
                "Link - YouTube",
                "Editoria - YouTube",
                "Origem",
            ],
            date_property_name="Veiculação - YouTube",
            date_from=DATE_FROM,
            sort_by_date=True,
            force_origin_relation_prop="Origem",
            extra_filters=youtube_platform_filter,
        )
    else:
        print("⏭️ [YouTube] Ignorado (RUN_YOUTUBE=0)")

    # -------------------------
    # CALENDÁRIO EDITORIAL (NOVO)
    # -------------------------
    if RUN_CALENDARIOEDITORIAL:
        src = require_env("DATABASE_ID_CALENDARIOEDITORIAL")
        dst = require_env("DATABASE_ID_CALENDARIOEDITORIAL_ESPELHO")

        src = resolve_database_id(src, "Calendário Editorial/origem")
        dst = resolve_database_id(dst, "Calendário Editorial/espelho")

        mirror_database(
            name="CalendarioEditorial",
            source_db_id=src,
            mirror_db_id=dst,
            include_only_props=[
                "Título",
                "Veiculação",
                "Plataforma",
                "Status",
                "Formato",
                "Editoria",
                "Links do post",
                "Origem",
            ],
            date_property_name="Veiculação",
            date_from=DATE_FROM,
            sort_by_date=True,
            force_origin_relation_prop="Origem",
        )
    else:
        print("⏭️ [Calendário Editorial] Ignorado (RUN_CALENDARIOEDITORIAL=0)")

    # -------------------------
    # TAREFAS GCMD
    # -------------------------
    if RUN_TAREFAS_GCMD:
        src = require_env("DATABASE_ID_TAREFAS_GCMD")
        dst = require_env("DATABASE_ID_TAREFAS_GCMD_ESPELHO")

        src = resolve_database_id(src, "Tarefas GCMD/origem")
        dst = resolve_database_id(dst, "Tarefas GCMD/espelho")

        mirror_database(
            name="Tarefas_GCMD",
            source_db_id=src,
            mirror_db_id=dst,
            include_only_props=[
                "Atividade",
                "Responsável",
                "Apoio",
                "Descrição",
                "Categoria",
                "Status",
                "Origem",
            ],
            date_property_name=None,  # tarefas não são filtradas por data
            date_from=DATE_FROM,      # ignorado quando date_property_name=None
            sort_by_date=False,
            force_origin_relation_prop="Origem",
        )
    else:
        print("⏭️ [Tarefas GCMD] Ignorado (RUN_TAREFAS_GCMD=0)")

    
    # -------------------------
    # CONTROLE DEMANDAS       
    # -------------------------
    if RUN_CONTROLE_DEMANDAS:
        src = require_env("DATABASE_ID_CONTROLE_DEMANDAS")
        dst = require_env("DATABASE_ID_CONTROLE_DEMANDAS_ESPELHO")

        src = resolve_database_id(src, "Controle Demandas/origem")
        dst = resolve_database_id(dst, "Controle Demandas/espelho")

        mirror_database(
            name="Controle_Demandas",
            source_db_id=src,
            mirror_db_id=dst,
            include_only_props=[
                "Descrição",
                "Nº ID",
                "Recebimento",
                "Recebimento de insumos",
                "Área solicitante",
                "Ação",          # rollup -> vira texto no espelho (se destino for rich_text)
                "Responsável",   # people -> vira texto no espelho (se destino for rich_text)
                "Tarefa",        # rollup -> vira texto no espelho (se destino for rich_text)
                "Origem",        # relation forçado para a página de origem
            ],
            date_property_name="Recebimento",
            date_from=DATE_FROM,  # ✅ usando a variável
            sort_by_date=True,
            force_origin_relation_prop="Origem",
            extra_filters=None,   # ✅ sem filtro adicional
        )
    else:
        print("⏭️ [Controle Demandas] Ignorado (RUN_CONTROLE_DEMANDAS=0)")


if __name__ == "__main__":
    main()
