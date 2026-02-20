#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
sync_espelho.py — Um único script para espelhar múltiplas bases no Notion

✅ Atualização (Reuniões)
- Reuniões agora NÃO copia tudo.
- Copia SOMENTE: Evento, Data, Local, Status, Participantes e Origem.
- Participantes: se na origem for "people" e no espelho for "rich_text",
  converte para texto "Nome 1 | Nome 2 | ...".
- Origem: força a relação do espelho apontando para a página original (source_id),
  desde que a propriedade "Origem" no espelho seja do tipo relation e esteja ligada
  ao DB original de Reuniões.

⚠️ Importante (Incremental)
- Para rodar incremental de verdade, configure:
  MIRROR_INCREMENTAL=1
  MIRROR_FORCE_FULL_SYNC=0
"""

import os
import json
import time
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

# Segurança / teste
MIRROR_DRY_RUN = os.getenv("MIRROR_DRY_RUN", "0").strip() == "1"   # se 1, não cria/atualiza
MIRROR_LIMIT = int(os.getenv("MIRROR_LIMIT", "0").strip())         # 0 = sem limite; >0 limita itens

# Execução
MIRROR_FORCE_FULL_SYNC = os.getenv("MIRROR_FORCE_FULL_SYNC", "1").strip() == "1"
MIRROR_INCREMENTAL = os.getenv("MIRROR_INCREMENTAL", "0").strip() == "1"
MIRROR_UPDATE_ARCHIVED = os.getenv("MIRROR_UPDATE_ARCHIVED", "1").strip() == "1"

MIRROR_SLEEP_MS = int(os.getenv("MIRROR_SLEEP_MS", "150").strip())
SLEEP_SEC = max(0, MIRROR_SLEEP_MS) / 1000.0

# A partir de 2026
DATE_FROM = "2026-01-01"

# =========================
# HTTP helpers
# =========================
def notion_headers() -> Dict[str, str]:
    if not NOTION_API_KEY:
        raise RuntimeError("Faltou NOTION_API_KEY no env.")
    return {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def http_get(url: str) -> Dict[str, Any]:
    r = requests.get(url, headers=notion_headers(), timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} GET {url}\n{r.text}")
    return r.json()


def http_post(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.post(url, headers=notion_headers(), json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} POST {url}\n{r.text}")
    return r.json()


def http_patch(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.patch(url, headers=notion_headers(), json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} PATCH {url}\n{r.text}")
    return r.json()


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
# Utilitários Notion props
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


def build_property_payload(prop: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Converte uma propriedade da API (origem) para payload de escrita (destino).
    Retorna None para tipos não graváveis.
    """
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
        return {"rich_text": [{"type": "text", "text": {"content": text}}]} if text else {"rich_text": []}
    if t == "number":
        return {"number": prop.get("number")}
    if t == "select":
        sel = prop.get("select")
        return {"select": sel} if sel else {"select": None}
    if t == "multi_select":
        return {"multi_select": prop.get("multi_select", [])}
    if t == "status":
        st = prop.get("status")
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
    """
    Monta o payload de properties a escrever no destino.

    - include_only_props: whitelist de nomes de propriedades.
    - mirror_db_schema: schema do DB destino para permitir conversões (ex.: people->rich_text).
    - force_origin_relation_prop:
        se informado (ex.: "Origem"), força escrever relation apontando para source_id
        (útil para manter vínculo com o item original).
    """
    src_props = source_page.get("properties", {}) or {}
    dest: Dict[str, Any] = {}

    # Mapa do schema do destino: nome -> tipo
    dest_prop_types: Dict[str, str] = {}
    if mirror_db_schema:
        for pname, pdef in (mirror_db_schema.get("properties", {}) or {}).items():
            ptype = pdef.get("type")
            if ptype:
                dest_prop_types[pname] = ptype

    for prop_name, prop in src_props.items():
        if include_only_props is not None and prop_name not in include_only_props:
            continue

        # Conversão especial: origem people -> destino rich_text (ex.: "Participantes")
        if prop.get("type") == "people" and dest_prop_types.get(prop_name) == "rich_text":
            txt = plain_text_from_people(prop)
            dest[prop_name] = {"rich_text": [{"type": "text", "text": {"content": txt}}]} if txt else {"rich_text": []}
            continue

        payload = build_property_payload(prop)
        if payload is not None:
            dest[prop_name] = payload

    # Força a relação "Origem" no espelho apontando para o source_id
    if include_only_props is not None and force_origin_relation_prop:
        if force_origin_relation_prop in include_only_props:
            # Só escreve se no destino a prop existir e for relation; se não, deixa claro no log depois (via erro do Notion)
            if dest_prop_types.get(force_origin_relation_prop) == "relation":
                dest[force_origin_relation_prop] = {"relation": [{"id": source_page["id"]}]}

    return dest


# =========================
# Query com filtro de data (>= 2026-01-01)
# =========================
def query_database_pages(
    db_id: str,
    date_property_name: Optional[str],
    date_from: str,
    incremental_after: Optional[str],
    sorts: Optional[List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/databases/{db_id}/query"

    filters: List[Dict[str, Any]] = []

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
# Upsert
# =========================
def create_page_in_mirror(mirror_db_id: str, properties: Dict[str, Any]) -> str:
    url = f"{BASE_URL}/pages"
    payload = {"parent": {"database_id": mirror_db_id}, "properties": properties}
    data = http_post(url, payload)
    return data["id"]


def update_page_in_mirror(page_id: str, properties: Dict[str, Any]) -> None:
    url = f"{BASE_URL}/pages/{page_id}"
    payload = {"properties": properties}
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
) -> None:
    state = load_state(name)
    mappings: Dict[str, str] = state.get("mappings", {}) or {}

    incremental_after = None
    if MIRROR_INCREMENTAL and not MIRROR_FORCE_FULL_SYNC:
        incremental_after = state.get("last_sync_time")

    sorts = None
    if sort_by_date and date_property_name:
        sorts = [{"property": date_property_name, "direction": "ascending"}]

    # Schema do destino para suportar conversões (people->rich_text, e validar Origem relation)
    mirror_schema = get_database_schema(mirror_db_id)

    pages = query_database_pages(
        db_id=source_db_id,
        date_property_name=date_property_name,
        date_from=date_from,
        incremental_after=incremental_after,
        sorts=sorts,
    )

    mode = "DRY_RUN" if MIRROR_DRY_RUN else ("FULL" if MIRROR_FORCE_FULL_SYNC or not MIRROR_INCREMENTAL else "INCR")
    print(
        f"🔄 [{name}] mode={mode}"
        f" | origem_itens={len(pages)} | from={date_from} | date_prop={date_property_name or 'N/A'}"
        + (f" | incremental_after={incremental_after}" if incremental_after else "")
    )

    # limita itens (modo teste)
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

            if MIRROR_DRY_RUN:
                continue  # não grava nem altera estado

            if source_id in mappings:
                update_page_in_mirror(mappings[source_id], props)
                updated += 1
            else:
                mirror_id = create_page_in_mirror(mirror_db_id, props)
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

    # checkpoint final
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
    # REUNIÕES
    # -------------------------
    if RUN_REUNIOES:
        src = require_env("DATABASE_ID_REUNIOES")
        dst = require_env("DATABASE_ID_REUNIOES_ESPELHO")
        REUNIOES_DATE_PROP = os.getenv("REUNIOES_DATE_PROP", "Data")

        # Resolve IDs caso alguém tenha passado PAGE ID
        src = resolve_database_id(src, "Reuniões/origem")
        dst = resolve_database_id(dst, "Reuniões/espelho")

        # ✅ Só publica um subconjunto de props
        # Obs.: "Origem" aqui é a relation no espelho que aponta para a página original (source_id).
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
    # YOUTUBE
    # -------------------------
    if RUN_YOUTUBE:
        src = require_env("DATABASE_ID_YOUTUBE")
        dst = require_env("DATABASE_ID_YOUTUBE_ESPELHO")

        src = resolve_database_id(src, "YouTube/origem")
        dst = resolve_database_id(dst, "YouTube/espelho")

        mirror_database(
            name="YouTube",
            source_db_id=src,
            mirror_db_id=dst,
            include_only_props=["Título", "Veiculação - YouTube", "Status - YouTube"],
            date_property_name="Veiculação - YouTube",
            date_from=DATE_FROM,
            sort_by_date=True,
        )
    else:
        print("⏭️ [YouTube] Ignorado (RUN_YOUTUBE=0)")


if __name__ == "__main__":
    main()
