#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de limpeza para remover posts duplicados numa base "espelho" do Notion.

Como funciona:
- Lê todas as páginas do banco espelho indicado.
- Agrupa as páginas pela relação "Origem" (que aponta para o source_id na base original).
- Para grupos com mais de uma página (duplicatas), mantém a página editada
  mais recentemente e arquiva as demais.
- Atualiza o arquivo de estado local (.state/mirror_<nome>.json) para que o
  mapping source_id -> mirror_id aponte para a página mantida, evitando que o
  script principal recrie a duplicata arquivada na próxima execução.

Uso (primeiro rode em modo DRY RUN para conferir o que seria feito):

    NOTION_API_KEY=xxx \
    DEDUPE_MIRROR_DB_ID=<id_da_base_espelho_calendario_editorial> \
    DEDUPE_STATE_NAME=CalendarioEditorial \
    DEDUPE_ORIGEM_PROP=Origem \
    DEDUPE_DRY_RUN=1 \
    python dedupe_mirror.py

Quando estiver confiante no resultado, rode de novo com DEDUPE_DRY_RUN=0
para aplicar de verdade (arquivar as duplicatas e atualizar o estado local).

IMPORTANTE: DEDUPE_STATE_NAME precisa ser exatamente o "name" usado na
chamada de mirror_database() no script principal (ex: "CalendarioEditorial"),
para que o arquivo de estado atualizado seja o mesmo que o script principal lê.
"""

import os
import json
import time
import requests
from typing import Any, Dict, List, Optional
from collections import defaultdict

NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_VERSION = "2022-06-28"
BASE_URL = "https://api.notion.com/v1"

MIRROR_DB_ID = os.getenv("DEDUPE_MIRROR_DB_ID", "").strip()
STATE_NAME = os.getenv("DEDUPE_STATE_NAME", "").strip()          # ex: "CalendarioEditorial"
ORIGEM_PROP = os.getenv("DEDUPE_ORIGEM_PROP", "Origem").strip()
DRY_RUN = os.getenv("DEDUPE_DRY_RUN", "1").strip() == "1"        # por padrão, só simula
STATE_DIR = ".state"


def notion_headers() -> Dict[str, str]:
    if not NOTION_API_KEY:
        raise RuntimeError("Faltou NOTION_API_KEY no env.")
    return {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


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


def query_all_pages(db_id: str) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/databases/{db_id}/query"
    results: List[Dict[str, Any]] = []
    next_cursor: Optional[str] = None
    payload: Dict[str, Any] = {"page_size": 100}

    while True:
        if next_cursor:
            payload["start_cursor"] = next_cursor
        else:
            payload.pop("start_cursor", None)

        data = http_post(url, payload)
        results.extend(data.get("results", []))

        if data.get("has_more"):
            next_cursor = data.get("next_cursor")
            time.sleep(0.15)
        else:
            break

    return results


def get_origem_id(page: Dict[str, Any], origem_prop: str) -> Optional[str]:
    prop = (page.get("properties") or {}).get(origem_prop)
    if not prop or prop.get("type") != "relation":
        return None
    rel = prop.get("relation") or []
    if not rel:
        return None
    return rel[0].get("id")


def archive_page(page_id: str) -> None:
    http_patch(f"{BASE_URL}/pages/{page_id}", {"archived": True})


def state_path(name: str) -> str:
    safe = "".join(c for c in name.lower() if c.isalnum() or c in ("-", "_"))
    return os.path.join(STATE_DIR, f"mirror_{safe}.json")


def load_state(name: str) -> Dict[str, Any]:
    path = state_path(name)
    if not os.path.exists(path):
        return {"mappings": {}, "last_sync_time": None}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(name: str, state: Dict[str, Any]) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    path = state_path(name)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def main() -> None:
    if not MIRROR_DB_ID:
        raise RuntimeError("Faltou DEDUPE_MIRROR_DB_ID no env.")

    print(f"🔎 Lendo páginas do espelho {MIRROR_DB_ID} ...")
    pages = query_all_pages(MIRROR_DB_ID)
    print(f"   Total de páginas no espelho: {len(pages)}")

    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    sem_origem = 0

    for page in pages:
        origem_id = get_origem_id(page, ORIGEM_PROP)
        if not origem_id:
            sem_origem += 1
            continue
        groups[origem_id].append(page)

    if sem_origem:
        print(f"⚠️  {sem_origem} páginas no espelho sem relação '{ORIGEM_PROP}' preenchida (ignoradas).")

    duplicated_groups = {k: v for k, v in groups.items() if len(v) > 1}
    print(f"🔁 Grupos com duplicatas (mesmo source_id, mais de uma página no espelho): {len(duplicated_groups)}")

    if not duplicated_groups:
        print("✅ Nenhuma duplicata encontrada.")
        return

    state = load_state(STATE_NAME) if STATE_NAME else {"mappings": {}, "last_sync_time": None}
    mappings: Dict[str, str] = state.get("mappings", {}) or {}

    archived_count = 0

    for source_id, dup_pages in duplicated_groups.items():
        # Mantém a página editada mais recentemente; entre empates, prioriza a que não está arquivada
        dup_pages_sorted = sorted(
            dup_pages,
            key=lambda p: (not p.get("archived", False), p.get("last_edited_time", "")),
            reverse=True,
        )
        keep = dup_pages_sorted[0]
        to_archive = dup_pages_sorted[1:]

        print(
            f"\n🧩 source_id={source_id} | {len(dup_pages)} cópias no espelho"
            f" | mantendo mirror_id={keep['id']} (last_edited={keep.get('last_edited_time')})"
        )

        for p in to_archive:
            already_archived = p.get("archived", False)
            print(
                f"   {'(já arquivada) ' if already_archived else ''}"
                f"arquivando mirror_id={p['id']} (last_edited={p.get('last_edited_time')})"
            )
            if not DRY_RUN and not already_archived:
                archive_page(p["id"])
                archived_count += 1
                time.sleep(0.15)

        if STATE_NAME:
            mappings[source_id] = keep["id"]

    if STATE_NAME and not DRY_RUN:
        state["mappings"] = mappings
        save_state(STATE_NAME, state)
        print(f"\n💾 Estado local atualizado em {state_path(STATE_NAME)} (mapping aponta para as páginas mantidas).")

    if DRY_RUN:
        print("\n🧪 DRY RUN — nada foi alterado no Notion. Rode com DEDUPE_DRY_RUN=0 para aplicar de fato.")
    else:
        print(f"\n✅ Concluído. Páginas arquivadas: {archived_count}")


if __name__ == "__main__":
    main()
