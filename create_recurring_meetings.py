import os
import datetime
import requests
from dateutil.relativedelta import relativedelta
import traceback
import json

# 🔐 Variáveis de ambiente
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
DATABASE_ID_REUNIOES = os.getenv("DATABASE_ID_REUNIOES_TESTE")

# 🧮 Limite padrão de geração
LIMIT_DAYS = 30
MAX_MONTHS = 12

HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

RECURRING_EMOJI = "🔁"

CREATABLE_PROP_TYPES = {
    "title",
    "rich_text",
    "number",
    "select",
    "multi_select",
    "date",
    "people",
    "files",
    "checkbox",
    "url",
    "email",
    "phone_number",
    "relation"
}


def debug(*args):
    print(*args)


def get_meetings():
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID_REUNIOES}/query"
    all_results = []
    payload = {"page_size": 100}
    next_cursor = None

    while True:
        if next_cursor:
            payload["start_cursor"] = next_cursor
        r = requests.post(url, headers=HEADERS, json=payload)
        r.raise_for_status()
        data = r.json()
        all_results.extend(data.get("results", []))
        debug(f"→ carregadas {len(data.get('results', []))} (total {len(all_results)})")
        next_cursor = data.get("next_cursor")
        if not next_cursor:
            break

    return all_results


def instance_exists_for_date(base_meeting, date_to_check):
    page_id = base_meeting["id"]
    date_str = date_to_check.strftime("%Y-%m-%d")
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID_REUNIOES}/query"
    payload = {
        "filter": {
            "and": [
                {"property": "Reuniões relacionadas (recorrência)", "relation": {"contains": page_id}},
                {"property": "Data", "date": {"on_or_after": date_str}},
                {"property": "Data", "date": {"on_or_before": date_str}}
            ]
        },
        "page_size": 1
    }
    r = requests.post(url, headers=HEADERS, json=payload)
    r.raise_for_status()
    results = r.json().get("results", [])
    debug(f"    instance_exists_for_date {date_str} -> {len(results)} resultados")
    return len(results) > 0


def check_existing_instance_by_title_date(base_event, date_to_check):
    date_str = date_to_check.strftime("%Y-%m-%d")
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID_REUNIOES}/query"
    payload = {
        "filter": {
            "and": [
                {"property": "Evento", "rich_text": {"equals": base_event}},
                {"property": "Data", "date": {"on_or_after": date_str}},
                {"property": "Data", "date": {"on_or_before": date_str}}
            ]
        },
        "page_size": 1
    }
    r = requests.post(url, headers=HEADERS, json=payload)
    r.raise_for_status()
    results = r.json().get("results", [])
    debug(f"    check_existing_instance_by_title_date '{base_event}' {date_str} -> {len(results)}")
    return len(results) > 0


def _get_title_text(props):
    try:
        title_prop = props.get("Evento", {}).get("title", [])
        if title_prop and isinstance(title_prop, list):
            return title_prop[0].get("plain_text") or title_prop[0].get("text", {}).get("content", "")
    except Exception:
        pass
    return "(sem título)"


def _is_non_empty_content(prop_type, content):
    if content is None:
        return False
    if isinstance(content, list):
        return len(content) > 0
    if isinstance(content, dict):
        return len(content) > 0
    if isinstance(content, str):
        return content.strip() != ""
    if isinstance(content, (int, float, bool)):
        return True
    return True


def create_instance(base_meeting, target_date):
    try:
        props = base_meeting.get("properties", {})
        event_text = _get_title_text(props)
        page_id = base_meeting["id"]

        # Evita duplicações
        if instance_exists_for_date(base_meeting, target_date):
            debug(f"⚠️ Ignorando (já existe) {event_text} {target_date}")
            return None
        if check_existing_instance_by_title_date(event_text, target_date):
            debug(f"⚠️ Ignorando (já existe por título) {event_text} {target_date}")
            return None

        new_properties = {}

        for key, val in props.items():
            prop_type = val.get("type")
            if not prop_type or prop_type not in CREATABLE_PROP_TYPES:
                continue
            if key in ("Data", "Reuniões relacionadas (recorrência)", "Evento", "Recorrência"):
                continue

            content = val.get(prop_type)
            if not _is_non_empty_content(prop_type, content):
                continue

            if prop_type == "people":
                ids = [{"id": p["id"]} for p in content if "id" in p]
                if ids:
                    new_properties[key] = {"people": ids}
                continue

            if prop_type == "relation":
                ids = [{"id": r["id"]} for r in content if "id" in r]
                if ids:
                    new_properties[key] = {"relation": ids}
                continue

            new_properties[key] = {prop_type: content}

        # sobrescreve campos principais
        new_properties["Evento"] = {"title": [{"text": {"content": f"{RECURRING_EMOJI} {event_text}"}}]}
        new_properties["Data"] = {"date": {"start": target_date.isoformat()}}
        new_properties["Reuniões relacionadas (recorrência)"] = {"relation": [{"id": page_id}]}
        new_properties["Recorrência"] = {"select": None}  # campo vazio nas instâncias

        payload = {"parent": {"database_id": DATABASE_ID_REUNIOES}, "properties": new_properties}
        r = requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=payload)
        if r.status_code not in (200, 201):
            debug(f"❌ Erro ao criar '{event_text}' {target_date}: {r.status_code} {r.text}")
            return None

        debug(f"✅ Criada: {event_text} → {target_date}")
        return r.json()

    except Exception as e:
        debug("Exception em create_instance:", str(e))
        traceback.print_exc()
        return None


def generate_daily(base_meeting, base_date):
    limit_date = base_date + datetime.timedelta(days=LIMIT_DAYS)
    next_date = base_date + datetime.timedelta(days=1)
    while next_date <= limit_date:
        # pula fins de semana
        if next_date.weekday() < 5:
            create_instance(base_meeting, next_date)
        next_date += datetime.timedelta(days=1)


def generate_weekly(base_meeting, base_date):
    limit_date = base_date + datetime.timedelta(days=LIMIT_DAYS)
    next_date = base_date + datetime.timedelta(weeks=1)
    while next_date <= limit_date:
        create_instance(base_meeting, next_date)
        next_date += datetime.timedelta(weeks=1)


def generate_monthly(base_meeting, base_date):
    limit_date = base_date + relativedelta(months=MAX_MONTHS)
    next_date = base_date + relativedelta(months=1)
    while next_date <= limit_date:
        create_instance(base_meeting, next_date)
        next_date += relativedelta(months=1)


def count_related_instances_via_query(base_meeting):
    """
    Conta quantas páginas na base têm a relação apontando para a reunião original.
    Usa uma query ao banco para garantir contagem correta mesmo que a propriedade
    dentro da página original não reflita tudo.
    """
    page_id = base_meeting["id"]
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID_REUNIOES}/query"
    all_results = []
    payload = {
        "filter": {
            "property": "Reuniões relacionadas (recorrência)",
            "relation": {"contains": page_id}
        },
        "page_size": 100
    }
    next_cursor = None
    while True:
        if next_cursor:
            payload["start_cursor"] = next_cursor
        r = requests.post(url, headers=HEADERS, json=payload)
        r.raise_for_status()
        data = r.json()
        batch = data.get("results", [])
        all_results.extend(batch)
        next_cursor = data.get("next_cursor")
        if not next_cursor:
            break
    debug(f"    count_related_instances_via_query -> {len(all_results)}")
    return len(all_results)


def main():
    debug("🔄 Iniciando geração de reuniões recorrentes...")
    meetings = get_meetings()
    today = datetime.date.today()
    debug(f"Hoje: {today}  Reuniões carregadas: {len(meetings)}")

    for meeting in meetings:
        try:
            props = meeting.get("properties", {})
            recurrence_prop = props.get("Recorrência", {}).get("select")
            if not recurrence_prop:
                continue
            recurrence = recurrence_prop["name"].strip().lower()
            if recurrence in ("", "nenhuma"):
                continue

            data_prop = props.get("Data", {}).get("date")
            if not data_prop or not data_prop.get("start"):
                continue
            base_date = datetime.date.fromisoformat(data_prop["start"][:10])
            event = _get_title_text(props)

            # ----- CONTAGEM VIA QUERY: confere instâncias que apontam para a original -----
            existentes = count_related_instances_via_query(meeting)

            if recurrence == "diária":
                total_esperado = sum(
                    1
                    for i in range(1, LIMIT_DAYS + 1)
                    if (base_date + datetime.timedelta(days=i)).weekday() < 5
                )
            elif recurrence == "semanal":
                total_esperado = 4
            elif recurrence == "mensal":
                total_esperado = MAX_MONTHS
            else:
                total_esperado = 0

            if existentes >= total_esperado:
                debug(f"🔹 {event} ({recurrence}) já tem {existentes}/{total_esperado} instâncias relacionadas (via query). Nenhuma nova será criada.")
                continue
            # ------------------------------------------------------------------------------

            debug(f"\n🔁 {event} — recorrência: {recurrence} — base: {base_date} ({existentes}/{total_esperado} criadas)")

            if recurrence == "diária":
                generate_daily(meeting, base_date)
            elif recurrence == "semanal":
                generate_weekly(meeting, base_date)
            elif recurrence == "mensal":
                generate_monthly(meeting, base_date)
            else:
                debug(f"⚠️ Tipo desconhecido: {recurrence}")

        except Exception as e:
            debug(f"Erro no loop principal: {e}")
            traceback.print_exc()

    debug("🏁 Rotina concluída.")


if __name__ == "__main__":
    main()
