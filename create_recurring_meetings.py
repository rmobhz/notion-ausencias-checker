import os
import datetime
import requests
from dateutil.relativedelta import relativedelta
import json
import traceback

# Config
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
DATABASE_ID_REUNIOES = os.getenv("DATABASE_ID_REUNIOES_TESTE")  # <— fixado para o ambiente de teste
LIMIT_DAYS = int(os.getenv("RECURRING_LIMIT_DAYS", "30"))
MAX_MONTHS = os.getenv("RECURRING_MAX_MONTHS", "12")
MAX_MONTHS = int(MAX_MONTHS) if MAX_MONTHS and MAX_MONTHS.isdigit() else None

HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

RECURRING_EMOJI = "🔁"
VERBOSE = True  # Ative/desative logs detalhados


def debug(*args, **kwargs):
    if VERBOSE:
        print(*args, **kwargs)


def safe_json(d):
    try:
        return json.dumps(d, ensure_ascii=False, default=str)
    except Exception:
        return str(d)


def get_all_meetings():
    """Obtém todas as reuniões, paginando a API até acabar."""
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID_REUNIOES}/query"
    payload = {"page_size": 100}
    results = []
    page_count = 0

    while True:
        page_count += 1
        r = requests.post(url, headers=HEADERS, json=payload)
        try:
            r.raise_for_status()
        except Exception:
            debug(f"ERRO ao buscar página {page_count}: {r.status_code} {r.text}")
            raise

        data = r.json()
        batch = data.get("results", [])
        results.extend(batch)
        debug(f"→ Página {page_count}: {len(batch)} reuniões carregadas (total até agora: {len(results)})")

        next_cursor = data.get("next_cursor")
        if not next_cursor:
            break
        payload["start_cursor"] = next_cursor

    debug(f"✅ Total de reuniões carregadas: {len(results)}\n")
    return results


def instance_exists_for_date(base_meeting, date_to_check):
    """Verifica se já existe instância relacionada na data indicada."""
    page_id = base_meeting["id"]
    date_str = date_to_check.strftime("%Y-%m-%d")
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID_REUNIOES}/query"
    payload = {
        "filter": {
            "and": [
                {"property": "Reunião original", "relation": {"contains": page_id}},
                {"property": "Data", "date": {"on_or_after": date_str}},
                {"property": "Data", "date": {"on_or_before": date_str}}
            ]
        }
    }
    r = requests.post(url, headers=HEADERS, json=payload)
    r.raise_for_status()
    results = r.json().get("results", [])
    debug(f"    -> instance_exists_for_date({date_str}): {len(results)} resultados")
    return len(results) > 0


def check_existing_instance_by_title_date(base_event, date_to_check):
    """Verifica se já existe qualquer página com mesmo título e data."""
    date_str = date_to_check.strftime("%Y-%m-%d")
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID_REUNIOES}/query"
    payload = {
        "filter": {
            "and": [
                {"property": "Evento", "rich_text": {"equals": base_event}},
                {"property": "Data", "date": {"on_or_after": date_str}},
                {"property": "Data", "date": {"on_or_before": date_str}}
            ]
        }
    }
    r = requests.post(url, headers=HEADERS, json=payload)
    r.raise_for_status()
    results = r.json().get("results", [])
    debug(f"    -> check_existing_instance_by_title_date('{base_event}', {date_str}): {len(results)} resultados")
    return len(results) > 0


def create_instance(base_meeting, target_date):
    try:
        props = base_meeting["properties"]
        event = props.get("Evento", {}).get("title", [])
        event = event[0]["plain_text"] if event else "(sem título)"
        recurrence = props.get("Recorrência", {}).get("select", {}).get("name", "(sem recorrência)")
        page_id = base_meeting["id"]

        debug(f"  -> Tentando criar instância para '{event}' em {target_date} (origem id={page_id})")

        if instance_exists_for_date(base_meeting, target_date):
            debug(f"     - PULANDO: já existe instância relacionada para {target_date}")
            return None
        if check_existing_instance_by_title_date(event, target_date):
            debug(f"     - PULANDO: já existe página com mesmo título/data para {target_date}")
            return None

        new_event = f"{RECURRING_EMOJI} {event}"
        payload = {
            "parent": {"database_id": DATABASE_ID_REUNIOES},
            "properties": {
                "Evento": {"title": [{"text": {"content": new_event}}]},
                "Data": {"date": {"start": target_date.isoformat()}},
                "Recorrência": {"select": {"name": recurrence}},
                "Reunião original": {"relation": [{"id": page_id}]},
            }
        }

        r = requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=payload)
        if r.status_code not in (200, 201):
            debug(f"     - ERRO criando página: {r.status_code} {r.text}")
            r.raise_for_status()
        debug(f"✅ Instância criada: {new_event} → {target_date}")
        return r.json()
    except Exception as e:
        debug("Exception em create_instance:", str(e))
        traceback.print_exc()
        return None


def generate_daily(base_meeting, base_date, today, limit_date):
    start = base_date + datetime.timedelta(days=1)
    debug(f"  ▶ generate_daily: {start} até {limit_date}")
    next_date = start
    while next_date <= limit_date:
        if next_date.weekday() in (5, 6):
            debug(f"    - pulando fim de semana ({next_date})")
        else:
            create_instance(base_meeting, next_date)
        next_date += datetime.timedelta(days=1)


def generate_weekly(base_meeting, base_date, today, limit_date):
    start = base_date + datetime.timedelta(weeks=1)
    debug(f"  ▶ generate_weekly: {start} até {limit_date}")
    next_date = start
    while next_date <= limit_date:
        create_instance(base_meeting, next_date)
        next_date += datetime.timedelta(weeks=1)


def generate_monthly(base_meeting, base_date, today, limit_date):
    start = base_date + relativedelta(months=1)
    debug(f"  ▶ generate_monthly: {start} até {limit_date} (MAX_MONTHS={MAX_MONTHS})")
    next_date = start
    months_created = 0
    while next_date <= limit_date:
        if MAX_MONTHS and months_created >= MAX_MONTHS:
            debug(f"    - atingiu MAX_MONTHS ({MAX_MONTHS}), parando.")
            break
        create_instance(base_meeting, next_date)
        months_created += 1
        next_date += relativedelta(months=1)


def main():
    debug("🔄 Iniciando rotina com paginação completa...\n")
    meetings = get_all_meetings()
    today = datetime.date.today()
    limit_date = today + datetime.timedelta(days=LIMIT_DAYS)
    debug(f"Hoje: {today} | Limit_date: {limit_date} | Total reuniões: {len(meetings)}\n")

    for i, meeting in enumerate(meetings, start=1):
        try:
            props = meeting.get("properties", {})
            recurrence_raw = props.get("Recorrência")
            recurrence_prop = recurrence_raw.get("select") if recurrence_raw else None

            if not recurrence_prop:
                continue

            recurrence_name = recurrence_prop.get("name")
            recurrence_norm = recurrence_name.lower().strip() if recurrence_name else ""
            if recurrence_norm in ("", "nenhuma"):
                continue

            data_raw = props.get("Data", {}).get("date")
            if not data_raw or not data_raw.get("start"):
                continue
            base_date = datetime.date.fromisoformat(data_raw["start"][:10])

            if base_date > limit_date:
                continue

            debug(f"\n[{i}] Reunião com recorrência '{recurrence_name}' ({meeting['id']}) | Data base: {base_date}")

            if recurrence_norm == "diária":
                generate_daily(meeting, base_date, today, limit_date)
            elif recurrence_norm == "semanal":
                generate_weekly(meeting, base_date, today, limit_date)
            elif recurrence_norm == "mensal":
                generate_monthly(meeting, base_date, today, limit_date)
            else:
                debug(f"  -> Tipo de recorrência desconhecido: {recurrence_name}")

        except Exception as e:
            debug(f"Erro processando reunião {i}: {e}")
            traceback.print_exc()

    debug("\n🏁 Rotina concluída.")


if __name__ == "__main__":
    main()
