import os
import datetime
import requests
from dateutil.relativedelta import relativedelta

# 🔐 Variáveis de ambiente
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
DATABASE_ID_REUNIOES = os.getenv("DATABASE_ID_REUNIOES_TESTE")

# 🧮 Limite de dias futuros para criar instâncias (padrão: 30 dias)
LIMIT_DAYS = int(os.getenv("RECURRING_LIMIT_DAYS", "30"))
# Opcional: limite de meses para recorrência mensal (None = sem limite extra)
MAX_MONTHS = os.getenv("RECURRING_MAX_MONTHS", "12")
MAX_MONTHS = int(MAX_MONTHS) if MAX_MONTHS and MAX_MONTHS.isdigit() else None

HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

RECURRING_EMOJI = "🔁"


def get_meetings():
    """Obtém todas as reuniões do banco."""
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID_REUNIOES}/query"
    payload = {"page_size": 100}
    response = requests.post(url, headers=HEADERS, json=payload)
    response.raise_for_status()
    return response.json().get("results", [])


def instance_exists_for_date(base_meeting, date_to_check):
    """Verifica se já existe uma instância gerada desta 'Reunião original' na data indicada."""
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
    return len(r.json().get("results", [])) > 0


def check_existing_instance_by_title_date(base_event, date_to_check):
    """Verifica se já existe qualquer página com mesmo Evento e mesma data (checagem extra)."""
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
    return len(r.json().get("results", [])) > 0


def create_instance(base_meeting, target_date):
    """Cria uma nova instância da reunião recorrente."""
    props = base_meeting["properties"]
    event = props["Evento"]["title"][0]["plain_text"]
    recurrence = props["Recorrência"]["select"]["name"]
    page_id = base_meeting["id"]

    # segurança dupla: se já existir por relação/data ou por título/data, pula
    if instance_exists_for_date(base_meeting, target_date):
        print(f"⚠️ Instância já existe por relação: '{event}' em {target_date}")
        return None
    if check_existing_instance_by_title_date(event, target_date):
        print(f"⚠️ Instância já existe por título: '{event}' em {target_date}")
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
    r.raise_for_status()
    print(f"✅ Instância criada: {new_event} → {target_date}")
    return r.json()


def generate_daily(base_meeting, base_date, today, limit_date):
    next_date = base_date + datetime.timedelta(days=1)
    while next_date <= limit_date:
        if next_date <= today:
            next_date += datetime.timedelta(days=1)
            continue
        if next_date.weekday() in (5, 6):  # pula sábado e domingo
            print(f"⏭️ Pulando fim de semana: {next_date}")
            next_date += datetime.timedelta(days=1)
            continue
        create_instance(base_meeting, next_date)
        next_date += datetime.timedelta(days=1)


def generate_weekly(base_meeting, base_date, today, limit_date):
    next_date = base_date + datetime.timedelta(weeks=1)
    while next_date <= limit_date:
        if next_date > today:
            create_instance(base_meeting, next_date)
        next_date += datetime.timedelta(weeks=1)


def generate_monthly(base_meeting, base_date, today, limit_date):
    next_date = base_date + relativedelta(months=1)
    months_created = 0
    while next_date <= limit_date:
        if MAX_MONTHS is not None and months_created >= MAX_MONTHS:
            break
        if next_date > today:
            create_instance(base_meeting, next_date)
            months_created += 1
        next_date += relativedelta(months=1)


def main():
    print("🔄 Iniciando geração de reuniões recorrentes (multi-instâncias)...")
    meetings = get_meetings()
    today = datetime.date.today()
    limit_date = today + datetime.timedelta(days=LIMIT_DAYS)

    for meeting in meetings:
        props = meeting["properties"]
        recurrence_prop = props.get("Recorrência", {}).get("select")
        if not recurrence_prop:
            continue

        recurrence = recurrence_prop["name"].lower().strip()
        if recurrence in ("", "nenhuma"):
            continue

        data_prop = props.get("Data", {}).get("date")
        if not data_prop:
            continue

        base_date = datetime.date.fromisoformat(data_prop["start"][:10])
        event = props["Evento"]["title"][0]["plain_text"]

        if base_date < today:
            print(f"⏸️ Ignorando '{event}' — data base {base_date} já passou.")
            continue

        print(f"🔁 Processando '{event}' — recorrência: {recurrence}")

        if recurrence == "diária":
            generate_daily(meeting, base_date, today, limit_date)
        elif recurrence == "semanal":
            generate_weekly(meeting, base_date, today, limit_date)
        elif recurrence == "mensal":
            generate_monthly(meeting, base_date, today, limit_date)
        else:
            print(f"⚠️ Tipo de recorrência desconhecido: {recurrence}")

    print("🏁 Rotina concluída com sucesso.")


if __name__ == "__main__":
    main()
