import os
import datetime
import requests

# 🔐 Variáveis de ambiente (consistentes com seus outros scripts)
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
DATABASE_ID_REUNIOES = os.getenv("DATABASE_ID_REUNIOES_TESTE")

# 🧮 Limite de dias futuros para criar instâncias (padrão: 30 dias)
LIMIT_DAYS = int(os.getenv("RECURRING_LIMIT_DAYS", "30"))

HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

# 🏷️ Emoji que marca as instâncias criadas automaticamente
RECURRING_EMOJI = "🔁"


def get_meetings():
    """Busca todas as reuniões na base"""
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID_REUNIOES}/query"
    payload = {"page_size": 100}
    response = requests.post(url, headers=HEADERS, json=payload)
    response.raise_for_status()
    return response.json().get("results", [])


def check_existing_instance(base_event, date_to_check):
    """Verifica se já existe uma instância para o mesmo evento e data"""
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID_REUNIOES}/query"
    payload = {
        "filter": {
            "and": [
                {"property": "Evento", "rich_text": {"contains": base_event}},
                {"property": "Data", "date": {"on_or_after": date_to_check.isoformat()}},
                {"property": "Data", "date": {"on_or_before": date_to_check.isoformat()}}
            ]
        }
    }
    response = requests.post(url, headers=HEADERS, json=payload)
    response.raise_for_status()
    results = response.json().get("results", [])
    return len(results) > 0


def create_meeting(base_meeting, new_date):
    """Cria uma nova instância de reunião recorrente"""
    props = base_meeting["properties"]
    event = props["Evento"]["title"][0]["plain_text"]
    recurrence = props["Recorrência"]["select"]["name"]
    page_id = base_meeting["id"]

    # Evita duplicar instâncias
    if check_existing_instance(event, new_date):
        print(f"⚠️ Já existe uma instância de '{event}' em {new_date}")
        return None

    new_event = f"{RECURRING_EMOJI} {event}"
    new_page = {
        "parent": {"database_id": DATABASE_ID_REUNIOES},
        "properties": {
            "Evento": {"title": [{"text": {"content": new_event}}]},
            "Data": {"date": {"start": new_date.isoformat()}},
            "Recorrência": {"select": {"name": recurrence}},
            "Reunião Original": {"relation": [{"id": page_id}]},
        }
    }

    r = requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=new_page)
    r.raise_for_status()
    print(f"✅ Instância criada: {new_event} ({new_date})")
    return r.json()


def delete_recurring_instances():
    """Apaga instâncias órfãs (sem reunião original)"""
    print("🧹 Limpando instâncias órfãs...")
    meetings = get_meetings()
    for meeting in meetings:
        event_prop = meeting["properties"].get("Evento", {}).get("title", [])
        if not event_prop:
            continue
        event = event_prop[0]["plain_text"]

        if event.startswith(RECURRING_EMOJI):
            origem = meeting["properties"].get("Reunião Original", {}).get("relation", [])
            if not origem:
                page_id = meeting["id"]
                print(f"🗑️ Apagando instância órfã: {event}")
                url = f"https://api.notion.com/v1/pages/{page_id}"
                payload = {"archived": True}
                requests.patch(url, headers=HEADERS, json=payload)


def main():
    print("🔄 Iniciando geração de reuniões recorrentes...")
    meetings = get_meetings()
    today = datetime.date.today()
    limit_date = today + datetime.timedelta(days=LIMIT_DAYS)

    for meeting in meetings:
        props = meeting["properties"]
        recurrence_prop = props.get("Recorrência", {}).get("select")
        if not recurrence_prop:
            continue

        recurrence = recurrence_prop["name"].lower().strip()

        # Recorrência desativada
        if recurrence in ("", "nenhuma"):
            continue

        data_prop = props.get("Data", {}).get("date")
        if not data_prop:
            continue

        base_date = datetime.date.fromisoformat(data_prop["start"][:10])
        event = props["Evento"]["title"][0]["plain_text"]

        # Define a próxima data de acordo com a recorrência
        if recurrence == "diária":
            next_date = base_date + datetime.timedelta(days=1)
        elif recurrence == "semanal":
            next_date = base_date + datetime.timedelta(weeks=1)
        elif recurrence == "mensal":
            # Adiciona um mês respeitando mudanças de tamanho do mês
            year, month = base_date.year, base_date.month
            new_month = month + 1 if month < 12 else 1
            new_year = year if month < 12 else year + 1
            day = min(base_date.day, [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][new_month - 1])
            next_date = datetime.date(new_year, new_month, day)
        else:
            continue

        if next_date > limit_date:
            print(f"🚫 Ignorando '{event}' — próxima data ({next_date}) ultrapassa o limite de {LIMIT_DAYS} dias.")
            continue

        if next_date > today:
            print(f"➕ Criando instância futura de '{event}' para {next_date}")
            create_meeting(meeting, next_date)

    delete_recurring_instances()
    print("🏁 Rotina concluída com sucesso.")


if __name__ == "__main__":
    main()
