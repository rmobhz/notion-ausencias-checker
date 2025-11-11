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


def check_existing_instance(base_title, date_to_check):
    """Verifica se já existe uma instância para o mesmo título e data"""
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID_REUNIOES}/query"
    payload = {
        "filter": {
            "and": [
                {"property": "Título", "rich_text": {"contains": base_title}},
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
    title = props["Título"]["title"][0]["plain_text"]
    recurrence = props["Recorrência"]["select"]["name"]
    page_id = base_meeting["id"]

    # Evita duplicar instâncias
    if check_existing_instance(title, new_date):
        print(f"⚠️ Já existe uma instância de '{title}' em {new_date}")
        return None

    new_title = f"{RECURRING_EMOJI} {title}"
    new_page = {
        "parent": {"database_id": DATABASE_ID_REUNIOES},
        "properties": {
            "Título": {"title": [{"text": {"content": new_title}}]},
            "Data": {"date": {"start": new_date.isoformat()}},
            "Recorrência": {"select": {"name": recurrence}},
            "Origem": {"relation": [{"id": page_id}]} if "Origem" in props else None,
        }
    }
    new_page["properties"] = {k: v for k, v in new_page["properties"].items() if v is not None}

    r = requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=new_page)
    r.raise_for_status()
    print(f"✅ Instância criada: {new_title} ({new_date})")
    return r.json()


def delete_recurring_instances():
    """Apaga instâncias órfãs (sem origem)"""
    print("🧹 Limpando instâncias órfãs...")
    meetings = get_meetings()
    for meeting in meetings:
        title_prop = meeting["properties"].get("Título", {}).get("title", [])
        if not title_prop:
            continue
        title = title_prop[0]["plain_text"]

        if title.startswith(RECURRING_EMOJI):
            origem = meeting["properties"].get("Origem", {}).get("relation", [])
            if not origem:
                page_id = meeting["id"]
                print(f"🗑️ Apagando instância órfã: {title}")
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
        if "Recorrência" not in props or not props["Recorrência"]["select"]:
            continue

        recurrence = props["Recorrência"]["select"]["name"].lower()
        data_prop = props.get("Data", {}).get("date")
        if not data_prop:
            continue

        base_date = datetime.date.fromisoformat(data_prop["start"][:10])
        title = props["Título"]["title"][0]["plain_text"]

        # Permite pausar uma recorrência via checkbox "Ativo"
        if "Ativo" in props and props["Ativo"].get("checkbox") is False:
            print(f"⏸️ Recorrência pausada para '{title}'")
            continue

        if recurrence == "diária":
            next_date = base_date + datetime.timedelta(days=1)
        elif recurrence == "semanal":
            next_date = base_date + datetime.timedelta(days=7)
        else:
            continue

        if next_date > limit_date:
            print(f"🚫 Ignorando '{title}' — próxima data ({next_date}) ultrapassa o limite de {LIMIT_DAYS} dias.")
            continue

        if next_date > today:
            print(f"➕ Criando instância futura de '{title}' para {next_date}")
            create_meeting(meeting, next_date)

    delete_recurring_instances()
    print("🏁 Rotina concluída com sucesso.")


if __name__ == "__main__":
    main()
