from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
import os

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID_REUNIOES_TESTE = os.getenv("DATABASE_ID_REUNIOES_TESTE")
headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}


def create_instance(base_meeting, next_date):
    """Cria uma instância da reunião recorrente."""
    base_props = base_meeting["properties"]

    # Copia todas as propriedades, exceto Data e Recorrência
    properties = {}
    for key, value in base_props.items():
        if key in ["Data", "Recorrência"]:
            continue
        properties[key] = value

    # Nova data
    properties["Data"] = {"date": {"start": next_date.strftime("%Y-%m-%d")}}

    # Campo Recorrência deve vir vazio nas instâncias
    properties["Recorrência"] = {"select": None}

    # Vincula à reunião original
    properties["Reuniões relacionadas (recorrência)"] = {
        "relation": [{"id": base_meeting["id"]}]
    }

    data = {"parent": {"database_id": DATABASE_ID_REUNIOES_TESTE}, "properties": properties}
    r = requests.post("https://api.notion.com/v1/pages", headers=headers, json=data)

    if not r.ok:
        print(
            f"❌ Erro ao criar instância para '{base_props['Nome']['title'][0]['plain_text']}' em {next_date}: {r.status_code} {r.text}"
        )
    else:
        print(
            f"✅ Instância criada para '{base_props['Nome']['title'][0]['plain_text']}' em {next_date.strftime('%Y-%m-%d')}"
        )


def generate_daily(base_meeting, base_date):
    limit_date = base_date + timedelta(days=30)
    current_date = base_date + timedelta(days=1)

    # Gera de base_date+1 até o limite (sem restrição de passado)
    while current_date <= limit_date:
        create_instance(base_meeting, current_date)
        current_date += timedelta(days=1)


def generate_weekly(base_meeting, base_date):
    limit_date = base_date + timedelta(days=30)
    current_date = base_date + timedelta(weeks=1)

    while current_date <= limit_date:
        create_instance(base_meeting, current_date)
        current_date += timedelta(weeks=1)


def generate_monthly(base_meeting, base_date):
    limit_date = base_date + relativedelta(months=12)
    current_date = base_date + relativedelta(months=1)

    while current_date <= limit_date:
        create_instance(base_meeting, current_date)
        current_date += relativedelta(months=1)


def main():
    print("🔄 Iniciando geração de reuniões recorrentes (multi-instâncias)...")

    url = f"https://api.notion.com/v1/databases/{DATABASE_ID_REUNIOES_TESTE}/query"
    r = requests.post(url, headers=headers)
    results = r.json().get("results", [])

    for meeting in results:
        props = meeting["properties"]
        recurrence = props["Recorrência"]["select"]["name"] if props["Recorrência"]["select"] else None

        if not recurrence or recurrence == "Nenhuma":
            continue

        date_str = props["Data"]["date"]["start"]
        base_date = datetime.strptime(date_str, "%Y-%m-%d")

        print(f"🔁 Processando '{props['Nome']['title'][0]['plain_text']}' — recorrência: {recurrence}")

        if recurrence == "Diária":
            generate_daily(meeting, base_date)
        elif recurrence == "Semanal":
            generate_weekly(meeting, base_date)
        elif recurrence == "Mensal":
            generate_monthly(meeting, base_date)

    print("✅ Conclusão da geração de reuniões recorrentes.")


if __name__ == "__main__":
    main()
