import os
from datetime import datetime, timedelta
from notion_client import Client

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("DATABASE_ID_REUNIOES_TESTE")
FUTURE_OCCURRENCES = 4  # número de ocorrências futuras geradas

notion = Client(auth=NOTION_TOKEN)


def get_recurring_meetings():
    response = notion.databases.query(
        **{
            "database_id": DATABASE_ID,
            "filter": {
                "and": [
                    {"property": "Recorrência", "select": {"does_not_equal": "Nenhuma"}},
                    {"property": "Data da reunião", "date": {"is_not_empty": True}},
                ]
            },
        }
    )
    return response["results"]


def add_period(date_str, recurrence, n=1):
    """Adiciona 1 ou mais períodos (dia, semana, mês) à data original."""
    date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    if recurrence == "Diária":
        return date + timedelta(days=n)
    elif recurrence == "Semanal":
        return date + timedelta(weeks=n)
    elif recurrence == "Mensal":
        # lógica simples: soma 30 dias
        return date + timedelta(days=30 * n)
    return None


def clone_properties(properties):
    """Clona propriedades relevantes de uma página do Notion."""
    new_props = {}
    for key, prop in properties.items():
        if key in ["Data da reunião", "Última ocorrência gerada"]:
            continue

        if "title" in prop:
            new_props[key] = {"title": prop["title"]}
        elif "rich_text" in prop:
            new_props[key] = {"rich_text": prop["rich_text"]}
        elif "people" in prop:
            new_props[key] = {"people": prop["people"]}
        elif "url" in prop:
            new_props[key] = {"url": prop["url"]}
        elif "select" in prop:
            new_props[key] = {"select": prop["select"]}
        elif "multi_select" in prop:
            new_props[key] = {"multi_select": prop["multi_select"]}
        elif "checkbox" in prop:
            new_props[key] = {"checkbox": prop["checkbox"]}
        elif "number" in prop:
            new_props[key] = {"number": prop["number"]}

    return new_props


def create_meeting(properties, new_date):
    """Cria uma nova reunião no Notion com data atualizada."""
    new_props = clone_properties(properties)
    iso_date = new_date.isoformat()

    new_props["Data da reunião"] = {"date": {"start": iso_date}}
    new_props["Última ocorrência gerada"] = {"date": {"start": iso_date}}

    notion.pages.create(
        parent={"database_id": DATABASE_ID},
        properties=new_props
    )


def update_last_generated(page_id, date_obj):
    """Atualiza o campo 'Última ocorrência gerada' da página original."""
    notion.pages.update(
        page_id=page_id,
        properties={
            "Última ocorrência gerada": {"date": {"start": date_obj.isoformat()}}
        },
    )


def main():
    meetings = get_recurring_meetings()
    for page in meetings:
        props = page["properties"]
        recurrence = props["Recorrência"]["select"]["name"]
        date_start = props["Data da reunião"]["date"]["start"]
        last_generated = props.get("Última ocorrência gerada", {}).get("date", {}).get("start")

        if not date_start:
            continue

        last_generated_date = datetime.fromisoformat(
            last_generated.replace("Z", "+00:00")
        ) if last_generated else datetime.fromisoformat(date_start.replace("Z", "+00:00"))

        for i in range(1, FUTURE_OCCURRENCES + 1):
            new_date = add_period(date_start, recurrence, i)
            if new_date and new_date > last_generated_date:
                create_meeting(props, new_date)
                last_generated_date = new_date
                print(f"✔ Nova reunião ({recurrence}) criada para {new_date.date()}")

        update_last_generated(page["id"], last_generated_date)


if __name__ == "__main__":
    main()
