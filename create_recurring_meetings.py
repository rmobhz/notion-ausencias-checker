import os
from datetime import datetime, timedelta
from notion_client import Client

# === Configurações ===
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
DATABASE_ID_REUNIOES = os.getenv("DATABASE_ID_REUNIOES_TESTE")
FUTURE_OCCURRENCES = 4  # número de reuniões futuras a gerar

notion = Client(auth=NOTION_API_KEY)


def get_recurring_meetings():
    """Obtém as reuniões com recorrência ativa na base do Notion."""
    response = notion.databases.query(
        **{
            "database_id": DATABASE_ID_REUNIOES,
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
    """Retorna a próxima data de acordo com a recorrência."""
    date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    if recurrence == "Diária":
        return date + timedelta(days=n)
    elif recurrence == "Semanal":
        return date + timedelta(weeks=n)
    elif recurrence == "Mensal":
        # aproximação: soma 30 dias por mês
        return date + timedelta(days=30 * n)
    return None


def clone_properties(properties):
    """Copia propriedades relevantes da página original."""
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
    """Cria uma nova reunião com base na original."""
    new_props = clone_properties(properties)
    iso_date = new_date.isoformat()

    new_props["Data da reunião"] = {"date": {"start": iso_date}}
    new_props["Última ocorrência gerada"] = {"date": {"start": iso_date}}

    notion.pages.create(
        parent={"database_id": DATABASE_ID_REUNIOES},
        properties=new_props
    )


def update_last_generated(page_id, date_obj):
    """Atualiza a data da última ocorrência gerada na reunião original."""
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
