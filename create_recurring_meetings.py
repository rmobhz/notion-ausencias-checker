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

# Tipos de propriedade que podemos setar ao criar uma página
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


def get_meetings():
    """Obtém todas as reuniões do banco, com suporte à paginação."""
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID_REUNIOES}/query"
    all_results = []
    payload = {"page_size": 100}
    has_more = True
    next_cursor = None

    while has_more:
        if next_cursor:
            payload["start_cursor"] = next_cursor
        response = requests.post(url, headers=HEADERS, json=payload)
        response.raise_for_status()
        data = response.json()
        all_results.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        next_cursor = data.get("next_cursor")

    return all_results


def instance_exists_for_date(base_meeting, date_to_check):
    """Verifica se já existe uma instância gerada desta 'Reuniões relacionadas (recorrência)' na data indicada."""
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


def _get_title_text(props):
    """Pega texto do título base de forma segura."""
    try:
        title_prop = props.get("Evento", {}).get("title", [])
        if title_prop and isinstance(title_prop, list):
            return title_prop[0].get("plain_text") or title_prop[0].get("text", {}).get("content", "")
    except Exception:
        pass
    return "(sem título)"


def create_instance(base_meeting, target_date):
    """Cria uma nova instância da reunião recorrente copiando propriedades válidas."""
    props = base_meeting.get("properties", {})
    event_text = _get_title_text(props)
    recurrence = None
    try:
        recurrence = props.get("Recorrência", {}).get("select", {}).get("name")
    except Exception:
        recurrence = None
    page_id = base_meeting["id"]

    # segurança dupla: se já existir por relação/data ou por título/data, pula
    if instance_exists_for_date(base_meeting, target_date):
        print(f"⚠️ Instância já existe por relação: '{event_text}' em {target_date}")
        return None
    if check_existing_instance_by_title_date(event_text, target_date):
        print(f"⚠️ Instância já existe por título: '{event_text}' em {target_date}")
        return None

    # --- Monta propriedades copiadas apenas das que são criáveis ---
    new_properties = {}

    for key, val in props.items():
        # Se propriedade não tem 'type' (incomum), pule
        prop_type = val.get("type")
        if not prop_type or prop_type not in CREATABLE_PROP_TYPES:
            continue

        # Evita copiar propriedades que vamos sobrescrever
        if key in ("Data", "Reuniões relacionadas (recorrência)", "Evento"):
            continue

        # Para títulos/rich_text/select/multi_select/date/people/checkbox/url/email/etc,
        # o retorno da API costuma já estar no formato aceito - então copiamos 'val[prop_type]'.
        # Ex.: val = {"id": "...", "type":"select", "select": {"name":"X"}}
        # Precisamos enviar {"select": {"name":"X"}}
        try:
            new_properties[key] = {prop_type: val.get(prop_type)}
        except Exception:
            # fallback: tente usar o valor bruto
            new_properties[key] = val

    # Define a nova data (substitui)
    new_properties["Data"] = {"date": {"start": target_date.isoformat()}}

    # Título: coloca emoji e mantém texto
    new_title_text = f"{RECURRING_EMOJI} {event_text}"
    new_properties["Evento"] = {"title": [{"text": {"content": new_title_text}}]}

    # Reuniões relacionadas (recorrência) relation apontando para a origem
    new_properties["Reuniões relacionadas (recorrência)"] = {"relation": [{"id": page_id}]}

    # Mantém Recorrência se existir
    if recurrence:
        new_properties["Recorrência"] = {"select": {"name": recurrence}}

    # POST para criar a página
    payload = {
        "parent": {"database_id": DATABASE_ID_REUNIOES},
        "properties": new_properties
    }

    r = requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=payload)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        # mostrar erro mais informativo
        print(f"❌ Erro ao criar instância para '{event_text}' em {target_date}: {r.status_code} {r.text}")
        raise
    print(f"✅ Instância criada: {new_title_text} → {target_date}")
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
        props = meeting.get("properties", {})
        recurrence_prop = props.get("Recorrência", {}).get("select")
        if not recurrence_prop:
            continue

        recurrence = recurrence_prop["name"].strip().lower()
        if recurrence in ("", "nenhuma"):
            continue

        data_prop = props.get("Data", {}).get("date")
        if not data_prop:
            continue

        base_date = datetime.date.fromisoformat(data_prop["start"][:10])
        event = _get_title_text(props)

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
