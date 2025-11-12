import os
import datetime
import requests
from dateutil.relativedelta import relativedelta
import traceback
import json

# 🔐 Variáveis de ambiente
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
DATABASE_ID_REUNIOES = os.getenv("DATABASE_ID_REUNIOES_TESTE")

# 🧮 Limite de dias futuros para criar instâncias (padrão: 30 dias)
LIMIT_DAYS = int(os.getenv("RECURRING_LIMIT_DAYS", "30"))
MAX_MONTHS = os.getenv("RECURRING_MAX_MONTHS", "12")
MAX_MONTHS = int(MAX_MONTHS) if MAX_MONTHS and MAX_MONTHS.isdigit() else None

HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

RECURRING_EMOJI = "🔁"

# Tipos que podemos tentar definir ao criar página
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


def debug(*args, **kwargs):
    print(*args, **kwargs)


def get_meetings():
    """Obtém todas as reuniões do banco, com paginação."""
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
        batch = data.get("results", [])
        all_results.extend(batch)
        debug(f"→ carregadas {len(batch)} (total {len(all_results)})")
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
        }
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
        }
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
    """Decide se o conteúdo é válido (não vazio) para ser enviado."""
    if content is None:
        return False
    # lists: require len>0
    if isinstance(content, list):
        return len(content) > 0
    # dicts: treat empty dict as empty
    if isinstance(content, dict):
        return len(content) > 0
    # strings: non-empty
    if isinstance(content, str):
        return content.strip() != ""
    # numbers and booleans: allow 0/False
    if isinstance(content, (int, float, bool)):
        return True
    return True


def create_instance(base_meeting, target_date):
    """Cria instância copiando propriedades válidas e normalizando people/relations."""
    try:
        props = base_meeting.get("properties", {})
        event_text = _get_title_text(props)
        recurrence = props.get("Recorrência", {}).get("select", {}).get("name")
        page_id = base_meeting["id"]

        # duplicação segura: não recria se já existe
        if instance_exists_for_date(base_meeting, target_date):
            debug(f"⚠️ Ignorando (já existe por relação) {event_text} {target_date}")
            return None
        if check_existing_instance_by_title_date(event_text, target_date):
            debug(f"⚠️ Ignorando (já existe por título) {event_text} {target_date}")
            return None

        new_properties = {}

        for key, val in props.items():
            # val is the property object returned by Notion
            prop_type = val.get("type")
            if not prop_type or prop_type not in CREATABLE_PROP_TYPES:
                # pula propriedades não-criáveis (formula, rollup, created_time, etc.)
                continue

            # não copie campos que vamos sobrescrever
            if key in ("Data", "Reuniões relacionadas (recorrência)", "Evento"):
                continue

            content = val.get(prop_type)

            # se conteúdo vazio/None, não inclua no payload
            if not _is_non_empty_content(prop_type, content):
                continue

            # especial: people -> enviar apenas ids
            if prop_type == "people":
                people_list = content or []
                people_ids = []
                for p in people_list:
                    # a API do banco retorna pessoas com 'id' quando são usuários do workspace
                    pid = p.get("id")
                    if pid:
                        people_ids.append({"id": pid})
                if people_ids:
                    new_properties[key] = {"people": people_ids}
                # se ficou vazio, não adiciona
                continue

            # special: relation -> keep list of {"id": ...} if present
            if prop_type == "relation":
                rels = content or []
                rel_ids = []
                for ritem in rels:
                    rid = ritem.get("id")
                    if rid:
                        rel_ids.append({"id": rid})
                if rel_ids:
                    new_properties[key] = {"relation": rel_ids}
                continue

            # multi_select/select/date/number/checkbox/url/email/phone_number/files/rich_text/title
            # For these, content is usually already in the shape expected by the API, so include it.
            # But ensure it's not None/empty (checked above).
            new_properties[key] = {prop_type: content}

        # sobrescreve os campos obrigatórios/ajustados
        new_title_text = f"{RECURRING_EMOJI} {event_text}"
        new_properties["Evento"] = {"title": [{"text": {"content": new_title_text}}]}
        new_properties["Data"] = {"date": {"start": target_date.isoformat()}}
        new_properties["Reuniões relacionadas (recorrência)"] = {"relation": [{"id": page_id}]}
        if recurrence:
            new_properties["Recorrência"] = {"select": {"name": recurrence}}

        payload = {"parent": {"database_id": DATABASE_ID_REUNIOES}, "properties": new_properties}

        r = requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=payload)
        if r.status_code not in (200, 201):
            # imprime erro detalhado e devolve None sem quebrar tudo
            debug(f"❌ Erro ao criar instância para '{event_text}' em {target_date}: {r.status_code} {r.text}")
            return None

        debug(f"✅ Instância criada: {new_title_text} → {target_date}")
        return r.json()

    except Exception as e:
        debug("Exception em create_instance:", str(e))
        traceback.print_exc()
        return None


def generate_daily(base_meeting, base_date, today, limit_date):
    next_date = base_date + datetime.timedelta(days=1)
    while next_date <= limit_date:
        if next_date <= today:
            next_date += datetime.timedelta(days=1)
            continue
        if next_date.weekday() in (5, 6):
            debug(f"⏭️ Pulando fim de semana: {next_date}")
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
    debug("🔄 Iniciando geração de reuniões recorrentes (multi-instâncias)...")
    meetings = get_meetings()
    today = datetime.date.today()
    limit_date = today + datetime.timedelta(days=LIMIT_DAYS)
    debug(f"Hoje: {today}  Limite: {limit_date}  Reuniões carregadas: {len(meetings)}")

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

            # aceita base_date == today (não pula)
            if base_date > limit_date:
                debug(f"⏸️ Ignorando '{event}' — data base {base_date} além do limite.")
                continue

            debug(f"\n🔁 Processando '{event}' — recorrência: {recurrence} — base: {base_date}")

            if recurrence == "diária":
                generate_daily(meeting, base_date, today, limit_date)
            elif recurrence == "semanal":
                generate_weekly(meeting, base_date, today, limit_date)
            elif recurrence == "mensal":
                generate_monthly(meeting, base_date, today, limit_date)
            else:
                debug(f"⚠️ Tipo desconhecido: {recurrence}")

        except Exception as e:
            debug(f"Erro no loop principal para meeting {meeting.get('id')}: {e}")
            traceback.print_exc()

    debug("🏁 Rotina concluída com sucesso.")


if __name__ == "__main__":
    main()
