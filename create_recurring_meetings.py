import os
import re
import datetime
import requests
from dateutil.relativedelta import relativedelta
import traceback

# 🔐 Variáveis de ambiente
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
DATABASE_ID_REUNIOES = os.getenv("DATABASE_ID_REUNIOES_TESTE")

# 🧮 Limite padrão de geração
LIMIT_DAYS = 30
MAX_MONTHS = 12
BIWEEKLY_MONTHS = 6  # ✅ quinzenais pelos próximos 6 meses

HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

RECURRING_EMOJI = "🔁"

# ==========================
# ✅ CONFIG: feriados
# ==========================
HOLIDAY_TYPE_PROP_NAME = "Tipo"
HOLIDAY_TYPE_VALUE = "Feriado"

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

# ✅ Cache global de feriados (datas)
HOLIDAYS_SET = set()


def debug(*args):
    print(*args)


# ============================================
# 🕒 NORMALIZAÇÃO DE DATA DO NOTION
# ============================================
def normalize_notion_date(start_str):
    if not start_str:
        return None

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", start_str):
        return datetime.date.fromisoformat(start_str)

    try:
        dt = datetime.datetime.fromisoformat(start_str.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.datetime.fromisoformat(start_str.split("T")[0] + "T00:00:00+00:00")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)

    sp = datetime.timezone(datetime.timedelta(hours=-3))
    return dt.astimezone(sp).date()


# ✅ Evita herdar ⚠️/(Ausências: ...) e 🔁 no título das instâncias geradas
def sanitize_event_title_for_recurrence(title: str) -> str:
    if not title:
        return "(sem título)"

    t = title.replace("\u00A0", " ").strip()

    # remove sufixo de ausências
    t = re.sub(r"\s*\((Ausentes|Ausências):.*?\)\s*$", "", t).strip()

    warn = r"⚠\uFE0F?"
    rec  = r"🔁\uFE0F?"
    t = re.sub(rf"^\s*(?:({warn}|{rec})\s*)+", "", t).strip()

    return t or "(sem título)"


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


# ==========================
# ✅ FERIADOS (1x, com cache)
# ==========================
def _daterange_inclusive(start_date: datetime.date, end_date: datetime.date):
    cur = start_date
    while cur <= end_date:
        yield cur
        cur += datetime.timedelta(days=1)


def load_holidays_set(window_start: datetime.date, window_end: datetime.date) -> set:
    """
    Busca no Notion todos os itens com:
      - Tipo (select) == Feriado
      - Data no intervalo [window_start, window_end]
    E devolve um set com todas as datas cobertas (expandindo start..end se houver end).
    """
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID_REUNIOES}/query"
    all_results = []
    payload = {
        "filter": {
            "and": [
                {"property": "Data", "date": {"on_or_after": window_start.strftime("%Y-%m-%d")}},
                {"property": "Data", "date": {"on_or_before": window_end.strftime("%Y-%m-%d")}},
                {"property": HOLIDAY_TYPE_PROP_NAME, "select": {"equals": HOLIDAY_TYPE_VALUE}},
            ]
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

    holidays = set()

    for page in all_results:
        props = page.get("properties", {})
        date_prop = props.get("Data", {}).get("date") or {}
        start_raw = date_prop.get("start")
        end_raw = date_prop.get("end")

        start_date = normalize_notion_date(start_raw) if start_raw else None
        end_date = normalize_notion_date(end_raw) if end_raw else None

        if not start_date:
            continue

        # se tiver end, expande; senão só start
        if end_date and end_date >= start_date:
            for d in _daterange_inclusive(start_date, end_date):
                if window_start <= d <= window_end:
                    holidays.add(d)
        else:
            if window_start <= start_date <= window_end:
                holidays.add(start_date)

    return holidays


def is_holiday_date(date_to_check: datetime.date) -> bool:
    return date_to_check in HOLIDAYS_SET


# ==========================
# ✅ EXISTÊNCIA DE INSTÂNCIA
# ==========================
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


# ==========================
# ✅ CRIAÇÃO DE INSTÂNCIA
# ==========================
def create_instance(base_meeting, target_date):
    try:
        # ✅ NOVO: pula datas com feriado (cache)
        if is_holiday_date(target_date):
            debug(f"🎉 Ignorando criação (feriado): {target_date}")
            return None

        props = base_meeting.get("properties", {})
        event_text_raw = _get_title_text(props)
        event_text = sanitize_event_title_for_recurrence(event_text_raw)

        page_id = base_meeting["id"]

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

        new_properties["Data"] = {"date": {"start": target_date.strftime("%Y-%m-%d"), "end": None}}
        new_properties["Evento"] = {"title": [{"text": {"content": f"{RECURRING_EMOJI} {event_text}"}}]}
        new_properties["Reuniões relacionadas (recorrência)"] = {"relation": [{"id": page_id}]}
        new_properties["Recorrência"] = {"select": None}

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


# ==========================
# ✅ GERADORES
# ==========================
def generate_daily(base_meeting, base_date):
    limit_date = base_date + datetime.timedelta(days=LIMIT_DAYS)
    next_date = base_date + datetime.timedelta(days=1)
    while next_date <= limit_date:
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


def generate_biweekly(base_meeting, base_date):
    limit_date = base_date + relativedelta(months=BIWEEKLY_MONTHS)
    next_date = base_date + datetime.timedelta(weeks=2)
    while next_date <= limit_date:
        create_instance(base_meeting, next_date)
        next_date += datetime.timedelta(weeks=2)


# ==========================
# ✅ CONTAGEM (instâncias relacionadas)
# ==========================
def count_related_instances_via_query(base_meeting):
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


def _estimate_end_date_for_meeting(recurrence: str, base_date: datetime.date) -> datetime.date:
    """
    Define até onde podemos precisar gerar instâncias, pra montar a janela de feriados.
    """
    if recurrence == "diária":
        return base_date + datetime.timedelta(days=LIMIT_DAYS)
    if recurrence == "semanal":
        return base_date + datetime.timedelta(days=LIMIT_DAYS)
    if recurrence == "mensal":
        return base_date + relativedelta(months=MAX_MONTHS)
    if recurrence in ("quinzenal", "quinzenais"):
        return base_date + relativedelta(months=BIWEEKLY_MONTHS)
    return base_date


def main():
    global HOLIDAYS_SET

    debug("🔄 Iniciando geração de reuniões recorrentes...")
    meetings = get_meetings()
    today = datetime.date.today()
    debug(f"Hoje: {today}  Reuniões carregadas: {len(meetings)}")

    # 1) Determina janela necessária de feriados (mínimo start, máximo end)
    window_start = today
    window_end = today

    recurrent_meetings = []
    for meeting in meetings:
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

        base_date = normalize_notion_date(data_prop["start"])
        if not base_date:
            continue

        recurrent_meetings.append(meeting)

        if base_date < window_start:
            window_start = base_date

        est_end = _estimate_end_date_for_meeting(recurrence, base_date)
        if est_end > window_end:
            window_end = est_end

    # 2) Carrega feriados 1x
    if recurrent_meetings:
        debug(f"🎉 Carregando feriados de {window_start} até {window_end}...")
        HOLIDAYS_SET = load_holidays_set(window_start, window_end)
        debug(f"🎉 Feriados carregados: {len(HOLIDAYS_SET)} datas no cache.")
    else:
        debug("ℹ️ Nenhuma reunião recorrente encontrada. Não carreguei feriados.")

    # 3) Loop normal de geração
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

            base_date = normalize_notion_date(data_prop["start"])
            event = _get_title_text(props)
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
            elif recurrence in ("quinzenal", "quinzenais"):
                limit_date = base_date + relativedelta(months=BIWEEKLY_MONTHS)
                total_esperado = 0
                next_date = base_date + datetime.timedelta(weeks=2)
                while next_date <= limit_date:
                    total_esperado += 1
                    next_date += datetime.timedelta(weeks=2)
            else:
                total_esperado = 0

            if existentes >= total_esperado:
                debug(f"🔹 {event} ({recurrence}) já tem {existentes}/{total_esperado} instâncias relacionadas (via query).")
                continue

            debug(f"\n🔁 {event} — recorrência: {recurrence} — base: {base_date} ({existentes}/{total_esperado} criadas)")

            if recurrence == "diária":
                generate_daily(meeting, base_date)
            elif recurrence == "semanal":
                generate_weekly(meeting, base_date)
            elif recurrence == "mensal":
                generate_monthly(meeting, base_date)
            elif recurrence in ("quinzenal", "quinzenais"):
                generate_biweekly(meeting, base_date)

        except Exception as e:
            debug(f"Erro no loop principal: {e}")
            traceback.print_exc()

    debug("🏁 Rotina concluída.")


if __name__ == "__main__":
    main()
