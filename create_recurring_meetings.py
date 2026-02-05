import os
import re
import datetime
import requests
from dateutil.relativedelta import relativedelta
import traceback

# 🔐 Variáveis de ambiente
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
DATABASE_ID_REUNIOES = os.getenv("DATABASE_ID_REUNIOES_TESTE")

# 🧮 Limites
LIMIT_DAYS = 30
MAX_MONTHS = 12
BIWEEKLY_MONTHS = 6

HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

RECURRING_EMOJI = "🔁"

# ==========================
# CONFIG: FERIADOS
# ==========================
HOLIDAY_TYPE_PROP_NAME = "Tipo"      # multi_select
HOLIDAY_TYPE_VALUE = "Feriado"

# Timezone SP
SP_TZ = datetime.timezone(datetime.timedelta(hours=-3))

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

HOLIDAYS_SET = set()


def debug(*args):
    print(*args)


# ==========================
# DATAS
# ==========================
def normalize_notion_date(start_str):
    if not start_str:
        return None

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", start_str):
        return datetime.date.fromisoformat(start_str)

    dt = datetime.datetime.fromisoformat(start_str.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)

    return dt.astimezone(SP_TZ).date()


def _parse_notion_dt(dt_str):
    if not dt_str or re.fullmatch(r"\d{4}-\d{2}-\d{2}", dt_str):
        return None

    dt = datetime.datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)

    return dt.astimezone(SP_TZ)


def build_date_payload_from_base(base_meeting, target_date):
    props = base_meeting.get("properties", {})
    base_date = props.get("Data", {}).get("date") or {}
    start_raw = base_date.get("start")
    end_raw = base_date.get("end")

    # Caso sem horário
    if not start_raw or re.fullmatch(r"\d{4}-\d{2}-\d{2}", start_raw):
        return {"start": target_date.strftime("%Y-%m-%d"), "end": None}

    base_start_dt = _parse_notion_dt(start_raw)
    if not base_start_dt:
        return {"start": target_date.strftime("%Y-%m-%d"), "end": None}

    new_start_dt = base_start_dt.replace(
        year=target_date.year,
        month=target_date.month,
        day=target_date.day
    )

    new_end_dt = None
    if end_raw and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", end_raw):
        base_end_dt = _parse_notion_dt(end_raw)
        if base_end_dt:
            duration = base_end_dt - base_start_dt
            if duration.total_seconds() > 0:
                new_end_dt = new_start_dt + duration

    return {
        "start": new_start_dt.isoformat(),
        "end": new_end_dt.isoformat() if new_end_dt else None
    }


# ==========================
# NOTION HELPERS
# ==========================
def notion_query(payload):
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID_REUNIOES}/query"
    r = requests.post(url, headers=HEADERS, json=payload)
    if r.status_code == 400:
        debug("❌ Erro 400 Notion")
        debug(payload)
        debug(r.text)
    r.raise_for_status()
    return r.json()


def get_meetings():
    results = []
    payload = {"page_size": 100}
    cursor = None

    while True:
        if cursor:
            payload["start_cursor"] = cursor
        data = notion_query(payload)
        results.extend(data.get("results", []))
        cursor = data.get("next_cursor")
        if not cursor:
            break

    return results


# ==========================
# FERIADOS (CACHE)
# ==========================
def _daterange(start, end):
    cur = start
    while cur <= end:
        yield cur
        cur += datetime.timedelta(days=1)


def load_holidays_set(start, end):
    holidays = set()
    try:
        payload = {
            "filter": {
                "and": [
                    {"property": "Data", "date": {"on_or_after": start.strftime("%Y-%m-%d")}},
                    {"property": "Data", "date": {"on_or_before": end.strftime("%Y-%m-%d")}},
                    {"property": HOLIDAY_TYPE_PROP_NAME, "multi_select": {"contains": HOLIDAY_TYPE_VALUE}},
                ]
            },
            "page_size": 100
        }

        cursor = None
        pages = []

        while True:
            if cursor:
                payload["start_cursor"] = cursor
            data = notion_query(payload)
            pages.extend(data.get("results", []))
            cursor = data.get("next_cursor")
            if not cursor:
                break

        for page in pages:
            date_prop = page["properties"]["Data"]["date"]
            start_d = normalize_notion_date(date_prop["start"])
            end_d = normalize_notion_date(date_prop.get("end"))

            if not start_d:
                continue

            if end_d:
                for d in _daterange(start_d, end_d):
                    if start <= d <= end:
                        holidays.add(d)
            else:
                holidays.add(start_d)

    except Exception as e:
        debug("⚠️ Falha ao carregar feriados, seguindo sem bloqueio.")
        traceback.print_exc()

    return holidays


def is_holiday(date):
    return date in HOLIDAYS_SET


# ==========================
# TÍTULO
# ==========================
def sanitize_title(title):
    if not title:
        return "(sem título)"

    t = title.replace("\u00A0", " ").strip()
    t = re.sub(r"\s*\((Ausentes|Ausências):.*?\)\s*$", "", t)
    t = re.sub(r"^\s*(⚠️?|🔁)\s*", "", t)
    return t.strip()


def get_title(props):
    title = props.get("Evento", {}).get("title", [])
    return title[0]["plain_text"] if title else "(sem título)"


# ==========================
# INSTÂNCIA
# ==========================
def instance_exists(base_id, date):
    payload = {
        "filter": {
            "and": [
                {"property": "Reuniões relacionadas (recorrência)", "relation": {"contains": base_id}},
                {"property": "Data", "date": {"on_or_after": date}},
                {"property": "Data", "date": {"on_or_before": date}},
            ]
        },
        "page_size": 1
    }
    data = notion_query(payload)
    return bool(data["results"])


def create_instance(base_meeting, target_date):
    if is_holiday(target_date):
        debug(f"🎉 Feriado {target_date}, pulando")
        return

    props = base_meeting["properties"]
    base_id = base_meeting["id"]

    title = sanitize_title(get_title(props))

    date_payload = build_date_payload_from_base(base_meeting, target_date)

    if instance_exists(base_id, date_payload["start"][:10]):
        return

    new_props = {
        "Evento": {"title": [{"text": {"content": f"{RECURRING_EMOJI} {title}"}}]},
        "Data": {"date": date_payload},
        "Reuniões relacionadas (recorrência)": {"relation": [{"id": base_id}]},
        "Recorrência": {"select": None},
    }

    for k, v in props.items():
        if k in new_props or v["type"] not in CREATABLE_PROP_TYPES:
            continue
        content = v.get(v["type"])
        if content:
            new_props[k] = {v["type"]: content}

    payload = {"parent": {"database_id": DATABASE_ID_REUNIOES}, "properties": new_props}
    requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=payload)
    debug(f"✅ Criada: {title} → {date_payload['start']}")


# ==========================
# GERADORES
# ==========================
def generate_daily(m, base):
    for i in range(1, LIMIT_DAYS + 1):
        d = base + datetime.timedelta(days=i)
        if d.weekday() < 5:
            create_instance(m, d)


def generate_weekly(m, base):
    d = base + datetime.timedelta(weeks=1)
    for _ in range(4):
        create_instance(m, d)
        d += datetime.timedelta(weeks=1)


def generate_monthly(m, base):
    d = base + relativedelta(months=1)
    for _ in range(MAX_MONTHS):
        create_instance(m, d)
        d += relativedelta(months=1)


def generate_biweekly(m, base):
    d = base + datetime.timedelta(weeks=2)
    limit = base + relativedelta(months=BIWEEKLY_MONTHS)
    while d <= limit:
        create_instance(m, d)
        d += datetime.timedelta(weeks=2)


# ==========================
# MAIN
# ==========================
def main():
    global HOLIDAYS_SET

    meetings = get_meetings()
    today = datetime.date.today()

    window_start = today
    window_end = today

    for m in meetings:
        r = m["properties"].get("Recorrência", {}).get("select")
        if not r:
            continue
        base = normalize_notion_date(m["properties"]["Data"]["date"]["start"])
        if base:
            window_start = min(window_start, base)
            window_end = max(window_end, base + relativedelta(months=MAX_MONTHS))

    HOLIDAYS_SET = load_holidays_set(window_start, window_end)
    debug(f"🎉 Feriados carregados: {len(HOLIDAYS_SET)}")

    for m in meetings:
        r = m["properties"].get("Recorrência", {}).get("select")
        if not r:
            continue

        base = normalize_notion_date(m["properties"]["Data"]["date"]["start"])
        rec = r["name"].lower()

        if rec == "diária":
            generate_daily(m, base)
        elif rec == "semanal":
            generate_weekly(m, base)
        elif rec == "mensal":
            generate_monthly(m, base)
        elif rec in ("quinzenal", "quinzenais"):
            generate_biweekly(m, base)


if __name__ == "__main__":
    main()
