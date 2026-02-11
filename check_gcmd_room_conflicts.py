import os
import re
import json
import hashlib
import requests
from datetime import datetime, timedelta, timezone, date
from dateutil import parser as dateparser

# =========================
# ENV VARS
# =========================
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
DATABASE_ID_REUNIOES = os.getenv("DATABASE_ID_REUNIOES")
DATABASE_ID_EQUIPE_GCMD = os.getenv("DATABASE_ID_EQUIPE_GCMD")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")

NOTION_VERSION = "2022-06-28"
GCMD_REGEX = re.compile(r"gcmd", re.IGNORECASE)

STATE_DIR = ".state"
STATE_FILE = os.path.join(STATE_DIR, "gcmd_conflicts.json")

KEEP_WEEKS = 12  # mantém estado só das últimas 12 semanas

# Assumir fuso para datas "naive" vindas do Notion (sem offset)
# (Brasil/BH/SP normalmente UTC-03)
DEFAULT_TZ = timezone(timedelta(hours=-3))

# =========================
# DATETIME NORMALIZATION
# =========================
def ensure_aware(dt: datetime, default_tz: timezone = DEFAULT_TZ) -> datetime:
    """
    Garante que dt seja timezone-aware.
    Se vier sem tzinfo (naive), assume default_tz.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=default_tz)
    return dt

def to_utc(dt: datetime) -> datetime:
    """
    Converte dt para UTC (mantendo aware).
    """
    dt = ensure_aware(dt)
    return dt.astimezone(timezone.utc)

# =========================
# STATE (anti-flood 1x/semana + limpeza)
# =========================
def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict) and "sent" in data and isinstance(data["sent"], dict):
                return data
    except Exception:
        pass
    return {"sent": {}}  # { signature: "YYYY-Www" }

def save_state(state: dict):
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def current_week_key(now: datetime) -> str:
    iso_year, iso_week, _ = now.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"

def week_key_to_monday(week_key: str) -> date | None:
    try:
        year_str, week_str = week_key.split("-W")
        y = int(year_str)
        w = int(week_str)
        return date.fromisocalendar(y, w, 1)  # Monday
    except Exception:
        return None

def prune_state(state: dict, now: datetime, keep_weeks: int = KEEP_WEEKS) -> int:
    sent = state.get("sent", {})
    if not isinstance(sent, dict) or not sent:
        return 0

    cutoff_date = (now.date() - timedelta(weeks=keep_weeks))

    to_delete = []
    for sig, wk in sent.items():
        monday = week_key_to_monday(str(wk))
        if monday is None or monday < cutoff_date:
            to_delete.append(sig)

    for sig in to_delete:
        sent.pop(sig, None)

    state["sent"] = sent
    return len(to_delete)

def conflict_signature(group_meetings: list[dict]) -> str:
    local_norm = (group_meetings[0]["local"] or "").strip().lower()
    min_start = min(m["start"] for m in group_meetings).isoformat()
    max_end = max(m["end"] for m in group_meetings).isoformat()
    ids_sorted = ",".join(sorted(m["page_id"] for m in group_meetings))

    raw = f"{local_norm}|{min_start}|{max_end}|{ids_sorted}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def should_send(signature: str, state: dict, now: datetime) -> bool:
    last_week = state.get("sent", {}).get(signature)
    return last_week != current_week_key(now)

def mark_sent(signature: str, state: dict, now: datetime):
    state.setdefault("sent", {})[signature] = current_week_key(now)

# =========================
# NOTION
# =========================
def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }

def notion_query_database(database_id: str, payload: dict):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    results = []
    cursor = None

    while True:
        body = dict(payload)
        if cursor:
            body["start_cursor"] = cursor

        r = requests.post(url, headers=notion_headers(), json=body, timeout=30)
        r.raise_for_status()
        data = r.json()

        results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    return results

def get_prop_text(page: dict, prop_name: str) -> str:
    prop = page.get("properties", {}).get(prop_name)
    if not prop:
        return ""
    t = prop.get("type")
    if t == "title":
        return "".join(x.get("plain_text", "") for x in prop.get("title", []))
    if t == "rich_text":
        return "".join(x.get("plain_text", "") for x in prop.get("rich_text", []))
    return ""

def parse_date_range(page: dict, prop_name="Data"):
    date_obj = page.get("properties", {}).get(prop_name, {}).get("date")
    if not date_obj or not date_obj.get("start"):
        return None, None

    start = dateparser.isoparse(date_obj["start"])
    if date_obj.get("end"):
        end = dateparser.isoparse(date_obj["end"])
    else:
        end = start + timedelta(hours=1)

    # Normaliza para UTC, evitando naive/aware misturado
    start_utc = to_utc(start)
    end_utc = to_utc(end)

    return start_utc, end_utc

def parse_created_time(page: dict) -> datetime | None:
    """
    Lê created_time do Notion e normaliza para UTC.
    """
    ct = page.get("created_time")
    if not ct:
        return None
    try:
        return to_utc(dateparser.isoparse(ct))
    except Exception:
        return None

def fetch_notion_user_name(user_id: str) -> str | None:
    url = f"https://api.notion.com/v1/users/{user_id}"
    r = requests.get(url, headers=notion_headers(), timeout=30)
    if r.status_code != 200:
        return None
    return (r.json() or {}).get("name")

# =========================
# EQUIPE | GCMD (People -> email)
# =========================
def load_team_user_map():
    pages = notion_query_database(DATABASE_ID_EQUIPE_GCMD, {"page_size": 100})
    user_map = {}

    for p in pages:
        people_prop = p.get("properties", {}).get("Usuário no Notion")
        email_prop = p.get("properties", {}).get("E-mail")

        if not people_prop or people_prop.get("type") != "people":
            continue
        if not email_prop or email_prop.get("type") != "email":
            continue

        email = email_prop.get("email")
        if not email:
            continue

        for person in people_prop.get("people", []):
            uid = person.get("id")
            if uid:
                user_map[uid] = email.lower()

    return user_map

# =========================
# CONFLITOS
# =========================
def intervals_overlap(a_start, a_end, b_start, b_end):
    return a_start < b_end and a_end > b_start

def build_conflict_groups(meetings):
    groups = []
    used = set()

    for i, m in enumerate(meetings):
        if i in used:
            continue

        group = [i]
        for j in range(i + 1, len(meetings)):
            if intervals_overlap(m["start"], m["end"], meetings[j]["start"], meetings[j]["end"]):
                group.append(j)

        if len(group) < 2:
            continue

        changed = True
        while changed:
            changed = False
            for j in range(len(meetings)):
                if j in group:
                    continue
                if any(
                    intervals_overlap(meetings[k]["start"], meetings[k]["end"], meetings[j]["start"], meetings[j]["end"])
                    for k in group
                ):
                    group.append(j)
                    changed = True

        for idx in group:
            used.add(idx)
        groups.append(group)

    return groups

# =========================
# SLACK
# =========================
def slack_headers():
    return {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json; charset=utf-8",
    }

def slack_lookup_user_id_by_email(email):
    r = requests.get(
        "https://slack.com/api/users.lookupByEmail",
        headers=slack_headers(),
        params={"email": email},
        timeout=30,
    )
    data = r.json()
    return data["user"]["id"] if data.get("ok") else None

def slack_open_dm(user_id):
    r = requests.post(
        "https://slack.com/api/conversations.open",
        headers=slack_headers(),
        json={"users": user_id},
        timeout=30,
    )
    data = r.json()
    return data["channel"]["id"] if data.get("ok") else None

def slack_post_message(channel_id, text):
    r = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers=slack_headers(),
        json={"channel": channel_id, "text": text},
        timeout=30,
    )
    return bool(r.json().get("ok"))

# =========================
# MAIN
# =========================
def main():
    missing = [k for k, v in {
        "NOTION_API_KEY": NOTION_API_KEY,
        "DATABASE_ID_REUNIOES": DATABASE_ID_REUNIOES,
        "DATABASE_ID_EQUIPE_GCMD": DATABASE_ID_EQUIPE_GCMD,
        "SLACK_BOT_TOKEN": SLACK_BOT_TOKEN,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Faltam variáveis de ambiente: {', '.join(missing)}")

    state = load_state()
    now = datetime.now(timezone.utc)

    removed = prune_state(state, now, KEEP_WEEKS)
    if removed:
        print(f"[INFO] Estado podado: removi {removed} entrada(s) antiga(s).")

    team_user_map = load_team_user_map()

    pages = notion_query_database(
        DATABASE_ID_REUNIOES,
        {
            "page_size": 100,
            "filter": {
                "and": [
                    {"property": "Data", "date": {"on_or_after": (now - timedelta(days=1)).date().isoformat()}},
                    {"property": "Data", "date": {"on_or_before": (now + timedelta(days=14)).date().isoformat()}},
                ]
            },
            "sorts": [{"property": "Data", "direction": "ascending"}],
        },
    )

    user_name_cache: dict[str, str] = {}
    meetings = []

    for p in pages:
        local = get_prop_text(p, "Local")
        if not local or not GCMD_REGEX.search(local):
            continue

        start, end = parse_date_range(p, "Data")
        if not start or not end:
            continue

        created_time = parse_created_time(p) or datetime.min.replace(tzinfo=timezone.utc)

        page_id = p.get("id") or ""
        title = (get_prop_text(p, "Evento") or "(Sem título)").strip()
        url = p.get("url") or ""

        creator_id = (p.get("created_by") or {}).get("id") or ""
        email = team_user_map.get(creator_id)

        creator_name = ""
        if creator_id:
            if creator_id not in user_name_cache:
                user_name_cache[creator_id] = fetch_notion_user_name(creator_id) or ""
            creator_name = user_name_cache.get(creator_id, "")
        if not creator_name:
            creator_name = "Pessoa não identificada"

        meetings.append({
            "page_id": page_id,
            "title": title,
            "url": url,
            "creator": creator_name,
            "email": email,
            "start": start,
            "end": end,
            "local": local,
            "created_time": created_time,
        })

    # Continua ordenando p/ detectar conflitos (não muda a lógica de conflito)
    meetings.sort(key=lambda m: m["start"])
    conflict_groups = build_conflict_groups(meetings)

    sent_any = False

    for group in conflict_groups:
        group_meetings = [meetings[i] for i in group]
        sig = conflict_signature(group_meetings)

        # anti-flood 1x por semana
        if not should_send(sig, state, now):
            continue

        emails = {m["email"] for m in group_meetings if m["email"]}
        if not emails:
            continue

        # ✅ ORDENAR PARA A MENSAGEM NO SLACK POR ORDEM DE CRIAÇÃO
        group_meetings_sorted_for_slack = sorted(group_meetings, key=lambda m: m["created_time"])

        lines = [
            "Opa! Encontrei um possível conflito de horários na sala de reuniões da GCMD 👀",
            "",
        ]

        for m in group_meetings_sorted_for_slack:
            created_local = m["created_time"].astimezone(DEFAULT_TZ)
            start_local = m["start"].astimezone(DEFAULT_TZ)
            end_local = m["end"].astimezone(DEFAULT_TZ)

            lines.extend([
                f"🗓️ {m['title']} - {m['url']}",
                f"Criada por {m['creator']} em {created_local.strftime('%d/%m/%Y %H:%M')}",
                f"Data: {start_local.strftime('%d/%m/%Y, %H:%M')}–{end_local.strftime('%H:%M')}",
                f"Local: {m['local']}",
                "",
            ])

        lines.append("👉 Vale alinhar entre vocês e ajustar o horário ou o local")
        text = "\n".join(lines)

        for email in emails:
            user_id = slack_lookup_user_id_by_email(email)
            if not user_id:
                continue
            channel = slack_open_dm(user_id)
            if channel:
                slack_post_message(channel, text)

        mark_sent(sig, state, now)
        sent_any = True

    removed2 = prune_state(state, now, KEEP_WEEKS)
    if removed2:
        print(f"[INFO] Estado podado no final: removi {removed2} entrada(s) antiga(s).")

    save_state(state)
    print("[DONE] OK" + (" (enviou alertas)" if sent_any else " (nada a enviar)"))

if __name__ == "__main__":
    main()
