import os
import re
import unicodedata
import requests
from datetime import datetime, timedelta, timezone
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

# =========================
# NOTION HELPERS
# =========================
def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }

def normalize_str(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"\s+", " ", s)
    return s

def get_prop_text(page: dict, prop_name: str) -> str:
    prop = page.get("properties", {}).get(prop_name)
    if not prop:
        return ""
    t = prop.get("type")

    if t == "title":
        return "".join(x.get("plain_text", "") for x in prop.get("title", []))
    if t == "rich_text":
        return "".join(x.get("plain_text", "") for x in prop.get("rich_text", []))
    if t == "email":
        return prop.get("email") or ""
    return ""

def parse_date_range(page: dict, date_prop_name: str = "Data"):
    date_obj = page.get("properties", {}).get(date_prop_name, {}).get("date")
    if not date_obj or not date_obj.get("start"):
        return None, None

    start = dateparser.isoparse(date_obj["start"])
    if date_obj.get("end"):
        end = dateparser.isoparse(date_obj["end"])
    else:
        end = start + timedelta(hours=1)

    return start, end

def notion_query_database(database_id: str, payload: dict):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    results = []
    start_cursor = None

    while True:
        body = dict(payload)
        if start_cursor:
            body["start_cursor"] = start_cursor

        r = requests.post(url, headers=notion_headers(), json=body, timeout=30)
        r.raise_for_status()
        data = r.json()

        results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        start_cursor = data.get("next_cursor")

    return results

def fetch_meetings(window_start: datetime, window_end: datetime):
    payload = {
        "page_size": 100,
        "filter": {
            "and": [
                {"property": "Data", "date": {"on_or_after": window_start.date().isoformat()}},
                {"property": "Data", "date": {"on_or_before": window_end.date().isoformat()}},
            ]
        },
        "sorts": [{"property": "Data", "direction": "ascending"}],
    }
    return notion_query_database(DATABASE_ID_REUNIOES, payload)

# =========================
# EQUIPE | GCMD
# =========================
def load_team_email_map():
    payload = {"page_size": 100}
    pages = notion_query_database(DATABASE_ID_EQUIPE_GCMD, payload)

    name_to_email = {}
    for p in pages:
        nome = get_prop_text(p, "Nome")
        email = get_prop_text(p, "E-mail")
        n = normalize_str(nome)
        e = (email or "").strip().lower()

        if n and e and "@" in e:
            name_to_email[n] = e

    return name_to_email

def resolve_email_from_name(team_map: dict, name: str | None) -> str | None:
    if not name:
        return None
    cb = normalize_str(name)
    if not cb:
        return None

    if cb in team_map:
        return team_map[cb]

    for team_name_norm, email in team_map.items():
        if cb in team_name_norm or team_name_norm in cb:
            return email

    return None

# =========================
# CRIADOR
# =========================
def get_creator_user_id(page: dict) -> str | None:
    prop = page.get("properties", {}).get("Criado por")
    if prop and prop.get("type") == "created_by":
        cb = prop.get("created_by") or {}
        if cb.get("id"):
            return cb["id"]

    cb2 = page.get("created_by") or {}
    return cb2.get("id")

def fetch_notion_user_name(user_id: str) -> str | None:
    url = f"https://api.notion.com/v1/users/{user_id}"
    r = requests.get(url, headers=notion_headers(), timeout=30)
    if r.status_code != 200:
        return None
    return r.json().get("name")

# =========================
# CONFLITOS
# =========================
def intervals_overlap(a_start, a_end, b_start, b_end) -> bool:
    return a_start < b_end and a_end > b_start

def build_conflict_groups(meetings: list[dict]) -> list[list[int]]:
    n = len(meetings)
    groups = []
    used = set()

    for i in range(n):
        if i in used:
            continue

        group = [i]
        for j in range(i + 1, n):
            if intervals_overlap(
                meetings[i]["start"], meetings[i]["end"],
                meetings[j]["start"], meetings[j]["end"]
            ):
                group.append(j)

        if len(group) < 2:
            continue

        changed = True
        while changed:
            changed = False
            for j in range(n):
                if j in group:
                    continue
                if any(
                    intervals_overlap(
                        meetings[k]["start"], meetings[k]["end"],
                        meetings[j]["start"], meetings[j]["end"]
                    )
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

def slack_lookup_user_id_by_email(email: str) -> str | None:
    r = requests.get(
        "https://slack.com/api/users.lookupByEmail",
        headers=slack_headers(),
        params={"email": email},
        timeout=30,
    )
    data = r.json()
    return data["user"]["id"] if data.get("ok") else None

def slack_open_dm(user_id: str) -> str | None:
    r = requests.post(
        "https://slack.com/api/conversations.open",
        headers=slack_headers(),
        json={"users": user_id},
        timeout=30,
    )
    data = r.json()
    return data["channel"]["id"] if data.get("ok") else None

def slack_post_message(channel_id: str, text: str) -> bool:
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
    team_map = load_team_email_map()

    now = datetime.now(timezone.utc)
    window_start = now - timedelta(days=1)
    window_end = now + timedelta(days=14)

    pages = fetch_meetings(window_start, window_end)

    user_name_cache = {}
    meetings = []

    for p in pages:
        local = get_prop_text(p, "Local")
        if not local or not GCMD_REGEX.search(local):
            continue

        start, end = parse_date_range(p, "Data")
        if not start or not end:
            continue

        title = get_prop_text(p, "Evento").strip() or "(Sem título)"
        url = p.get("url", "")

        creator_id = get_creator_user_id(p)
        creator_name = ""

        if creator_id:
            if creator_id not in user_name_cache:
                user_name_cache[creator_id] = fetch_notion_user_name(creator_id) or ""
            creator_name = user_name_cache[creator_id]

        email = resolve_email_from_name(team_map, creator_name)

        meetings.append({
            "title": title,
            "url": url,
            "local": local,
            "start": start,
            "end": end,
            "creator_name": creator_name or "Pessoa não identificada",
            "creator_email": email,
        })

    meetings.sort(key=lambda m: m["start"])
    conflict_groups = build_conflict_groups(meetings)

    for group in conflict_groups:
        group_meetings = [meetings[i] for i in group]
        emails = sorted({m["creator_email"] for m in group_meetings if m["creator_email"]})

        if not emails:
            continue

        lines = [
            "⚠️ Opa! Detectei um possível conflito de agenda na sala de reuniões da GCMD.",
            "",
            "Existem duas ou mais reuniões marcadas para o mesmo local e horário. Dá uma conferida:",
        ]

        for m in group_meetings:
            lines.append(
                f"\n• 🗓️ {m['title']} - {m['url']}\n"
                f"  Criada por: {m['creator_name']}\n"
                f"  {m['start'].strftime('%d/%m/%Y, %H:%M')}–{m['end'].strftime('%H:%M')}\n"
                f"  Local: {m['local']}\n"
            )

        lines.append("👉 Vale alinhar com o pessoal e ajustar o horário ou o local, se necessário.")
        text = "\n".join(lines)

        for email in emails:
            slack_user_id = slack_lookup_user_id_by_email(email)
            if not slack_user_id:
                continue

            channel_id = slack_open_dm(slack_user_id)
            if not channel_id:
                continue

            slack_post_message(channel_id, text)

if __name__ == "__main__":
    main()
