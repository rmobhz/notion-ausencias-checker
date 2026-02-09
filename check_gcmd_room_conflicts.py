import os
import re
import unicodedata
import requests
from datetime import datetime, timedelta, timezone
from dateutil import parser as dateparser

NOTION_API_KEY = os.getenv("NOTION_API_KEY")
DATABASE_ID_REUNIOES = os.getenv("DATABASE_ID_REUNIOES")
DATABASE_ID_EQUIPE_GCMD = os.getenv("DATABASE_ID_EQUIPE_GCMD")  # <-- novo
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")

NOTION_VERSION = "2022-06-28"
GCMD_REGEX = re.compile(r"gcmd", re.IGNORECASE)

# ====== Notion ======

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
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")  # remove acentos
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
        end = start + timedelta(hours=1)  # regra: sem fim = 1h

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

def load_team_email_map():
    """
    Lê a base Equipe | GCMD e monta um mapa por nome normalizado -> email.
    Espera propriedades: 'Nome' e 'E-mail' (como você disse).
    """
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

def resolve_email_from_created_by(team_map: dict, created_by_name: str | None) -> str | None:
    """
    Tenta achar o e-mail na base Equipe | GCMD a partir do nome do created_by.
    Estratégia:
      1) match exato (normalizado)
      2) match por "contém" (duas direções)
    """
    if not created_by_name:
        return None

    cb = normalize_str(created_by_name)
    if not cb:
        return None

    # 1) exato
    if cb in team_map:
        return team_map[cb]

    # 2) contém (ex.: "rafael marques" vs "rafael marques (gcmd)")
    for team_name_norm, email in team_map.items():
        if cb in team_name_norm or team_name_norm in cb:
            return email

    return None

# ====== Conflitos ======

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
            if intervals_overlap(meetings[i]["start"], meetings[i]["end"], meetings[j]["start"], meetings[j]["end"]):
                group.append(j)

        if len(group) < 2:
            continue

        changed = True
        while changed:
            changed = False
            for j in range(n):
                if j in group:
                    continue
                if any(intervals_overlap(meetings[k]["start"], meetings[k]["end"], meetings[j]["start"], meetings[j]["end"]) for k in group):
                    group.append(j)
                    changed = True

        for idx in group:
            used.add(idx)
        groups.append(group)

    return groups

# ====== Slack ======

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
    if not data.get("ok"):
        return None
    return data["user"]["id"]

def slack_open_dm(user_id: str) -> str | None:
    r = requests.post(
        "https://slack.com/api/conversations.open",
        headers=slack_headers(),
        json={"users": user_id},
        timeout=30,
    )
    data = r.json()
    if not data.get("ok"):
        return None
    return data["channel"]["id"]

def slack_post_message(channel_id: str, text: str) -> bool:
    r = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers=slack_headers(),
        json={"channel": channel_id, "text": text},
        timeout=30,
    )
    data = r.json()
    return bool(data.get("ok"))

# ====== Main ======

def main():
    missing = [k for k, v in {
        "NOTION_API_KEY": NOTION_API_KEY,
        "DATABASE_ID_REUNIOES": DATABASE_ID_REUNIOES,
        "DATABASE_ID_EQUIPE_GCMD": DATABASE_ID_EQUIPE_GCMD,
        "SLACK_BOT_TOKEN": SLACK_BOT_TOKEN,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Faltam variáveis de ambiente: {', '.join(missing)}")

    print("[INFO] Carregando mapa de e-mails da base Equipe | GCMD...")
    team_map = load_team_email_map()
    print(f"[INFO] Pessoas mapeadas na Equipe | GCMD: {len(team_map)}")

    now = datetime.now(timezone.utc)
    window_start = now - timedelta(days=1)
    window_end = now + timedelta(days=14)

    pages = fetch_meetings(window_start, window_end)

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

        created_by = p.get("created_by") or {}
        created_by_name = created_by.get("name") or ""

        email = resolve_email_from_created_by(team_map, created_by_name)

        meetings.append({
            "title": title,
            "url": url,
            "local": local,
            "start": start,
            "end": end,
            "created_by_name": created_by_name,
            "created_by_email": email,
        })

    meetings.sort(key=lambda m: m["start"])
    conflict_groups = build_conflict_groups(meetings)

    print(f"[INFO] Reuniões GCMD analisadas: {len(meetings)}")
    print(f"[INFO] Grupos de conflito: {len(conflict_groups)}")

    for group in conflict_groups:
        group_meetings = [meetings[i] for i in group]

        emails = sorted({m["created_by_email"] for m in group_meetings if m["created_by_email"]})
        missing_people = sorted({m["created_by_name"] for m in group_meetings if not m["created_by_email"]})

        if not emails:
            print("[WARN] Conflito encontrado, mas não consegui mapear NENHUM e-mail via Equipe | GCMD.")
            print("       Criadores sem match:", ", ".join(missing_people) if missing_people else "(vazio)")
            for m in group_meetings:
                print(f"       - {m['title']} | {m['start'].isoformat()} | {m['created_by_name']} | {m['url']}")
            continue

        lines = ["⚠️ Conflito de sala (GCMD): há reuniões sobrepostas no mesmo horário/local."]
        for m in group_meetings:
            lines.append(
                f"• {m['title']} — {m['start'].isoformat()} → {m['end'].isoformat()} — Local: {m['local']} — {m['url']}"
            )

        if missing_people:
            lines.append("")
            lines.append("⚠️ Obs.: Não consegui identificar o e-mail (Equipe | GCMD) para:")
            for name in missing_people:
                lines.append(f"• {name}")

        text = "\n".join(lines)

        for email in emails:
            user_id = slack_lookup_user_id_by_email(email)
            if not user_id:
                print(f"[WARN] Slack lookupByEmail falhou para: {email}")
                continue

            channel_id = slack_open_dm(user_id)
            if not channel_id:
                print(f"[WARN] conversations.open falhou para user_id={user_id} (email={email})")
                continue

            ok = slack_post_message(channel_id, text)
            if not ok:
                print(f"[WARN] chat.postMessage falhou em channel_id={channel_id} (email={email})")

    print("[DONE] Execução finalizada.")

if __name__ == "__main__":
    main()
