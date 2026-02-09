import os
import re
import requests
from datetime import datetime, timedelta, timezone
from dateutil import parser as dateparser

NOTION_API_KEY = os.getenv("NOTION_API_KEY")
DATABASE_ID_REUNIOES = os.getenv("DATABASE_ID_REUNIOES")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")

NOTION_VERSION = "2022-06-28"
GCMD_REGEX = re.compile(r"gcmd", re.IGNORECASE)

# ---------- Notion helpers ----------

def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }

def get_prop_text(page: dict, prop_name: str) -> str:
    prop = page.get("properties", {}).get(prop_name)
    if not prop:
        return ""
    t = prop.get("type")

    if t == "title":
        return "".join(x.get("plain_text", "") for x in prop.get("title", []))
    if t == "rich_text":
        return "".join(x.get("plain_text", "") for x in prop.get("rich_text", []))
    if t == "select":
        sel = prop.get("select")
        return (sel or {}).get("name", "") if sel else ""
    if t == "multi_select":
        return ", ".join(x.get("name", "") for x in prop.get("multi_select", []))
    return ""

def parse_date_range(page: dict, date_prop_name: str = "Data"):
    date_obj = page.get("properties", {}).get(date_prop_name, {}).get("date")
    if not date_obj or not date_obj.get("start"):
        return None, None

    start = dateparser.isoparse(date_obj["start"])
    if date_obj.get("end"):
        end = dateparser.isoparse(date_obj["end"])
    else:
        # regra: sem fim = 1 hora
        end = start + timedelta(hours=1)

    return start, end

def fetch_meetings(window_start: datetime, window_end: datetime):
    """
    Busca reuniões na janela de datas.
    Observação: filtro por "on_or_after / on_or_before" opera por data, não por timestamp.
    """
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID_REUNIOES}/query"
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

    results = []
    start_cursor = None

    while True:
        if start_cursor:
            payload["start_cursor"] = start_cursor

        r = requests.post(url, headers=notion_headers(), json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()

        results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        start_cursor = data.get("next_cursor")

    return results

def get_created_by_email(page: dict) -> str | None:
    """
    Assumindo que 'created_by' fornece email.
    Se não vier, retorna None e o script segue sem quebrar.
    """
    created_by = page.get("created_by") or {}
    person = created_by.get("person") or {}
    email = person.get("email")
    if email and isinstance(email, str) and "@" in email:
        return email.strip()
    return None

# ---------- Conflict detection ----------

def intervals_overlap(a_start, a_end, b_start, b_end) -> bool:
    return a_start < b_end and a_end > b_start

def build_conflict_groups(meetings: list[dict]) -> list[list[int]]:
    """
    Gera grupos de conflito com fecho transitivo:
    se A conflita com B e B conflita com C, ficam no mesmo grupo.
    """
    n = len(meetings)
    groups = []
    used = set()

    for i in range(n):
        if i in used:
            continue

        group = [i]
        # primeira varredura
        for j in range(i + 1, n):
            if intervals_overlap(meetings[i]["start"], meetings[i]["end"], meetings[j]["start"], meetings[j]["end"]):
                group.append(j)

        if len(group) < 2:
            continue

        # fecho transitivo
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

# ---------- Slack helpers ----------

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
    # com im:write, isso deve funcionar
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

# ---------- Main ----------

def main():
    # valida env
    missing = [k for k, v in {
        "NOTION_API_KEY": NOTION_API_KEY,
        "DATABASE_ID_REUNIOES": DATABASE_ID_REUNIOES,
        "SLACK_BOT_TOKEN": SLACK_BOT_TOKEN,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Faltam variáveis de ambiente: {', '.join(missing)}")

    # janela (ajuste se quiser)
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
        created_email = get_created_by_email(p)

        meetings.append({
            "id": p.get("id"),
            "title": title,
            "url": url,
            "local": local,
            "start": start,
            "end": end,
            "created_by_email": created_email,
        })

    # detecta conflitos
    meetings.sort(key=lambda m: m["start"])
    conflict_groups = build_conflict_groups(meetings)

    print(f"[INFO] Reuniões GCMD analisadas: {len(meetings)}")
    print(f"[INFO] Grupos de conflito: {len(conflict_groups)}")

    # notifica
    for group in conflict_groups:
        group_meetings = [meetings[i] for i in group]

        # quem notificar: "Criado por" de todas as reuniões envolvidas
        emails = sorted({m["created_by_email"] for m in group_meetings if m["created_by_email"]})
        if not emails:
            print("[WARN] Conflito encontrado, mas nenhum email disponível em created_by.person.email.")
            for m in group_meetings:
                print(f"       - {m['title']} | {m['start'].isoformat()} | {m['url']}")
            continue

        # mensagem
        lines = ["⚠️ Conflito de sala (GCMD): há reuniões sobrepostas no mesmo horário/local."]
        for m in group_meetings:
            lines.append(
                f"• {m['title']} — {m['start'].isoformat()} → {m['end'].isoformat()} — Local: {m['local']} — {m['url']}"
            )
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
