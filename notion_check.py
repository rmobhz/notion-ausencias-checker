import os
import requests
from datetime import datetime, timedelta

NOTION_API_KEY = os.getenv("NOTION_API_KEY")
DATABASE_ID_REUNIOES = os.getenv("DATABASE_ID_REUNIOES")
DATABASE_ID_AUSENCIAS = os.getenv("DATABASE_ID_AUSENCIAS")

HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

def fetch_database(database_id):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    response = requests.post(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()["results"]

def parse_data(prop_data):
    if prop_data["type"] == "date":
        date_info = prop_data["date"]
        if date_info:
            start = datetime.fromisoformat(date_info["start"])
            end = datetime.fromisoformat(date_info.get("end") or date_info["start"])
            return start.date(), end.date()
    return None, None

def date_ranges_overlap(start1, end1, start2, end2):
    return start1 <= end2 and start2 <= end1

def main():
    print("🔄 Verificando conflitos entre reuniões e ausências...")

    reunioes = fetch_database(DATABASE_ID_REUNIOES)
    ausencias = fetch_database(DATABASE_ID_AUSENCIAS)

    for reuniao in reunioes:
        evento_id = reuniao["id"]
        props = reuniao["properties"]
        data_reuniao = props.get("Data")
        participantes = props.get("Participantes", {}).get("people", [])
        titulo = props.get("Evento", {}).get("title", [])
        titulo_texto = titulo[0]["text"]["content"] if titulo else "Sem título"
        start_r, end_r = parse_data(data_reuniao)

        if not (start_r and participantes):
            continue

        servidores_em_conflito = []

        for ausencia in ausencias:
            props_aus = ausencia["properties"]
            data_aus = props_aus.get("Data")
            servidor = props_aus.get("Servidor", {}).get("people", [])
            start_a, end_a = parse_data(data_aus)

            if not (start_a and servidor):
                continue

            servidor_info = servidor[0]
            servidor_id = servidor_info["id"]
            servidor_nome = servidor_info.get("name") or servidor_info.get("id", "Desconhecido")

            if any(p["id"] == servidor_id for p in participantes):
                if date_ranges_overlap(start_r, end_r, start_a, end_a):
                    servidores_em_conflito.append(servidor_nome)

        if servidores_em_conflito:
            novo_titulo = f"⚠️ {titulo_texto} – Conflito: {', '.join(servidores_em_conflito)}"
            print(f"⚠️ Conflito encontrado em '{titulo_texto}': {servidores_em_conflito}")
            update_url = f"https://api.notion.com/v1/pages/{evento_id}"
            update_data = {
                "properties": {
                    "Evento": {
                        "title": [
                            {
                                "text": {
                                    "content": novo_titulo
                                }
                            }
                        ]
                    }
                }
            }
            response = requests.patch(update_url, headers=HEADERS, json=update_data)
            response.raise_for_status()
        else:
            print(f"✅ Sem conflito: {titulo_texto}")

if __name__ == "__main__":
    main()
