import os
import requests
from datetime import datetime

NOTION_API_KEY = os.environ["NOTION_API_KEY"]
DATABASE_ID_REUNIOES = os.environ["DATABASE_ID_REUNIOES"]
DATABASE_ID_AUSENCIAS = os.environ["DATABASE_ID_AUSENCIAS"]
HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

def fetch_database(database_id):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    response = requests.post(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()["results"]

def parse_date(date_obj):
    if not date_obj:
        return None, None
    start = date_obj["start"]
    end = date_obj["end"] or start
    return datetime.fromisoformat(start), datetime.fromisoformat(end)

def check_overlap(start1, end1, start2, end2):
    return start1 <= end2 and start2 <= end1

def main():
    print("🔄 Verificando conflitos entre reuniões e ausências...")

    reunioes = fetch_database(DATABASE_ID_REUNIOES)
    ausencias = fetch_database(DATABASE_ID_AUSENCIAS)

    for reuniao in reunioes:
        reuniao_id = reuniao["id"]
        nome_reuniao = reuniao["properties"].get("Evento", {}).get("title", [])
        data_reuniao_raw = reuniao["properties"].get("Data", {}).get("date")
        participantes = reuniao["properties"].get("Participantes", {}).get("people", [])

        if not data_reuniao_raw or not participantes or not nome_reuniao:
            continue

        reuniao_inicio, reuniao_fim = parse_date(data_reuniao_raw)
        nome_original = nome_reuniao[0]["plain_text"]

        servidores_em_conflito = []

        for ausencia in ausencias:
            data_ausencia_raw = ausencia["properties"].get("Data", {}).get("date")
            servidor = ausencia["properties"].get("Servidor", {}).get("people", [])
            if not data_ausencia_raw or not servidor:
                continue

            ausencia_inicio, ausencia_fim = parse_date(data_ausencia_raw)
            servidor_id = servidor[0]["id"]
            servidor_nome = servidor[0]["name"]

            # Se servidor está participando da reunião e tem ausência no mesmo período
            if any(p["id"] == servidor_id for p in participantes):
                if check_overlap(reuniao_inicio, reuniao_fim, ausencia_inicio, ausencia_fim):
                    servidores_em_conflito.append(servidor_nome)

        # Atualiza nome da reunião, se necessário
        if servidores_em_conflito:
            conflito_nomes = ", ".join(servidores_em_conflito)
            novo_nome = f"⚠️ {nome_original} ({conflito_nomes} ausentes)"
        else:
            novo_nome = nome_original

        # Atualiza a página apenas se o nome estiver diferente
        if nome_reuniao[0]["plain_text"] != novo_nome:
            update_url = f"https://api.notion.com/v1/pages/{reuniao_id}"
            update_data = {
                "properties": {
                    "Evento": {
                        "title": [{"text": {"content": novo_nome}}]
                    }
                }
            }
            r = requests.patch(update_url, headers=HEADERS, json=update_data)
            r.raise_for_status()
            print(f"🔁 Reunião atualizada: {novo_nome}")

if __name__ == "__main__":
    main()
