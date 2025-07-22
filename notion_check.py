import os
import requests
from datetime import datetime
import re

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

def extrair_nome_original(titulo):
    # Remove prefixo "⚠️" e sufixos como "– Conflito: ..." ou "(Ausentes: ...)"
    titulo = titulo.strip()
    if titulo.startswith("⚠️"):
        titulo = titulo[2:].strip()
    titulo = re.sub(r"\s+[–-]\s+Conflito:.*$", "", titulo)
    titulo = re.sub(r"\s+\(Ausentes:.*\)$", "", titulo)
    return titulo.strip()

def atualizar_titulo(evento_id, novo_titulo):
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
        titulo_atual = titulo[0]["text"]["content"] if titulo else "Sem título"
        titulo_original = extrair_nome_original(titulo_atual)
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
            novo_titulo = f"⚠️ {titulo_original} (Ausentes: {', '.join(servidores_em_conflito)})"
            if titulo_atual != novo_titulo:
                print(f"⚠️ Conflito em '{titulo_original}': {servidores_em_conflito}")
                atualizar_titulo(evento_id, novo_titulo)
        else:
            if titulo_atual.startswith("⚠️"):
                print(f"✅ Conflito resolvido: restaurando nome original '{titulo_original}'")
                atualizar_titulo(evento_id, titulo_original)
            else:
                print(f"✅ Sem conflito: {titulo_atual}")

if __name__ == "__main__":
    main()
