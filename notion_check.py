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

def parse_date(date_obj):
    if not date_obj:
        return None, None
    start = datetime.fromisoformat(date_obj["start"][:10])
    end = datetime.fromisoformat(date_obj["end"][:10]) if date_obj.get("end") else start
    return start, end

def date_ranges_overlap(start1, end1, start2, end2):
    return start1 <= end2 and end1 >= start2

def patch_database(page_id, campo, novo_titulo):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {
        "properties": {
            campo: {
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
    response = requests.patch(url, headers=HEADERS, json=payload)
    response.raise_for_status()

def main():
    print("🔄 Verificando conflitos entre reuniões e ausências...")

    reunioes = fetch_database(DATABASE_ID_REUNIOES)
    ausencias = fetch_database(DATABASE_ID_AUSENCIAS)

    for reuniao in reunioes:
        props = reuniao["properties"]
        participantes = props["Participantes"]["people"]
        data_reuniao = props["Data"].get("date")
        reuniao_id = reuniao["id"]
        titulo_original = props["Evento"]["title"][0]["text"]["content"] if props["Evento"]["title"] else "Sem título"

        reuniao_start, reuniao_end = parse_date(data_reuniao)
        nomes_em_conflito = []

        for participante in participantes:
            servidor_id = participante["id"]
            servidor_nome = participante.get("name", "Desconhecido")

            for ausencia in ausencias:
                props_aus = ausencia["properties"]
                if props_aus["Servidor"]["people"]:
                    if props_aus["Servidor"]["people"][0]["id"] == servidor_id:
                        data_ausencia = props_aus["Data"].get("date")
                        aus_start, aus_end = parse_date(data_ausencia)
                        if aus_start and aus_end and reuniao_start and reuniao_end:
                            if date_ranges_overlap(reuniao_start, reuniao_end, aus_start, aus_end):
                                if servidor_nome not in nomes_em_conflito:
                                    nomes_em_conflito.append(servidor_nome)

        if nomes_em_conflito:
            if titulo_original.startswith("⚠️") and "(Ausentes:" in titulo_original:
                titulo_corrigido = titulo_original.replace("(Ausentes:", "(Ausências:")
                patch_database(reuniao_id, "Evento", titulo_corrigido)
                print(f"✏️ Corrigido título: {titulo_corrigido}")
            elif not titulo_original.startswith("⚠️") or "(Ausências:" not in titulo_original:
                novo_titulo = f"⚠️ {titulo_original} (Ausências: {', '.join(nomes_em_conflito)})"
                patch_database(reuniao_id, "Evento", novo_titulo)
                print(f"⚠️ Conflito detectado: {novo_titulo}")
        else:
            if titulo_original.startswith("⚠️"):
                partes = titulo_original.split("⚠️")
                if len(partes) > 1:
                    possivel_titulo = partes[-1]
                    if "(" in possivel_titulo:
                        possivel_titulo = possivel_titulo.split(" (")[0].strip()
                    patch_database(reuniao_id, "Evento", possivel_titulo)
                    print(f"✅ Conflito resolvido: {possivel_titulo}")

if __name__ == "__main__":
    main()
