import os
import requests
from datetime import datetime, timedelta

NOTION_API_KEY = os.getenv("NOTION_API_KEY")
DATABASE_ID_CALENDARIO = os.getenv("DATABASE_ID_CALENDARIOEDITORIAL")
DATABASE_ID_AUSENCIAS = os.getenv("DATABASE_ID_AUSENCIAS")

HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

PESSOAS_ENVOLVIDAS = ["Responsável", "Apoio", "Editor(a) imagem/vídeo"]
DATAS_DE_VEICULACAO = ["Veiculação", "Veiculação - YouTube", "Veiculação - TikTok"]

def fetch_database(database_id):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    response = requests.post(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()["results"]

def parse_date(date_obj):
    if not date_obj:
        return None
    return datetime.fromisoformat(date_obj["start"][:10])

def verificar_ausencia(pessoa_id, ausencias, margem_inicio, margem_fim):
    for ausencia in ausencias:
        props = ausencia["properties"]
        if props["Servidor"]["people"]:
            if props["Servidor"]["people"][0]["id"] == pessoa_id:
                data_ausencia = props["Data"].get("date")
                if data_ausencia:
                    aus_start = parse_date(data_ausencia)
                    aus_end = parse_date({"start": data_ausencia["end"]}) if data_ausencia.get("end") else aus_start
                    if aus_start <= margem_fim and aus_end >= margem_inicio:
                        return True
    return False

def main():
    print("🔄 Verificando ausências no Calendário Editorial...")

    posts = fetch_database(DATABASE_ID_CALENDARIO)
    ausencias = fetch_database(DATABASE_ID_AUSENCIAS)

    for post in posts:
        props = post["properties"]
        titulo = props["Nome"]["title"][0]["text"]["content"] if props["Nome"]["title"] else "Sem título"
        post_id = post["id"]
        pessoas_envolvidas = []

        for campo in PESSOAS_ENVOLVIDAS:
            if campo in props and props[campo]["people"]:
                for pessoa in props[campo]["people"]:
                    pessoas_envolvidas.append((pessoa["id"], pessoa.get("name", "Desconhecido")))

        for campo_data in DATAS_DE_VEICULACAO:
            if campo_data in props and props[campo_data]["date"]:
                data_veiculacao = parse_date(props[campo_data]["date"])
                if data_veiculacao:
                    margem_inicio = data_veiculacao - timedelta(days=3)
                    margem_fim = data_veiculacao

                    for pessoa_id, pessoa_nome in pessoas_envolvidas:
                        if verificar_ausencia(pessoa_id, ausencias, margem_inicio, margem_fim):
                            print(f"⚠️ {titulo} – {pessoa_nome} estará ausente antes de {campo_data.lower()} ({data_veiculacao.date()})")
                            break  # Opcional: parar após primeira ausência detectada para essa data

if __name__ == "__main__":
    main()
