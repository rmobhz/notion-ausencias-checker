import requests
import datetime
import pytz
import os

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("DATABASE_ID_CALENDARIOEDITORIAL")

headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# Pessoas a verificar
pessoas_para_verificar = ["Fernanda Domingos"]

def fetch_database_pages(database_id, headers, filtro):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    all_results = []
    payload = {
        "filter": filtro,
        "page_size": 100
    }

    has_more = True
    start_cursor = None

    while has_more:
        if start_cursor:
            payload["start_cursor"] = start_cursor
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        all_results.extend(data["results"])
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor", None)

    return all_results

def fetch_ausencias(headers, pessoa, data_post):
    url = "https://api.notion.com/v1/databases"
    filtro = {
        "and": [
            {
                "property": "Pessoa",
                "people": {
                    "contains": pessoa
                }
            },
            {
                "property": "Início",
                "date": {
                    "on_or_before": data_post
                }
            },
            {
                "property": "Fim",
                "date": {
                    "on_or_after": data_post
                }
            }
        ]
    }
    payload = {
        "filter": filtro
    }
    response = requests.post(f"{url}/{os.getenv('DATABASE_ID_AUSENCIAS')}/query", headers=headers, json=payload)
    response.raise_for_status()
    return response.json().get("results", [])

def get_property(page, nome_campo):
    props = page["properties"].get(nome_campo)
    if not props:
        return None
    tipo = props["type"]
    if tipo == "title":
        return props["title"][0]["plain_text"] if props["title"] else None
    elif tipo == "date":
        return props["date"]["start"] if props["date"] else None
    elif tipo == "people":
        return [p["name"] for p in props["people"]]
    return None

def verificar_conflitos():
    print("🔄 Verificando ausências no Calendário Editorial...")

    filtro = {
        "or": [
            {"property": "Veiculação", "date": {"is_not_empty": True}},
            {"property": "Veiculação - YouTube", "date": {"is_not_empty": True}},
            {"property": "Veiculação - TikTok", "date": {"is_not_empty": True}},
        ]
    }

    posts = fetch_database_pages(DATABASE_ID, headers, filtro)
    print(f"📄 Total de posts retornados: {len(posts)}")

    for post in posts:
        titulo = get_property(post, "Título")
        print(f"🔍 Post encontrado: {titulo}")

        campos_datas = ["Veiculação", "Veiculação - YouTube", "Veiculação - TikTok"]
        campos_pessoas = ["Responsável", "Apoio", "Editor(a) imagem/vídeo"]

        for campo_data in campos_datas:
            data_str = get_property(post, campo_data)
            if not data_str:
                continue

            # Converte string para data
            data_post = datetime.datetime.fromisoformat(data_str).astimezone(pytz.timezone("America/Sao_Paulo")).date()

            # Aplica margem de 3 dias antes
            margem_inicio = data_post - datetime.timedelta(days=3)
            margem_fim = data_post

            for campo_pessoa in campos_pessoas:
                pessoas = get_property(post, campo_pessoa)
                if not pessoas:
                    continue

                for pessoa in pessoas:
                    if pessoa not in pessoas_para_verificar:
                        continue

                    print(f"👤 Verificando {pessoa} no post: {titulo}")
                    print(f"📅 Data de veiculação: {data_post} (margem de {margem_inicio} até {margem_fim})")

                    data_cursor = margem_inicio
                    conflito_encontrado = False

                    while data_cursor <= margem_fim:
                        ausencias = fetch_ausencias(headers, pessoa, data_cursor.isoformat())
                        if ausencias:
                            print(f"⚠️ Conflito encontrado para {pessoa} no dia {data_cursor} no post: {titulo}")
                            conflito_encontrado = True
                            break
                        data_cursor += datetime.timedelta(days=1)

                    if not conflito_encontrado:
                        print(f"✅ Nenhuma ausência nessa margem.")

if __name__ == "__main__":
    print("🔎 Verificando conflitos entre reuniões e ausências...")
    verificar_conflitos()
