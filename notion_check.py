import os
import requests
from datetime import datetime

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

def patch_title(page_id, novo_titulo):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {
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
    response = requests.patch(url, headers=HEADERS, json=payload)
    response.raise_for_status()

def comentar_pagina_mencionando_usuario(page_id, user_id, texto_mensagem):
    url = "https://api.notion.com/v1/comments"
    payload = {
        "parent": {"page_id": page_id},
        "rich_text": [
            {
                "type": "mention",
                "mention": {"type": "user", "user": {"id": user_id}}
            },
            {
                "type": "text",
                "text": {"content": f" {texto_mensagem}"}
            }
        ]
    }
    try:
        response = requests.post(url, headers=HEADERS, json=payload)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"⚠️ Sem permissão para criar comentários na página {page_id}. Comentário não será criado.")
        else:
            raise

def listar_comentarios(page_id):
    url = f"https://api.notion.com/v1/comments?block_id={page_id}"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"⚠️ Sem permissão para listar comentários na página {page_id}. Comentários serão ignorados.")
            return []
        else:
            raise

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
        criador = props.get("Criado por", {}).get("people", [])
        reuniao_start, reuniao_end = parse_date(data_reuniao)
        conflitos = []

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
                                conflitos.append((servidor_nome, aus_start, aus_end))

        comentarios_existentes = listar_comentarios(reuniao_id)
        comentarios_texto = [
            c["rich_text"][0]["text"]["content"]
            for c in comentarios_existentes
            if c["object"] == "comment" and c["rich_text"]
        ]

        if conflitos:
            nomes = [nome for nome, _, _ in conflitos]
            nomes_str = ", ".join(nomes)
            titulo_base = titulo_original.split("⚠️")[-1].split(" (Ausências:")[0].strip()
            novo_titulo = f"⚠️ {titulo_base} (Ausências: {nomes_str})"
            patch_title(reuniao_id, novo_titulo)
            print(f"⚠️ Conflito detectado: {novo_titulo}")

            ausencias_formatadas = []
            for nome, inicio, fim in conflitos:
                if inicio.date() == fim.date():
                    data_str = inicio.strftime("%d/%m")
                else:
                    data_str = f"{inicio.strftime('%d')} a {fim.strftime('%d/%m')}"
                ausencias_formatadas.append(f"👤 **{nome}** ({data_str})")

            texto_mensagem = f"⚠️ Ausências detectadas: {', '.join(ausencias_formatadas)}"

            if not any("Ausências detectadas" in c for c in comentarios_texto):
                if criador and len(criador) > 0:
                    user_id = criador[0]["id"]
                    comentar_pagina_mencionando_usuario(reuniao_id, user_id, texto_mensagem)
                    print(f"💬 Comentário com menção adicionado para {user_id}")
                else:
                    print("⚠️ Criador não identificado, menção não adicionada.")

        else:
            if titulo_original.startswith("⚠️"):
                titulo_limpo = titulo_original.split("⚠️")[-1].split(" (Ausências:")[0].strip()
                patch_title(reuniao_id, titulo_limpo)
                print(f"✅ Conflito resolvido: {titulo_limpo}")

            for comentario in comentarios_texto:
                if "Ausências detectadas" in comentario:
                    print("🧹 Comentário de ausência permanece, mas deve ser removido manualmente.")

if __name__ == "__main__":
    main()
