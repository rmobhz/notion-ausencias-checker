import os
import requests
import json
from datetime import datetime, timedelta

NOTION_API_KEY = os.getenv("NOTION_API_KEY")
DATABASE_ID_CALENDARIO = os.getenv("DATABASE_ID_CALENDARIOEDITORIAL")
DATABASE_ID_AUSENCIAS = os.getenv("DATABASE_ID_AUSENCIAS")

HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

PESSOAS_ENVOLVIDAS = ["Responsável", "Editor(a) imagem/vídeo"]
DATAS_DE_VEICULACAO = ["Veiculação", "Veiculação - YouTube", "Veiculação - TikTok"]

STATUS_IGNORADOS = ["Publicação", "Monitoramente", "Arquivado", "Concluído"]
STATUS_YOUTUBE_IGNORADOS = ["não teve como publicar", "Concluído", "Não houve reunião", 
                           "Não teve programa", "Concluído com edição"]

def fetch_database(database_id, page_size=100):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    all_results = []
    has_more = True
    start_cursor = None
    
    while has_more:
        payload = { "page_size": page_size }
        if start_cursor:
            payload["start_cursor"] = start_cursor

        response = requests.post(url, headers=HEADERS, json=payload)
        response.raise_for_status()
        data = response.json()
        all_results.extend(data["results"])
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    return all_results

def parse_date(date_obj):
    if not date_obj:
        return None
    return datetime.fromisoformat(date_obj["start"][:10])

def verificar_ausencias_para_pessoa(pessoa_id, ausencias, margem_inicio, margem_fim):
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

def atualizar_titulo(post_id, titulo_original, nomes_conflito):
    novo_titulo = f"⚠️ {titulo_original} (Conflito: {', '.join(nomes_conflito)})"
    url = f"https://api.notion.com/v1/pages/{post_id}"
    data = {
        "properties": {
            "Título": {
                "title": [{"text": {"content": novo_titulo}}]
            }
        }
    }
    response = requests.patch(url, headers=HEADERS, json=data)
    response.raise_for_status()

def remover_alerta_titulo(post_id, titulo_com_alerta):
    if not titulo_com_alerta.startswith("⚠️"):
        return

    titulo_limpo = titulo_com_alerta.replace("⚠️ ", "").split(" (Conflito:")[0].strip()

    url = f"https://api.notion.com/v1/pages/{post_id}"
    data = {
        "properties": {
            "Título": {
                "title": [{"text": {"content": titulo_limpo}}]
            }
        }
    }
    response = requests.patch(url, headers=HEADERS, json=data)
    response.raise_for_status()

def deve_ignorar_post(props):
    status = props.get("Status", {}).get("select", {}).get("name", "")
    if status in STATUS_IGNORADOS:
        return True

    status_yt = props.get("Status - YouTube", {}).get("select", {}).get("name", "")
    if status_yt in STATUS_YOUTUBE_IGNORADOS:
        return True

    return False

def main():
    print("\n🔄 Verificando conflitos no Calendário Editorial...")
    print("⏳ Buscando posts no calendário editorial...")
    posts = fetch_database(DATABASE_ID_CALENDARIO)
    print(f"✅ Encontrados {len(posts)} posts no calendário")

    print("⏳ Buscando ausências registradas...")
    ausencias = fetch_database(DATABASE_ID_AUSENCIAS)
    print(f"✅ Encontradas {len(ausencias)} ausências\n")

    posts_com_alerta = 0
    alertas_removidos = 0
    ignorados = 0

    for post in posts:
        props = post["properties"]
        titulo_raw = props.get("Título", {}).get("title", [{}])
        if not titulo_raw or not titulo_raw[0].get("text", {}).get("content"):
            continue

        titulo_atual = titulo_raw[0]["text"]["content"]
        post_id = post["id"]

        print(f"\n🔍 Propriedades completas do post: {titulo_atual}")
        print(json.dumps(props, indent=2, ensure_ascii=False))
        status = props.get("Status", {}).get("select", {}).get("name", "")
        status_yt = props.get("Status - YouTube", {}).get("select", {}).get("name", "")        
        if status in STATUS_IGNORADOS or status_yt in STATUS_YOUTUBE_IGNORADOS:
            print(f"Ignorando post '{titulo_atual[:30]}' com status '{status}' e status YT '{status_yt}'")
        else:
            print(f"Analisando post '{titulo_atual[:30]}' com status '{status}' e status YT '{status_yt}'")
        if not status and not status_yt:
            print(f"⚠️ [STATUS VAZIO] Post '{titulo_atual[:50]}' sem status definido!")

        if deve_ignorar_post(props):
            ignorados += 1
            if titulo_atual.startswith("⚠️"):
                remover_alerta_titulo(post_id, titulo_atual)
                alertas_removidos += 1
                print(f"✅ [STATUS IGNORADO] Alerta removido: {titulo_atual[:50]}...")
            continue

        pessoas_envolvidas = []
        for campo in PESSOAS_ENVOLVIDAS:
            if campo in props and props[campo].get("people"):
                for pessoa in props[campo]["people"]:
                    pessoas_envolvidas.append((pessoa["id"], pessoa.get("name", "Desconhecido")))

        nomes_com_conflito = set()
        for campo_data in DATAS_DE_VEICULACAO:
            if campo_data in props and props[campo_data].get("date"):
                data_veiculacao = parse_date(props[campo_data]["date"])
                if data_veiculacao:
                    margem_inicio = data_veiculacao - timedelta(days=3)
                    margem_fim = data_veiculacao

                    for pessoa_id, pessoa_nome in pessoas_envolvidas:
                        if verificar_ausencias_para_pessoa(pessoa_id, ausencias, margem_inicio, margem_fim):
                            nomes_com_conflito.add(pessoa_nome)

        nomes_conflito = sorted(list(nomes_com_conflito))

        if nomes_conflito:
            if not titulo_atual.startswith("⚠️") or "Conflito:" not in titulo_atual:
                titulo_original = titulo_atual.replace("⚠️ ", "").split(" (Conflito:")[0].strip()
                atualizar_titulo(post_id, titulo_original, nomes_conflito)
                posts_com_alerta += 1
                print(f"⚠️ [CONFLITO DETECTADO] {titulo_original[:50]}... → {', '.join(nomes_conflito)}")
        else:
            if titulo_atual.startswith("⚠️") and "Conflito:" in titulo_atual:
                remover_alerta_titulo(post_id, titulo_atual)
                alertas_removidos += 1
                print(f"✅ [SEM CONFLITO] Alerta removido: {titulo_atual[:50]}...")

    print(f"\n🔍 Resumo da verificação:")
    print(f"• Posts analisados: {len(posts)}")
    print(f"• Alertas adicionados: {posts_com_alerta}")
    print(f"• Alertas removidos: {alertas_removidos}")
    print(f"• Posts com status ignorado: {ignorados}")
    print("✅ Verificação concluída!\n")

if __name__ == "__main__":
    main()
