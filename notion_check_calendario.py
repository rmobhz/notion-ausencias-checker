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

def fetch_database(database_id, page_size=100):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    all_results = []
    has_more = True
    start_cursor = None
    
    while has_more:
        payload = {
            "page_size": page_size
        }
        
        if start_cursor:
            payload["start_cursor"] = start_cursor
        
        response = requests.post(url, headers=HEADERS, json=payload)
        response.raise_for_status()
        data = response.json()
        
        all_results.extend(data["results"])
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")
        
        # Opcional: delay para evitar rate limiting
        # time.sleep(0.1)
    
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
    # Remove prefixo e sufixo - agora tratando tanto "Ausências:" quanto "Conflito:"
    if not titulo_com_alerta.startswith("⚠️"):
        return
    
    # Trata títulos com o padrão antigo ("Ausências:") ou novo ("Conflito:")
    if "(Ausências:" in titulo_com_alerta:
        titulo_limpo = titulo_com_alerta.replace("⚠️ ", "").split(" (Ausências:")[0].strip()
    elif "(Conflito:" in titulo_com_alerta:
        titulo_limpo = titulo_com_alerta.replace("⚠️ ", "").split(" (Conflito:")[0].strip()
    else:
        titulo_limpo = titulo_com_alerta.replace("⚠️ ", "").split(" (")[0].strip()
    
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

def migrar_titulos_antigos(post_id, titulo_atual):
    """Atualiza títulos com o padrão antigo ('Ausências:') para o novo padrão ('Conflito:')"""
    if "(Ausências:" in titulo_atual:
        # Extrai os nomes do padrão antigo
        partes = titulo_atual.split("(Ausências: ")
        if len(partes) > 1:
            titulo_base = partes[0].replace("⚠️ ", "").strip()
            nomes = partes[1].rstrip(")").strip()
            # Recria o título com o novo padrão
            novo_titulo = f"⚠️ {titulo_base} (Conflito: {nomes})"
            
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
            print(f"🔄 Título migrado de 'Ausências' para 'Conflito': {titulo_atual} → {novo_titulo}")

def main():
    print("🔄 Verificando conflitos no Calendário Editorial...")

    posts = fetch_database(DATABASE_ID_CALENDARIO)
    ausencias = fetch_database(DATABASE_ID_AUSENCIAS)

    for post in posts:
        props = post["properties"]
        titulo_raw = props["Título"]["title"]
        if not titulo_raw:
            continue

        titulo_atual = titulo_raw[0]["text"]["content"]
        post_id = post["id"]
        
        # Primeiro, verifica e migra títulos com o padrão antigo
        if "(Ausências:" in titulo_atual:
            migrar_titulos_antigos(post_id, titulo_atual)
            # Atualiza o título atual após migração
            titulo_atual = titulo_atual.replace("(Ausências:", "(Conflito:")
        
        pessoas_envolvidas = []

        for campo in PESSOAS_ENVOLVIDAS:
            if campo in props and props[campo]["people"]:
                for pessoa in props[campo]["people"]:
                    pessoas_envolvidas.append((pessoa["id"], pessoa.get("name", "Desconhecido")))

        nomes_com_conflito = set()

        for campo_data in DATAS_DE_VEICULACAO:
            if campo_data in props and props[campo_data]["date"]:
                data_veiculacao = parse_date(props[campo_data]["date"])
                if data_veiculacao:
                    margem_inicio = data_veiculacao - timedelta(days=3)
                    margem_fim = data_veiculacao

                    for pessoa_id, pessoa_nome in pessoas_envolvidas:
                        if verificar_ausencias_para_pessoa(pessoa_id, ausencias, margem_inicio, margem_fim):
                            nomes_com_conflito.add(pessoa_nome)

        nomes_conflito = sorted(list(nomes_com_conflito))

        # Atualiza ou remove alerta no título conforme necessário
        if nomes_conflito:
            if not titulo_atual.startswith("⚠️") or "Conflito:" not in titulo_atual:
                titulo_original = titulo_atual.replace("⚠️ ", "").split(" (Conflito:")[0].strip()
                if "(Ausências:" in titulo_original:  # Caso extra de migração
                    titulo_original = titulo_original.split(" (Ausências:")[0].strip()
                atualizar_titulo(post_id, titulo_original, nomes_conflito)
                print(f"⚠️ Conflito detectado no post: {titulo_original} – {', '.join(nomes_conflito)}")
        else:
            if titulo_atual.startswith("⚠️") and ("Conflito:" in titulo_atual or "Ausências:" in titulo_atual):
                remover_alerta_titulo(post_id, titulo_atual)
                print(f"✅ Alerta removido do post: {titulo_atual}")

if __name__ == "__main__":
    main()
