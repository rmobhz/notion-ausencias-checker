import os
from datetime import datetime, timedelta
from notion_client import Client
from dotenv import load_dotenv

load_dotenv()

notion = Client(auth=os.getenv("NOTION_TOKEN"))
DATABASE_ID = os.getenv("DATABASE_ID_CALENDARIOEDITORIAL")
DATABASE_ID_AUSENCIAS = os.getenv("DATABASE_ID_AUSENCIAS")

STATUS_IGNORADOS = ["Publicação", "Monitoramento", "Arquivado", "Concluído"]
STATUS_YOUTUBE_IGNORADOS = [
    "não teve como publicar", "Concluído", "Não houve reunião", 
    "Não teve programa", "Concluído com edição"
]
MARGEM_DIAS = 3

def obter_ausencias():
    resultados = notion.databases.query(
        **{
            "database_id": DATABASE_ID_AUSENCIAS,
            "filter": {
                "and": [
                    {"property": "Início", "date": {"is_not_empty": True}},
                    {"property": "Fim", "date": {"is_not_empty": True}},
                ]
            }
        }
    ).get("results", [])

    ausencias = []
    for a in resultados:
        nome = a["properties"]["Pessoa"]["people"][0]["name"] if a["properties"]["Pessoa"]["people"] else None
        inicio = a["properties"]["Início"]["date"]["start"]
        fim = a["properties"]["Fim"]["date"]["end"] or inicio
        if nome and inicio and fim:
            ausencias.append({
                "nome": nome,
                "inicio": datetime.fromisoformat(inicio),
                "fim": datetime.fromisoformat(fim),
            })
    return ausencias

def checar_conflito(data, pessoa, ausencias):
    if not data or not pessoa:
        return False
    data_obj = datetime.fromisoformat(data)
    for ausencia in ausencias:
        if ausencia["nome"] == pessoa:
            inicio = ausencia["inicio"]
            fim = ausencia["fim"]
            if inicio - timedelta(days=MARGEM_DIAS) <= data_obj <= fim + timedelta(days=MARGEM_DIAS):
                return True
    return False

def verificar_conflitos():
    print("🔎 Verificando conflitos entre posts e ausências...")

    ausencias = obter_ausencias()
    pagina = 0
    start_cursor = None

    total_analisados = 0
    total_alertas_adicionados = 0
    total_alertas_removidos = 0

    while True:
        response = notion.databases.query(
            **{
                "database_id": DATABASE_ID,
                "start_cursor": start_cursor,
                "page_size": 100,
            }
        )
        resultados = response.get("results", [])
        for post in resultados:
            total_analisados += 1
            propriedades = post["properties"]
            titulo_original = propriedades["Name"]["title"][0]["plain_text"] if propriedades["Name"]["title"] else ""
            titulo_com_alerta = titulo_original.startswith("⚠️")

            status = propriedades["Status"]["select"]["name"] if propriedades["Status"]["select"] else ""
            status_yt = propriedades["Status - YouTube"]["select"]["name"] if propriedades.get("Status - YouTube") and propriedades["Status - YouTube"]["select"] else ""

            ignorar = status in STATUS_IGNORADOS or status_yt in STATUS_YOUTUBE_IGNORADOS

            conflitos = []

            campos_responsaveis = [
                "Responsável", "Apoio", "Editor(a) imagem/vídeo"
            ]
            campos_datas = [
                "Veiculação", "Veiculação - YouTube", "Veiculação - TikTok"
            ]

            for campo_responsavel in campos_responsaveis:
                pessoas = propriedades[campo_responsavel]["people"]
                for pessoa in pessoas:
                    nome = pessoa["name"]
                    for campo_data in campos_datas:
                        data = propriedades[campo_data]["date"]["start"] if propriedades[campo_data]["date"] else None
                        if checar_conflito(data, nome, ausencias):
                            conflitos.append(nome)

            novo_titulo = titulo_original
            if conflitos:
                if not titulo_com_alerta:
                    novo_titulo = f"⚠️ {titulo_original} (Conflito: {', '.join(set(conflitos))})"
                    total_alertas_adicionados += 1
                elif "(Conflito:" not in titulo_original:
                    novo_titulo = f"{titulo_original} (Conflito: {', '.join(set(conflitos))})"
            else:
                if titulo_com_alerta:
                    novo_titulo = titulo_original.replace("⚠️ ", "").split(" (Conflito:")[0]
                    total_alertas_removidos += 1

            # Se for ignorado, mas ainda com alerta → remove o alerta
            if ignorar and titulo_com_alerta:
                novo_titulo = titulo_original.replace("⚠️ ", "").split(" (Conflito:")[0]
                total_alertas_removidos += 1

            # Atualiza título se necessário
            if novo_titulo != titulo_original:
                notion.pages.update(
                    page_id=post["id"],
                    properties={
                        "Name": {
                            "title": [
                                {
                                    "type": "text",
                                    "text": {"content": novo_titulo}
                                }
                            ]
                        }
                    }
                )

        if not response.get("has_more"):
            break
        start_cursor = response.get("next_cursor")
        pagina += 1

    print(f"\n🔍 Resumo da verificação:")
    print(f"• Posts analisados: {total_analisados}")
    print(f"• Alertas adicionados: {total_alertas_adicionados}")
    print(f"• Alertas removidos: {total_alertas_removidos}")

if __name__ == "__main__":
    verificar_conflitos()
