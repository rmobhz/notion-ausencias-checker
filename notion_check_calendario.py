import os
import datetime
from notion_client import Client
from dotenv import load_dotenv

load_dotenv()

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("DATABASE_ID_CALENDARIOEDITORIAL")
DATABASE_AUSENCIAS = os.getenv("DATABASE_ID_AUSENCIAS")

notion = Client(auth=NOTION_TOKEN)

STATUS_IGNORADOS = ["Publicação", "Monitoramente", "Arquivado", "Concluído"]
STATUS_YOUTUBE_IGNORADOS = [
    "não teve como publicar",
    "Concluído",
    "Não houve reunião",
    "Não teve programa",
    "Concluído com edição",
]

CAMPOS_RESPONSAVEIS = ["Responsável", "Editor(a) imagem/vídeo"]

MARGEM_DIAS = 3
HOJE = datetime.datetime.now().date()

print("🔄 Verificando conflitos entre ausências e posts do calendário editorial...")

res = notion.databases.query(
    **{
        "database_id": DATABASE_ID,
        "page_size": 100,
    }
)
posts = res["results"]

while res.get("has_more"):
    res = notion.databases.query(
        **{
            "database_id": DATABASE_ID,
            "start_cursor": res["next_cursor"],
            "page_size": 100,
        }
    )
    posts.extend(res["results"])

res_ausencias = notion.databases.query(
    **{
        "database_id": DATABASE_AUSENCIAS,
        "page_size": 100,
    }
)
ausencias = res_ausencias["results"]

while res_ausencias.get("has_more"):
    res_ausencias = notion.databases.query(
        **{
            "database_id": DATABASE_AUSENCIAS,
            "start_cursor": res_ausencias["next_cursor"],
            "page_size": 100,
        }
    )
    ausencias.extend(res_ausencias["results"])

print(f"• Posts analisados: {len(posts)}")

def get_prop_text(prop):
    if prop is None:
        return ""
    if prop.get("type") == "select":
        return prop["select"]["name"] if prop["select"] else ""
    return ""

def get_date(prop):
    if prop and prop.get("date") and prop["date"].get("start"):
        return datetime.datetime.fromisoformat(prop["date"]["start"]).date()
    return None

def get_responsaveis(post):
    pessoas = set()
    for campo in CAMPOS_RESPONSAVEIS:
        if campo in post["properties"]:
            prop = post["properties"][campo]
            if prop["type"] == "people":
                for pessoa in prop["people"]:
                    pessoas.add(pessoa["id"])
    return pessoas

alertas_adicionados = 0
alertas_removidos = 0

for post in posts:
    props = post["properties"]
    status = get_prop_text(props.get("Status"))
    status_yt = get_prop_text(props.get("Status - YouTube"))
    titulo = props["Name"]["title"][0]["plain_text"] if props["Name"]["title"] else ""

    if status in STATUS_IGNORADOS or status_yt in STATUS_YOUTUBE_IGNORADOS:
        if "⚠️" in titulo:
            novo_titulo = titulo.replace("⚠️ ", "").split(" (Conflito:")[0]
            notion.pages.update(
                **{
                    "page_id": post["id"],
                    "properties": {
                        "Name": {
                            "title": [{"text": {"content": novo_titulo}}]
                        }
                    },
                }
            )
            alertas_removidos += 1
        continue

    datas_checagem = set()
    for campo_data in ["Veiculação", "Veiculação - YouTube", "Veiculação - TikTok"]:
        data = get_date(props.get(campo_data))
        if data:
            for i in range(-MARGEM_DIAS, MARGEM_DIAS + 1):
                datas_checagem.add(data + datetime.timedelta(days=i))

    if not datas_checagem:
        continue

    responsaveis = get_responsaveis(post)
    conflitos = set()
    for ausencia in ausencias:
        prop = ausencia["properties"]
        pessoa = prop["Pessoa"]["people"]
        if not pessoa:
            continue
        pessoa_id = pessoa[0]["id"]
        if pessoa_id not in responsaveis:
            continue
        data = get_date(prop.get("Data"))
        if data and data in datas_checagem:
            conflitos.add(prop["Pessoa"]["people"][0]["name"])

    if conflitos and "⚠️" not in titulo:
        novo_titulo = f"⚠️ {titulo} (Conflito: {', '.join(sorted(conflitos))})"
        notion.pages.update(
            **{
                "page_id": post["id"],
                "properties": {
                    "Name": {
                        "title": [{"text": {"content": novo_titulo}}]
                    }
                },
            }
        )
        alertas_adicionados += 1
    elif not conflitos and "⚠️" in titulo:
        novo_titulo = titulo.replace("⚠️ ", "").split(" (Conflito:")[0]
        notion.pages.update(
            **{
                "page_id": post["id"],
                "properties": {
                    "Name": {
                        "title": [{"text": {"content": novo_titulo}}]
                    }
                },
            }
        )
        alertas_removidos += 1

print(f"• Alertas adicionados: {alertas_adicionados}")
print(f"• Alertas removidos: {alertas_removidos}")
