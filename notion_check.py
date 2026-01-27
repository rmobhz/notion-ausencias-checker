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

def fetch_database(database_id, page_size=100):
    """Busca todos os registros de um banco de dados com paginação"""
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    all_results = []
    start_cursor = None

    while True:
        payload = {"page_size": page_size}
        if start_cursor:
            payload["start_cursor"] = start_cursor

        response = requests.post(url, headers=HEADERS, json=payload)
        response.raise_for_status()
        data = response.json()

        all_results.extend(data["results"])

        if not data.get("has_more"):
            break

        start_cursor = data.get("next_cursor")

    return all_results

def parse_date(date_obj):
    if not date_obj:
        return None, None
    start = datetime.fromisoformat(date_obj["start"][:10])
    end = datetime.fromisoformat(date_obj["end"][:10]) if date_obj.get("end") else start
    return start, end

def date_ranges_overlap(start1, end1, start2, end2):
    return start1 <= end2 and end1 >= start2

def limpar_titulo(titulo):
    """
    Remove:
    - sufixo (Ausentes/Ausências: ...)
    - qualquer ⚠️ no prefixo (antes ou depois do 🔁)
    Preserva:
    - 🔁 no começo (se existir)
    Retorna: (prefixo_recorrencia, titulo_limpo)
    """
    if not titulo:
        return "", ""

    # remove sufixo de ausências
    t = re.sub(r"\s*\((Ausentes|Ausências):.*?\)\s*$", "", titulo).strip()

    # detecta se tem 🔁 no começo (mesmo que venha após ⚠️)
    has_rec = bool(re.match(r"^\s*(?:⚠️\s*)*🔁", t))

    # remove prefixos: ⚠️ repetidos, opcional 🔁, e ⚠️ repetidos novamente
    core = re.sub(r"^\s*(?:⚠️\s*)*(?:🔁\s*)?(?:⚠️\s*)*", "", t).strip()

    prefix = "🔁 " if has_rec else ""
    return prefix, core

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

    print("⏳ Buscando reuniões...")
    reunioes = fetch_database(DATABASE_ID_REUNIOES)
    print(f"✅ Encontradas {len(reunioes)} reuniões")

    print("⏳ Buscando ausências...")
    ausencias = fetch_database(DATABASE_ID_AUSENCIAS)
    print(f"✅ Encontradas {len(ausencias)} ausências")

    for reuniao in reunioes:
        props = reuniao["properties"]
        participantes = props["Participantes"]["people"]
        data_reuniao = props["Data"].get("date")
        reuniao_id = reuniao["id"]

        titulo_original = (
            props["Evento"]["title"][0]["text"]["content"]
            if props["Evento"]["title"]
            else "Sem título"
        )

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
            prefix, core = limpar_titulo(titulo_original)
            novo_titulo = f"⚠️ {prefix}{core} (Ausências: {', '.join(nomes_em_conflito)})"

            if titulo_original != novo_titulo:
                patch_database(reuniao_id, "Evento", novo_titulo)
                print(f"⚠️ Conflito detectado: {novo_titulo}")
        else:
            if (
                titulo_original.startswith("⚠️")
                or "(Ausências:" in titulo_original
                or "(Ausentes:" in titulo_original
            ):
                prefix, core = limpar_titulo(titulo_original)
                base_titulo = f"{prefix}{core}"
                patch_database(reuniao_id, "Evento", base_titulo)
                print(f"✅ Conflito resolvido: {base_titulo}")

if __name__ == "__main__":
    main()
