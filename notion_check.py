import os
import requests

NOTION_API_KEY = os.environ["NOTION_API_KEY"]
HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

def fetch_database(database_id):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    response = requests.post(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()["results"]

def main():
    db_reunioes = os.environ["DATABASE_ID_REUNIOES"]
    db_ausencias = os.environ["DATABASE_ID_AUSENCIAS"]

    reunioes = fetch_database(db_reunioes)
    ausencias = fetch_database(db_ausencias)

    print(f"Reuniões encontradas: {len(reunioes)}")
    print(f"Ausências encontradas: {len(ausencias)}")

if __name__ == "__main__":
    main()
