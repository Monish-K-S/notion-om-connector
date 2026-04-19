from notion_client import Client

def get_notion_client(api_key: str) -> Client:
    return Client(auth=api_key)


