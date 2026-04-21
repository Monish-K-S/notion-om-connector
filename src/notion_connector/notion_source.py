import json
import os
import backoff
from datetime import datetime
from typing import Iterable
from notion_client import Client
from notion_client.errors import APIResponseError
from metadata.ingestion.api.steps import Source
from metadata.ingestion.api.models import Entity
from metadata.generated.schema.api.services.createDatabaseService import CreateDatabaseServiceRequest
from metadata.generated.schema.entity.services.databaseService import DatabaseServiceType, DatabaseConnection
from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import CustomDatabaseConnection
from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createDatabaseSchema import CreateDatabaseSchemaRequest
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from notion_connector.mapper import map_notion_property_to_column
from notion_connector.connection import get_notion_client

class NotionSource(Source):
    def __init__(self, config: dict, metadata_config):
        super().__init__()
        self.config = config
        source_cfg = config.get('source', {})
        self.service_name = source_cfg.get('serviceName')
        self.notion = get_notion_client(source_cfg.get('notion_api_key'))
        self.state_file = ".notion_state.json"
        self.last_run_time = self._load_state()
        self.id_to_fqn = {} 
        self.relationships = []

    def _load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r") as f:
                    return json.load(f).get("last_run")
            except: return None
        return None

    def save_state(self):
        with open(self.state_file, "w") as f:
            json.dump({"last_run": datetime.utcnow().isoformat() + "Z"}, f)

    @backoff.on_exception(backoff.expo, APIResponseError, max_tries=5, giveup=lambda e: e.status != 429)
    def fetch_from_notion(self):
        return self.notion.search().get("results", [])

    @classmethod
    def create(cls, config_dict: dict, metadata_config):
        return cls(config_dict, metadata_config)

    def prepare(self):
        self.notion.users.me()

    def _iter(self) -> Iterable[Entity]:
        # Ship hierarchy
        yield CreateDatabaseServiceRequest(name=self.service_name, serviceType=DatabaseServiceType.CustomDatabase, connection=DatabaseConnection(config=CustomDatabaseConnection(type="CustomDatabase", connectionOptions={})) )
        yield CreateDatabaseRequest(name="Notion_Workspace", service=self.service_name)
        yield CreateDatabaseSchemaRequest(name="default", database=f"{self.service_name}.Notion_Workspace")

        response = self.fetch_from_notion()
        
        # PASS 1: Initial Name Mapping
        for obj in response:
            obj_id = obj.get("id").replace("-", "")
            raw_title = obj.get("title", [])
            if not raw_title and "properties" in obj:
                props = obj.get("properties", {})
                title_prop = props.get("title") or props.get("Name")
                if title_prop and title_prop.get("title"):
                    raw_title = title_prop["title"]
            clean_name = raw_title[0].get("plain_text", f"Table_{obj_id[:8]}").replace(" ", "_") if raw_title else f"Table_{obj_id[:8]}"
            self.id_to_fqn[obj_id] = f"{self.service_name}.Notion_Workspace.default.{clean_name}"

        # PASS 2: Detect Relations and Ship Tables
        for obj in response:
            if obj.get("object") in ["database", "data_source"]:
                obj_id = obj.get("id").replace("-", "")
                fqn = self.id_to_fqn[obj_id]
                
                # Record relations for ALL tables (needed for lineage)
                props = obj.get("properties", {})
                for p_name, p_data in props.items():
                    if p_data.get("type") == "relation":
                        target_id = p_data["relation"].get("database_id").replace("-", "")
                        self.relationships.append({"from": fqn, "to_id": target_id})

                # INCREMENTAL CHECK: Ship only if new/changed
                last_edited = obj.get("last_edited_time")
                if self.last_run_time and last_edited <= self.last_run_time:
                    continue 

                om_columns = [map_notion_property_to_column(n, d) for n, d in props.items()]
                yield CreateTableRequest(name=fqn.split(".")[-1], columns=om_columns, databaseSchema=f"{self.service_name}.Notion_Workspace.default")

        # PASS 3: THE RESCUE PASS (Fetch hidden IDs directly)
        for rel in self.relationships:
            if rel["to_id"] not in self.id_to_fqn:
                try:
                    remote_db = self.notion.databases.retrieve(database_id=rel["to_id"])
                    res_title = remote_db.get("title", [])
                    res_name = res_title[0].get("plain_text", "Rescued").replace(" ", "_") if res_title else "Rescued"
                    self.id_to_fqn[rel["to_id"]] = f"{self.service_name}.Notion_Workspace.default.{res_name}"
                except: pass

        # PASS 4: Yield Placeholders
        for rel in self.relationships:
            target_fqn = self.id_to_fqn.get(rel["to_id"])
            if target_fqn:
                yield {"type": "lineage_placeholder", "from": target_fqn, "to": rel["from"]}

    def test_connection(self): pass
    def close(self): pass