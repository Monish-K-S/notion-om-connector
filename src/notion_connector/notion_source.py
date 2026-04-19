from typing import Iterable
from notion_client import Client
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
        self.service_name = config['source']['serviceName']
        self.notion = get_notion_client(config['source']['notion_api_key'])
        self.id_to_fqn = {} 
        self.relationships = []

    @classmethod
    def create(cls, config_dict: dict, metadata_config):
        return cls(config_dict, metadata_config)

    def prepare(self):
        self.notion.users.me()

    def _iter(self) -> Iterable[Entity]:
        # Ship basic hierarchy
        yield CreateDatabaseServiceRequest(name=self.service_name, serviceType=DatabaseServiceType.CustomDatabase, connection=DatabaseConnection(config=CustomDatabaseConnection(type="CustomDatabase", connectionOptions={})))
        yield CreateDatabaseRequest(name="Notion_Workspace", service=self.service_name)
        yield CreateDatabaseSchemaRequest(name="default", database=f"{self.service_name}.Notion_Workspace")

        response = self.notion.search().get("results", [])
        
        # PASS 1: Map found objects
        for obj in response:
            obj_id = obj.get("id").replace("-", "")
            raw_title = obj.get("title", [])
            if not raw_title and "properties" in obj:
                props = obj.get("properties", {})
                title_prop = props.get("title") or props.get("Name")
                if title_prop and title_prop.get("title"):
                    raw_title = title_prop["title"]
            name = raw_title[0].get("plain_text", f"Table_{obj_id[:8]}").replace(" ", "_") if raw_title else f"Table_{obj_id[:8]}"
            self.id_to_fqn[obj_id] = f"{self.service_name}.Notion_Workspace.default.{name}"

        # PASS 2: Ship Tables
        for obj in response:
            if obj.get("object") in ["database", "data_source"]:
                obj_id = obj.get("id").replace("-", "")
                fqn = self.id_to_fqn[obj_id]
                name = fqn.split(".")[-1]
                om_columns = []
                for prop_name, prop_data in obj.get("properties", {}).items():
                    om_columns.append(map_notion_property_to_column(prop_name, prop_data))
                    if prop_data.get("type") == "relation":
                        target_id = prop_data["relation"].get("database_id").replace("-", "")
                        self.relationships.append((fqn, target_id))
                yield CreateTableRequest(name=name, columns=om_columns, databaseSchema=f"{self.service_name}.Notion_Workspace.default")

        # PASS 3: Rescue hidden relations
        for _, target_id in self.relationships:
            if target_id not in self.id_to_fqn:
                try:
                    remote_db = self.notion.databases.retrieve(database_id=target_id)
                    res_title = remote_db.get("title", [])
                    res_name = res_title[0].get("plain_text", "Rescued").replace(" ", "_") if res_title else "Rescued"
                    self.id_to_fqn[target_id] = f"{self.service_name}.Notion_Workspace.default.{res_name}"
                except: pass

        # PASS 4: YIELD PLACEHOLDERS (Main.py will turn these into real objects)
        for source_fqn, target_id in self.relationships:
            target_fqn = self.id_to_fqn.get(target_id)
            if target_fqn:
                # Yielding a dictionary skips Pydantic validation until main.py
                yield {
                    "type": "lineage_placeholder",
                    "from": target_fqn,
                    "to": source_fqn
                }

    def test_connection(self): pass
    def close(self): pass
