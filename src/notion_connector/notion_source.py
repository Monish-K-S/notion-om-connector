from typing import Iterable
from notion_client import Client
from metadata.ingestion.api.steps import Source
from metadata.ingestion.api.models import Entity

# --- NEW IMPORTS FOR SERVICE REGISTRATION ---
from metadata.generated.schema.api.services.createDatabaseService import CreateDatabaseServiceRequest
from metadata.generated.schema.entity.services.databaseService import DatabaseServiceType, DatabaseConnection
from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import CustomDatabaseConnection
# --------------------------------------------

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

    @classmethod
    def create(cls, config_dict: dict, metadata_config):
        return cls(config_dict, metadata_config)

    def prepare(self):
        self.notion.users.me()

    def _iter(self) -> Iterable[Entity]:
        # 1. CREATE THE SERVICE (The Cabinet)
        # This tells OM that 'notion_connector' exists as a source
        yield CreateDatabaseServiceRequest(
            name=self.service_name,
            serviceType=DatabaseServiceType.CustomDatabase,
            connection=DatabaseConnection(
                config=CustomDatabaseConnection(
                    type="CustomDatabase",
                    connectionOptions={} # We handle auth in our Python code
                )
            )
        )

        # 2. CREATE THE DATABASE (The Drawer)
        yield CreateDatabaseRequest(
            name="Notion_Workspace",
            service=self.service_name
        )

        # 3. CREATE THE SCHEMA (The Folder)
        yield CreateDatabaseSchemaRequest(
            name="default",
            database=f"{self.service_name}.Notion_Workspace"
        )

        # 4. SCAN AND YIELD TABLES (The Papers)
        print("🔍 Searching for Notion Databases...")
        response = self.notion.search().get("results", [])
        
        for obj in response:
            if obj.get("object") in ["database", "data_source"]:
                obj_id = obj.get("id")
                raw_title = obj.get("title", [])
                name = raw_title[0].get("plain_text", f"Table_{obj_id[:8]}") if raw_title else f"Table_{obj_id[:8]}"
                
                # Sanitize name: OM doesn't like spaces in names
                safe_name = name.replace(" ", "_")

                om_columns = []
                for prop_name, prop_data in obj.get("properties", {}).items():
                    om_columns.append(map_notion_property_to_column(prop_name, prop_data))

                print(f"   -> Shipping Table: {safe_name}")
                yield CreateTableRequest(
                    name=safe_name,
                    columns=om_columns,
                    databaseSchema=f"{self.service_name}.Notion_Workspace.default"
                )

    def test_connection(self): pass
    def close(self): pass
