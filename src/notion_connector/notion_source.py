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
        
        # Robustly get serviceName
        source_cfg = self.config.get('source', {})
        self.service_name = source_cfg.get('serviceName')
        
        if not self.service_name:
            raise ValueError("serviceName is missing in notion_config.yaml")

        # Initialize Notion
        api_key = source_cfg.get('notion_api_key')
        self.notion = get_notion_client(api_key)

    @classmethod
    def create(cls, config_dict: dict, metadata_config):
        return cls(config_dict, metadata_config)

    def prepare(self):
        self.notion.users.me()

    def _iter(self) -> Iterable[Entity]:
        # 1. Register Service
        yield CreateDatabaseServiceRequest(
            name=self.service_name,
            serviceType=DatabaseServiceType.CustomDatabase,
            connection=DatabaseConnection(
                config=CustomDatabaseConnection(type="CustomDatabase", connectionOptions={})
            )
        )

        # 2. Register Database
        yield CreateDatabaseRequest(name="Notion_Workspace", service=self.service_name)

        # 3. Register Schema
        yield CreateDatabaseSchemaRequest(
            name="default", 
            database=f"{self.service_name}.Notion_Workspace"
        )

        # 4. Process Notion Tables
        print("🔍 Searching Notion for Databases...")
        response = self.notion.search().get("results", [])
        
        for obj in response:
            if obj.get("object") in ["database", "data_source"]:
                obj_id = obj.get("id")
                # Extract Title
                title_list = obj.get("title", [])
                name = title_list[0].get("plain_text", f"Table_{obj_id[:8]}") if title_list else f"Table_{obj_id[:8]}"
                
                # SANITIZE: Remove spaces for OM compatibility
                safe_name = name.replace(" ", "_")

                # Map Columns
                om_columns = []
                for prop_name, prop_data in obj.get("properties", {}).items():
                    om_columns.append(map_notion_property_to_column(prop_name, prop_data))

                yield CreateTableRequest(
                    name=safe_name,
                    columns=om_columns,
                    databaseSchema=f"{self.service_name}.Notion_Workspace.default"
                )

    def test_connection(self): pass
    def close(self): pass