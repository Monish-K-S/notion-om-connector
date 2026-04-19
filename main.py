import sys
import os
import yaml
import logging
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.type.entityLineage import EntitiesEdge
from metadata.generated.schema.type.entityReference import EntityReference

sys.path.append(os.path.join(os.getcwd(), "src"))
from notion_connector.notion_source import NotionSource

logging.getLogger("metadata").setLevel(logging.WARNING)

def run():
    print("1. Loading Configuration...")
    with open("notion_config.yaml", "r") as f:
        config_dict = yaml.safe_load(f)

    server_config = OpenMetadataConnection(
        hostPort=config_dict["sink"]["hostPort"],
        authProvider="openmetadata",
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=config_dict["sink"]["jwtToken"])
    )
    metadata = OpenMetadata(server_config)
    source = NotionSource.create(config_dict, server_config)

    fqn_to_id = {}
    lineage_placeholders = []

    print("2. Starting Ingestion Loop...")
    try:
        source.prepare()
        
        for record in source._iter():
            # Check if this is our custom lineage dictionary
            if isinstance(record, dict) and record.get("type") == "lineage_placeholder":
                lineage_placeholders.append(record)
            else:
                # Process standard entities (Service, Database, Table)
                response = metadata.create_or_update(record)
                if hasattr(response, 'fullyQualifiedName') and hasattr(response, 'id'):
                    fqn = response.fullyQualifiedName.__root__
                    uuid = response.id.__root__
                    fqn_to_id[fqn] = uuid
                    print(f"   -> Shipped {response.name.__root__} (ID saved)")

        # 3. Final Pass: Construct Lineage now that we have all UUIDs
        print(f"\n🔗 Connecting Lineage Graph...")
        lineage_count = 0
        for placeholder in lineage_placeholders:
            from_fqn = placeholder["from"]
            to_fqn = placeholder["to"]
            
            from_id = fqn_to_id.get(from_fqn)
            to_id = fqn_to_id.get(to_fqn)
            
            if from_id and to_id:
                # Create the REAL AddLineageRequest here where we have the IDs
                lineage_req = AddLineageRequest(
                    edge=EntitiesEdge(
                        fromEntity=EntityReference(id=from_id, type="table"),
                        toEntity=EntityReference(id=to_id, type="table")
                    )
                )
                metadata.add_lineage(lineage_req)
                lineage_count += 1
                print(f"   -> SUCCESS: Connected {from_fqn.split('.')[-1]} -> {to_fqn.split('.')[-1]}")
            else:
                print(f"   ⚠️ SKIPPED: Could not find IDs for {from_fqn} or {to_fqn}")
                
        print(f"\n✅ DONE! Lineage edges created: {lineage_count}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")

if __name__ == "__main__":
    run()
