import sys
import os
import yaml
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig

# Ensure Python finds the local 'src' folder
sys.path.append(os.path.join(os.getcwd(), "src"))
from notion_connector.notion_source import NotionSource

def run():
    print("1. Loading Configuration...")
    with open("notion_config.yaml", "r") as f:
        config_dict = yaml.safe_load(f)

    # 2. Setup OpenMetadata Connection
    sink_cfg = config_dict.get("sink", {})
    host_port = sink_cfg.get("hostPort")
    jwt_token = sink_cfg.get("jwtToken")

    print(f"2. Connecting to OpenMetadata at {host_port}...")
    server_config = OpenMetadataConnection(
        hostPort=host_port,
        authProvider="openmetadata",
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=jwt_token)
    )
    metadata = OpenMetadata(server_config)

    # 3. Create the Source
    source = NotionSource.create(config_dict, server_config)

    # 4. Run Ingestion Loop
    print("3. Starting Ingestion Loop...")
    try:
        source.prepare()
        success_count = 0
        for record in source._iter():
            # Get name safely for display
            name = record.name.__root__ if hasattr(record.name, "__root__") else record.name
            print(f"   -> Shipping {name}...")
            
            # Direct API update
            metadata.create_or_update(record)
            success_count += 1
            
        print(f"\n✅ SUCCESS: Sent {success_count} entities to OpenMetadata!")
        print("Check: http://localhost:8585/explore/tables")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")

if __name__ == "__main__":
    run()