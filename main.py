import sys
import os
import yaml
from rich.console import Console
from rich.table import Table as RichTable
from rich.panel import Panel
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.type.entityLineage import EntitiesEdge
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.entity.data.table import Table

sys.path.append(os.path.join(os.getcwd(), "src"))
from notion_connector.notion_source import NotionSource

console = Console()

def run():
    console.print(Panel.fit("[bold blue]Notion → OpenMetadata[/bold blue]\n[white]Phase 5: Production Polish[/white]"))
    with open("notion_config.yaml", "r") as f:
        config_dict = yaml.safe_load(f)

    server_config = OpenMetadataConnection(hostPort=config_dict["sink"]["hostPort"], authProvider="openmetadata", securityConfig=OpenMetadataJWTClientConfig(jwtToken=config_dict["sink"]["jwtToken"]))
    metadata = OpenMetadata(server_config)
    source = NotionSource.create(config_dict, server_config)

    fqn_to_id = {}
    placeholders = []
    summary = RichTable(title="Ingestion Progress", show_header=True, header_style="bold cyan")
    summary.add_column("Entity Type", style="dim"); summary.add_column("Entity Name"); summary.add_column("Status", justify="right")

    try:
        source.prepare()
        console.print("[yellow]🔍 Processing Notion Metadata...[/yellow]")
        for record in source._iter():
            if isinstance(record, dict) and record.get("type") == "lineage_placeholder":
                placeholders.append(record)
            else:
                resp = metadata.create_or_update(record)
                if hasattr(resp, 'fullyQualifiedName') and hasattr(resp, 'id'):
                    fqn_to_id[resp.fullyQualifiedName.__root__] = resp.id.__root__
                    summary.add_row(record.__class__.__name__.replace("Create",""), resp.name.__root__, "[green]✓[/green]")

        console.print("[yellow]🔗 Connecting Lineage...[/yellow]")
        lineage_count = 0
        for p in placeholders:
            # 1. Get From ID (Current run or Server)
            f_id = fqn_to_id.get(p["from"])
            if not f_id:
                obj = metadata.get_by_name(entity=Table, fqn=p["from"])
                if obj: f_id = obj.id.__root__
            
            # 2. Get To ID (Current run or Server)
            t_id = fqn_to_id.get(p["to"])
            if not t_id:
                obj = metadata.get_by_name(entity=Table, fqn=p["to"])
                if obj: t_id = obj.id.__root__

            if f_id and t_id:
                metadata.add_lineage(AddLineageRequest(edge=EntitiesEdge(fromEntity=EntityReference(id=f_id, type="table"), toEntity=EntityReference(id=t_id, type="table"))))
                lineage_count += 1
                console.print(f"   [green]✓[/green] Lineage: {p['from'].split('.')[-1]} → {p['to'].split('.')[-1]}")
        
        source.save_state()
        console.print(summary)
        console.print(f"\n[bold green]✅ SUCCESS![/bold green] Total Lineage Edges: [bold white]{lineage_count}[/bold white]")
    except Exception as e:
        console.print(f"[bold red]❌ ERROR:[/bold red] {e}")

if __name__ == "__main__":
    run()