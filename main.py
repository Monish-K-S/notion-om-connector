import sys
import os
import yaml
import warnings
from datetime import datetime

# Rule: Hide system warnings for a clean production demo
warnings.filterwarnings("ignore")

from rich.console import Console, Group
from rich.live import Live
from rich.text import Text
from rich.spinner import Spinner

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

class IngestionManager:
    def __init__(self):
        self.start_time = datetime.now()
        self.entries = []
        self.step = "auth"
        self.counts = {"synced": 0, "lineage": 0}

    def update_log(self, category: str, message: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        # Exact alignment: Timestamp (grey) Category (Bold Blue, 8 spaces) Message (White)
        self.entries.append(f"[grey62]{timestamp}[/] [bold blue]{category:<8}[/] {message}")
        if len(self.entries) > 12:
            self.entries.pop(0)

    def __rich__(self):
        # 1. Header (notion-om-connector v1.2.4)
        header = Text.assemble(
            ("\nnotion-om-connector ", "bold white"),
            (f"v1.2.4", "grey37 italic"),
            ("\n", "")
        )

        # 2. Status Row with horizontal spacing
        def get_status(stage_name):
            order = ["auth", "sync", "lineage", "done"]
            if self.step == stage_name:
                return Spinner("dots", style="cyan"), "bold cyan"
            if order.index(self.step) > order.index(stage_name):
                return "✔", "bold green"
            return "○", "grey37"

        a_i, a_s = get_status("auth")
        s_i, s_s = get_status("sync")
        l_i, l_s = get_status("lineage")

        stages = Text.assemble(
            (f"  {a_i} ", a_s), ("Authentication    ", a_s),
            (f"{s_i} ", s_s), ("Metadata Sync    ", s_s),
            (f"{l_i} ", l_s), ("Lineage Construction", l_s),
            ("\n", "")
        )

        # 3. Log Feed
        logs = Group(*[Text.from_markup(f"  {e}") for e in self.entries])

        # 4. RESULTS bar (Black on White badge)
        duration = (datetime.now() - self.start_time).seconds
        footer = Text.assemble(
            ("\n RESULTS ", "bold reverse"),
            (f"   {self.counts['synced']} Assets Reconciled   ", "white"),
            (f"{self.counts['lineage']} Relations Mapped   ", "white"),
            (f"({duration}s)", "grey37")
        )

        return Group(header, stages, logs, footer)

def run():
    with open("notion_config.yaml", "r") as f:
        config_dict = yaml.safe_load(f)

    ui = IngestionManager()
    server_config = OpenMetadataConnection(
        hostPort=config_dict["sink"]["hostPort"],
        authProvider="openmetadata",
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=config_dict["sink"]["jwtToken"])
    )
    metadata = OpenMetadata(server_config)
    source = NotionSource.create(config_dict, server_config)

    fqn_to_id, placeholders = {}, []

    with Live(ui, console=console, refresh_per_second=10):
        try:
            ui.step = "auth"
            source.prepare()
            ui.update_log("AUTH", "Notion Connection Verified")

            ui.step = "sync"
            for record in source._iter():
                if isinstance(record, dict) and record.get("type") == "lineage_placeholder":
                    placeholders.append(record)
                else:
                    resp = metadata.create_or_update(record)
                    if hasattr(resp, 'fullyQualifiedName') and hasattr(resp, 'id'):
                        fqn = resp.fullyQualifiedName.__root__
                        fqn_to_id[fqn] = resp.id.__root__
                        
                        # Concise Sync Log
                        ui.counts["synced"] += 1
                        ui.update_log("SYNC", resp.name.__root__)

            ui.step = "lineage"
            for p in placeholders:
                f_id = fqn_to_id.get(p["from"]) or (metadata.get_by_name(entity=Table, fqn=p["from"]).id.__root__ if metadata.get_by_name(entity=Table, fqn=p["from"]) else None)
                t_id = fqn_to_id.get(p["to"]) or (metadata.get_by_name(entity=Table, fqn=p["to"]).id.__root__ if metadata.get_by_name(entity=Table, fqn=p["to"]) else None)

                if f_id and t_id:
                    metadata.add_lineage(AddLineageRequest(edge=EntitiesEdge(fromEntity=EntityReference(id=f_id, type="table"), toEntity=EntityReference(id=t_id, type="table"))))
                    ui.counts["lineage"] += 1
                    ui.update_log("GRAPH", f"{p['from'].split('.')[-1]} → {p['to'].split('.')[-1]}")

            source.save_state()
            ui.step = "done"
        except Exception as e:
            ui.update_log("ERROR", str(e))
            ui.step = "done"

if __name__ == "__main__":
    run()