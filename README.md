# 🚀 Notion-OM Connector

### Mapping the "Dark Matter" of Enterprise Metadata

Most companies store critical business logic, project roadmaps, and client data in **Notion**. However, to a Data Engineering team, this data is "Dark Matter"—it is invisible to the official data catalog, lacks schema enforcement, and has no traceable lineage.

The **Notion-OM Connector** is a production-grade ingestion engine that bridges this gap. Built using the official **OpenMetadata (OM) Source Protocol**, it extracts Notion databases, maps complex types, and reconstructs visual lineage graphs directly within the OpenMetadata ecosystem.

---

## 🌟 Key Features

- **Automated Lineage:** Detects Notion Relation properties and automatically draws transitive (multi-hop) visual graphs (e.g., Clients → Projects → Tasks).
    
- **Stateful Ingestion:** Implements **Incremental Sync** via a local state manager (.notion_state.json) to only process databases that have changed since the last run.
    
- **Resilient Design:** Integrated with **Exponential Backoff** to gracefully handle Notion’s strict 3 requests/second rate limits.
    
- **Production Hierarchy:** Respects the OpenMetadata 4-tier hierarchy: Service ➔ Database ➔ Schema ➔ Table.
    
- **Rich CLI Interface:** Provides a beautiful, color-coded terminal dashboard for real-time ingestion monitoring.
    

---

## 🏗 System Architecture

The connector follows a **Two-Pass Ingestion Strategy** to solve the "UUID Paradox" in OpenMetadata:

1. **Discovery Pass:** Scans Notion, creates Service/Database/Schema entities, and registers Tables. It captures the **Server-Generated UUIDs** for every table.
    
2. **Rescue Pass:** Fetches related databases by ID that may have been hidden from initial search results due to Notion’s unique permission scoping.
    
3. **Lineage Pass:** Uses the captured UUIDs to stitch together the AddLineageRequest objects, ensuring perfect graph rendering even if tables were skipped during incremental sync.
    

---

## 🛠 Prerequisites

- **Python 3.10:** (Required to avoid Regex compatibility issues with the OM SDK on version 3.11+).
    
- **Docker Desktop:** For running the OpenMetadata stack.
    
- **Notion Integration Token:** Created via [notion.so/my-integrations](https://www.google.com/url?sa=E&q=https%3A%2F%2Fwww.notion.so%2Fmy-integrations).
    

---

## 📥 Installation

1. **Clone the repository:**
    
    codeBash
    
    ```
    git clone <your-repo-url>
    cd notion-om-connector
    ```
    
2. **Create a Python 3.10 Virtual Environment:**
    
    codeBash
    
    ```
    python3.10 -m venv venv
    source venv/bin/activate
    ```
    
3. **Install the "Safe Stack" (Pinned Dependencies):**
    
    codeBash
    
    ```
    pip install --upgrade pip setuptools~=66.0.0
    pip install "openmetadata-ingestion==1.2.4.0" notion-client PyYAML backoff rich sqllineage "sqlparse==0.5.3" "sqlfluff==2.3.5"
    ```
    

---

## ⚙️ Configuration

Create a notion_config.yaml in the root directory.

> **How to get the jwtToken:** Go to OpenMetadata UI ➔ Settings ➔ Bots ➔ ingestion-bot ➔ Auth Config ➔ Generate Token.

codeYaml

```
source:
  serviceName: "notion_connector"
  notion_api_key: "secret_xxxxxxxxxxxxxxxxxxxxxxxx"
sink:
  hostPort: "http://localhost:8585/api"
  jwtToken: "eyJraWQiOiJHYV..." # Your OM Bot Token
```

---

## 🚀 Running the Connector

To ensure the environment stays stable on macOS and bypasses any system-level Python aliases, run using the direct path:

codeBash

```
./venv/bin/python main.py
```

### **The "Money Shot" (Visualizing Results)**

1. Open **[http://localhost:8585](https://www.google.com/url?sa=E&q=http%3A%2F%2Flocalhost%3A8585)**.
    
2. Go to **Explore** and search for your Notion tables (e.g., Tasks).
    
3. Click the **Lineage** tab.
    
4. **Behold:** Your interactive, multi-hop Notion relationship graph.
    

---

## 📂 Project Structure

codeText

```
notion-om-connector/
├── src/
│   └── notion_connector/        
│       ├── __init__.py          
│       ├── connection.py        # Notion API Client Factory
│       ├── notion_source.py     # Main Ingestion Logic & Rescue Pass
│       └── mapper.py            # Notion-to-OM Property Translator
├── .notion_state.json           # Incremental Sync Memory
├── notion_config.yaml           # Secrets & Server Config
└── main.py                      # Rich CLI Runner & Lineage Stitcher
```

---

## 🧪 Technical Hurdle Log (Hackathon Notes)

- **The Regex Bug:** Discovered that Python 3.11's new regex engine breaks Pydantic validation in OM 1.2.4. **Solution:** Hard-pinned environment to Python 3.10.
    
- **The UUID Paradox:** Lineage in OM requires UUIDs that don't exist until after the table is created. **Solution:** Built a loopback dictionary that captures server responses in real-time to build the graph in a post-process pass.
    
- **Permission Shadowing:** Notion's search API often misses "Relation" targets. **Solution:** Implemented a **"Rescue Pass"** that uses databases.retrieve to force-fetch metadata for specific IDs found in relation properties.
    

---

## 👨‍💻 Author

**Nishita Kumari** & **Monish Gowda**  
Built for OpenMetadata's - Back to the Metadata Hackathon,
April 2026