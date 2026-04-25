# Notion-OM Connector

### Mapping the "Dark Matter" of Enterprise Metadata

[](https://www.google.com/url?sa=E&q=https%3A%2F%2Fwww.python.org%2Fdownloads%2Frelease%2Fpython-3100%2F)

[![alt text](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.google.com/url?sa=E&q=https%3A%2F%2Fwww.python.org%2Fdownloads%2Frelease%2Fpython-3100%2F)

  
[

![alt text](https://img.shields.io/badge/OpenMetadata-1.2.4-green.svg)

](https://www.google.com/url?sa=E&q=https%3A%2F%2Fopen-metadata.org%2F)  
[

![alt text](https://img.shields.io/badge/Notion%20API-v1-black.svg)

](https://www.google.com/url?sa=E&q=https%3A%2F%2Fdevelopers.notion.com%2F)  
[

![alt text](https://img.shields.io/badge/License-MIT-yellow.svg)

](https://www.google.com/url?sa=E&q=https%3A%2F%2Fopensource.org%2Flicenses%2FMIT)

> **"Dark Matter Metadata"**: Companies store critical business logic and roadmaps in Notion. To Data Teams, this data is invisible—lacking schema, governance, and traceability. **We brought it into the light.**

The **Notion-OM Connector** is a production-grade ingestion engine that bridges the gap between Notion’s flexible workspace and OpenMetadata’s governed ecosystem.

---

## 🌟 Key Pillars

|   |   |
|---|---|
|Feature|Description|
|🔗 **Automated Lineage**|Detects Relation properties to draw multi-hop visual graphs (e.g., Clients → Projects → Tasks).|
|💾 **Stateful Ingestion**|Incremental Sync via .notion_state.json ensures only modified tables are processed.|
|🛡️ **Resilient Design**|Integrated **Exponential Backoff** to handle Notion’s strict 3 req/s rate limits.|
|🏛 **OM Hierarchy**|Strictly follows the 4-tier standard: Service ➔ Database ➔ Schema ➔ Table.|
|🖥️ **Rich CLI UI**|Color-coded terminal dashboard for real-time monitoring of ingestion status.|

---

## 🏗 System Architecture

We implemented a custom **Three-Pass Ingestion Strategy** to overcome the "UUID Paradox" inherent in modern metadata catalogs:

1. **Discovery Pass**: Scans Notion, creates the OM hierarchy, and registers Tables. Crucially, it captures the **Server-Generated UUIDs** in real-time.
    
2. **Rescue Pass**: Notion's search API often misses relation targets. We implemented a proactive rescue layer using databases.retrieve to force-fetch metadata for specific IDs.
    
3. **Lineage Pass**: Uses the loopback dictionary of captured UUIDs to stitch together AddLineageRequest objects, ensuring the visual graph connects even if tables were skipped during incremental sync.
    

---

## 🛠 Prerequisites

- **Python 3.10** (Critical for SDK Regex compatibility).
    
- **Docker Desktop** (To host the OpenMetadata stack).
    
- **Notion Integration Token** (Internal Integration type).
    

---

## 📥 Quick Start

### 1. Environment Setup

codeBash

```
# Clone the repository
git clone <your-repo-url>
cd notion-om-connector

# Create a clean Python 3.10 environment
python3.10 -m venv venv
source venv/bin/activate

# Install the pinned "Safe Stack" for macOS/Linux
pip install --upgrade pip setuptools~=66.0.0
pip install "openmetadata-ingestion==1.2.4.0" notion-client PyYAML backoff rich sqllineage "sqlparse==0.5.3" "sqlfluff==2.3.5"
```

### 2. Configuration

Create a notion_config.yaml in the root:

codeYaml

```
source:
  serviceName: "notion_connector"
  notion_api_key: "secret_xxxxxxxxxxxxxxxxxxxxxxxx"
sink:
  hostPort: "http://localhost:8585/api"
  jwtToken: "eyJraWQiOiJHYV..." # Get from OM Settings > Bots > ingestion-bot
```

### 3. Execution

codeBash

```
# Direct path execution ensures no system-alias interference
./venv/bin/python main.py
```

---

## 📸 The "Money Shot" (Visualizing Results)

1. Open **[http://localhost:8585](https://www.google.com/url?sa=E&q=http%3A%2F%2Flocalhost%3A8585)**.
    
2. Navigate to **Explore** and search for Tasks.
    
3. Click the **Lineage** tab.
    
4. **Behold**: An interactive, automated relationship graph reconstructs your business context from Notion.
    

---

## 🧪 Technical Hurdle Log

- **The Regex Regression**: Identified that Python 3.11's updated regex engine breaks Pydantic validation in the OM 1.2.4 SDK. **Solution**: Environment hard-pinning to 3.10.
    
- **The UUID Paradox**: Lineage requires IDs that don't exist until after creation. **Solution**: Developed a real-time loopback listener to capture server responses during the table ingestion pass.
    
- **Permission Shadowing**: Notion search is limited by bot invitations. **Solution**: Built an autonomous "Rescue Pass" to retrieve metadata directly via ID for hidden relation targets.
    

---

## 📂 Project Structure

codeText

```
notion-om-connector/
├── src/
│   └── notion_connector/        
│       ├── connection.py        # Notion Client Factory
│       ├── metadata.py          # Main Logic & Rescue Pass
│       └── mapper.py            # Notion-to-OM Type Translator
├── .notion_state.json           # Incremental Sync Memory
├── notion_config.yaml           # Secrets & Config
└── main.py                      # Rich CLI Runner & Lineage Resolver
```

---

## 👨‍💻 Authors

**Nishita Kumari & Monish Gowda**  
Built with ❤️ for OpenMetadata's - Back to the Metadata Hackathon, April 2026

---
