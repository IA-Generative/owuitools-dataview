# dataview

Service d'interrogation de fichiers tabulaires (CSV, XLS, XLSX, JSON, Parquet, ODS) en langage naturel.

## Architecture

- **API REST** (port 8093) : `/healthz`, `/preview`, `/schema`, `/query`
- **Serveur MCP** (port 8088) : 3 tools via FastMCP (Streamable HTTP)
- **Tool OWUI** : code Python injecté en DB pour OpenWebUI

## Quickstart

```bash
cp .env.example .env
# Remplir LLM_API_URL et LLM_API_KEY

docker compose up -d
curl http://localhost:8093/healthz
```

## Utilisation

```bash
# Aperçu d'un fichier
curl -X POST http://localhost:8093/preview \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.data.gouv.fr/fr/datasets/r/630e7917-02db-4838-8856-09235719551c"}'

# Interrogation en langage naturel
curl -X POST http://localhost:8093/query \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com/communes.csv", "question": "Les 10 communes les plus peuplées du 93"}'
```

## Tests

```bash
pip install pytest pytest-asyncio httpx
pytest tests/ -v
```

## Sécurité

- Pas d'`exec()`/`eval()` — opérations pandas whitelistées uniquement
- Timeout sur téléchargement (30s) et requête (10s)
- Taille max fichiers : 100 Mo
- Container non-root (UID 1000)
