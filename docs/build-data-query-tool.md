# Prompt : Créer le repo data-query-owui

Tu es un coding assistant. Tu vas créer un nouveau repo `data-query-owui` dans `~/Documents/GitHub/`.

## Contexte

On a un écosystème d'agent conversationnel basé sur OpenWebUI v0.8.12 déployé sur Scaleway K8s (namespace `miraiku`). L'exécution de code est **désactivée** dans OWUI pour des raisons de sécurité. On a besoin d'un service dédié qui permet d'interroger des fichiers tabulaires (CSV, XLS, XLSX, JSON, Parquet) via des questions en langage naturel.

Ce service sera appelé :
- Comme **tool OWUI** (code Python injecté en DB) en court terme
- Comme **serveur MCP** (Streamable HTTP) en moyen terme
- Le tout dans le même repo

Le service sera utilisé conjointement avec le MCP data.gouv.fr qui retourne des URLs de fichiers à analyser.

## Architecture

```
data-query-owui/
├── app/
│   ├── main.py                  # FastAPI app
│   ├── api.py                   # Routes REST (/query, /preview, /schema, /healthz)
│   ├── config.py                # Settings (pydantic-settings)
│   ├── models.py                # Pydantic models (request/response)
│   ├── query_engine.py          # Moteur de requête (pandas + traduction NL → pandas)
│   ├── file_loader.py           # Téléchargement + parsing (CSV, XLS, JSON, Parquet)
│   ├── cache.py                 # Cache fichiers téléchargés (TTL, taille max)
│   ├── sandbox.py               # Exécution sécurisée des opérations pandas
│   ├── mcp_server.py            # Serveur MCP (FastMCP)
│   └── mcp_app.py               # ASGI app MCP standalone (port 8088)
├── openwebui/
│   └── data_query_tool.py       # Tool OWUI à injecter en DB
├── tests/
│   ├── test_api.py
│   ├── test_query_engine.py
│   ├── test_file_loader.py
│   └── test_sandbox.py
├── owui-plugin.yaml
├── docker-compose.yaml
├── Dockerfile
├── entrypoint.py
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## Spécifications fonctionnelles

### API REST

```
GET  /healthz                    → status + formats supportés
POST /preview                    → aperçu du fichier (colonnes, types, 5 premières lignes)
POST /schema                     → schéma détaillé (colonnes, types, stats, valeurs uniques)
POST /query                      → interrogation en langage naturel
```

### Endpoint /preview

```json
// Request
{
  "url": "https://example.com/data.xlsx",
  "sheet": null          // optionnel, pour les fichiers Excel multi-feuilles
}

// Response
{
  "filename": "data.xlsx",
  "format": "xlsx",
  "rows": 35000,
  "columns": ["code_commune", "nom_commune", "population", "departement"],
  "dtypes": {"code_commune": "object", "nom_commune": "object", "population": "int64", "departement": "object"},
  "preview": [
    {"code_commune": "01001", "nom_commune": "L'Abergement-Clémenciat", "population": 789, "departement": "01"},
    ...
  ],
  "sheets": ["Feuil1", "Feuil2"]   // si Excel multi-feuilles
}
```

### Endpoint /schema

```json
// Request
{
  "url": "https://example.com/data.xlsx"
}

// Response
{
  "columns": [
    {
      "name": "departement",
      "dtype": "object",
      "unique_count": 101,
      "sample_values": ["01", "02", "75", "93"],
      "null_count": 0
    },
    {
      "name": "population",
      "dtype": "int64",
      "min": 1,
      "max": 2161000,
      "mean": 1890,
      "null_count": 5
    }
  ],
  "row_count": 35000
}
```

### Endpoint /query

```json
// Request
{
  "url": "https://example.com/data.xlsx",
  "question": "Quelles sont les 10 communes les plus peuplées du département 93 ?",
  "max_rows": 50
}

// Response
{
  "question": "...",
  "operation": "df[df['departement'] == '93'].nlargest(10, 'population')[['nom_commune', 'population']]",
  "result": [
    {"nom_commune": "Saint-Denis", "population": 113024},
    {"nom_commune": "Montreuil", "population": 111260},
    ...
  ],
  "row_count": 10,
  "truncated": false
}
```

## Spécifications techniques

### file_loader.py

- Supporte : CSV, XLS, XLSX, JSON, Parquet, ODS
- Détection automatique du format via l'extension URL ou le Content-Type
- Téléchargement via httpx avec timeout (30s) et taille max (100 Mo)
- Cache local des fichiers téléchargés (TTL 1h, max 500 Mo)
- Pour les Excel multi-feuilles : paramètre `sheet` optionnel, sinon première feuille

**IMPORTANT — Gestion des URLs cassées :**

Les URLs remontées par le MCP data.gouv.fr ne fonctionnent pas toujours :
- Certaines URLs sont des redirections mortes (302 → 404)
- Certaines pointent vers des serveurs DCAT tiers qui sont down
- Certaines ont changé depuis l'indexation du dataset

Le file_loader doit implémenter une **stratégie de fallback** :

1. Tenter le téléchargement de l'URL donnée
2. Si échec (404, 403, timeout, SSL error, redirect loop) :
   a. Extraire le `resource_id` de l'URL ou le demander en paramètre
   b. Appeler l'API data.gouv.fr pour récupérer l'URL à jour :
      `GET https://www.data.gouv.fr/api/1/datasets/{dataset_id}/resources/{resource_id}/`
   c. Tenter le téléchargement avec la nouvelle URL (`latest` field)
   d. En dernier recours, utiliser le proxy data.gouv.fr :
      `GET https://www.data.gouv.fr/fr/datasets/r/{resource_id}`
3. Si un dataset a **plusieurs resources** et qu'une URL échoue, tenter automatiquement
      les autres resources du même dataset (même format de préférence) via :
      `GET https://www.data.gouv.fr/api/1/datasets/{dataset_id}/`
      → itérer sur `resources[]` et tenter chaque `url` puis `latest`
4. Retourner un message d'erreur clair si tout échoue :
   ```json
   {
     "error": "file_unavailable",
     "message": "Le fichier n'est plus accessible. URL testées : [...]. Essayez avec un autre resource du même dataset.",
     "tried_urls": ["url1 → 404", "url2 → timeout", "url3 → 403"],
     "suggestion": "Utilisez data_schema avec le dataset_id pour voir les autres fichiers disponibles."
   }
   ```

**Robustesse du téléchargement :**
- Suivre les redirections (max 5 hops)
- Accepter les certificats auto-signés (option configurable, désactivé par défaut)
- User-Agent réaliste (certains serveurs bloquent les bots)
- Retry 1 fois avec backoff de 2s sur les erreurs 5xx
- Détection de format par magic bytes si l'extension est absente ou trompeuse
  (ex: une URL sans extension qui retourne du CSV)

### query_engine.py

**Approche : traduction NL → opérations pandas prédéfinies (PAS d'exec de code arbitraire)**

Le moteur ne fait PAS `exec()` ou `eval()` de code Python. Il fonctionne par **pattern matching** sur des opérations pandas sûres :

```python
SAFE_OPERATIONS = {
    "filter": lambda df, col, op, val: df[op_map[op](df[col], val)],
    "sort": lambda df, col, asc: df.sort_values(col, ascending=asc),
    "top_n": lambda df, col, n: df.nlargest(n, col),
    "bottom_n": lambda df, col, n: df.nsmallest(n, col),
    "group_count": lambda df, col: df.groupby(col).size().reset_index(name="count"),
    "group_sum": lambda df, group_col, sum_col: df.groupby(group_col)[sum_col].sum().reset_index(),
    "group_mean": lambda df, group_col, mean_col: df.groupby(group_col)[mean_col].mean().reset_index(),
    "select_columns": lambda df, cols: df[cols],
    "unique_values": lambda df, col: df[col].unique().tolist(),
    "count": lambda df: len(df),
    "describe": lambda df, col: df[col].describe().to_dict(),
    "search": lambda df, col, text: df[df[col].str.contains(text, case=False, na=False)],
}
```

Le LLM (Scaleway API) traduit la question en un **plan d'opérations JSON** :

```json
{
  "steps": [
    {"op": "filter", "col": "departement", "operator": "==", "value": "93"},
    {"op": "top_n", "col": "population", "n": 10},
    {"op": "select_columns", "cols": ["nom_commune", "population"]}
  ]
}
```

Le moteur exécute ce plan séquentiellement sur le DataFrame. Pas d'injection de code possible.

**Prompt système pour la traduction NL → plan :**

```
Tu es un assistant qui traduit des questions en langage naturel en un plan d'opérations sur un DataFrame pandas.

Voici le schéma du DataFrame :
{schema}

Opérations disponibles :
- filter: filtre les lignes (col, operator [==, !=, >, <, >=, <=, contains, startswith], value)
- sort: trie (col, ascending: true/false)
- top_n: les N plus grandes valeurs (col, n)
- bottom_n: les N plus petites valeurs (col, n)
- group_count: compte par groupe (col)
- group_sum: somme par groupe (group_col, sum_col)
- group_mean: moyenne par groupe (group_col, mean_col)
- select_columns: sélectionne des colonnes (cols: [])
- unique_values: valeurs uniques (col)
- count: nombre total de lignes
- describe: statistiques descriptives (col)
- search: recherche textuelle (col, text)

Retourne UNIQUEMENT un JSON valide avec le champ "steps".
```

### sandbox.py

- Whitelist stricte des opérations (pas d'exec/eval)
- Timeout sur chaque opération (5s)
- Limite de mémoire (max 200 Mo par DataFrame)
- Limite de lignes en sortie (max_rows, défaut 100)
- Log de chaque opération exécutée

### cache.py

- Cache fichier sur disque avec TTL configurable
- Clé = hash SHA256 de l'URL
- Nettoyage automatique (taille max, fichiers expirés)
- Headers `If-Modified-Since` pour les rechecks

### config.py

```python
class Settings(BaseSettings):
    # LLM pour la traduction NL → plan
    LLM_API_URL: str = "https://api.scaleway.ai/.../v1"
    LLM_API_KEY: str = ""
    LLM_MODEL: str = "mistral-small-3.2-24b-instruct-2506"

    # Limites
    MAX_FILE_SIZE_MB: int = 100
    MAX_ROWS_OUTPUT: int = 100
    CACHE_TTL_SECONDS: int = 3600
    CACHE_MAX_SIZE_MB: int = 500
    QUERY_TIMEOUT_SECONDS: int = 10
    DOWNLOAD_TIMEOUT_SECONDS: int = 30

    # Service
    PORT: int = 8093
    MCP_PORT: int = 8088
```

### mcp_server.py

Expose 3 tools MCP via FastMCP :

```python
@mcp.tool()
async def data_preview(url: str, sheet: str = "") -> str:
    """Aperçu d'un fichier tabulaire : colonnes, types, premières lignes."""

@mcp.tool()
async def data_schema(url: str) -> str:
    """Schéma détaillé d'un fichier : colonnes, types, stats, valeurs uniques."""

@mcp.tool()
async def data_query(url: str, question: str, max_rows: int = 50) -> str:
    """Interroge un fichier tabulaire en langage naturel."""
```

### openwebui/data_query_tool.py

Tool OWUI classique qui appelle l'API REST du service.

**IMPORTANT** : OpenWebUI exige une classe nommée exactement `Tools` (majuscule T, pas `Tool`, pas `DataQueryTool`). Sans cette classe, OWUI affiche `No Tools class found in the module`. La structure DOIT être :

```python
"""
title: Data Query
author: miraiku
version: 1.0.0
description: Interrogation de fichiers de données (CSV, Excel, JSON, Parquet) en langage naturel
"""

import json
from typing import Optional
from pydantic import BaseModel, Field


class Tools:
    """Tool OWUI pour interroger des fichiers tabulaires via le service data-query."""

    class Valves(BaseModel):
        """Configuration modifiable depuis l'UI OWUI."""
        data_query_api_url: str = Field(
            default="http://data-query:8093",
            description="URL du service data-query"
        )

    def __init__(self):
        self.valves = self.Valves()

    async def data_preview(self, url: str, __user__: dict = {}) -> str:
        """
        Aperçu d'un fichier de données (CSV, Excel, JSON, Parquet).
        Retourne les colonnes, types et premières lignes.

        :param url: URL du fichier à prévisualiser
        :return: Aperçu du fichier avec colonnes et premières lignes
        """
        import httpx
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    f"{self.valves.data_query_api_url}/preview",
                    json={"url": url}
                )
                resp.raise_for_status()
                data = resp.json()
                lines = [f"**Fichier** : {data.get('filename', '?')} ({data.get('format', '?')})"]
                lines.append(f"**Lignes** : {data.get('rows', '?')}")
                lines.append(f"**Colonnes** : {', '.join(data.get('columns', []))}")
                lines.append("")
                lines.append("**Aperçu :**")
                for row in data.get("preview", [])[:5]:
                    lines.append(f"  {row}")
                return "\n".join(lines)
        except Exception as e:
            return f"Erreur lors de la prévisualisation : {e}"

    async def data_schema(self, url: str, __user__: dict = {}) -> str:
        """
        Schéma détaillé d'un fichier de données : colonnes, types, statistiques.

        :param url: URL du fichier à analyser
        :return: Schéma avec types, stats et valeurs d'exemple
        """
        import httpx
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    f"{self.valves.data_query_api_url}/schema",
                    json={"url": url}
                )
                resp.raise_for_status()
                data = resp.json()
                lines = [f"**{data.get('row_count', '?')} lignes**", ""]
                for col in data.get("columns", []):
                    line = f"- **{col['name']}** ({col['dtype']})"
                    if "min" in col:
                        line += f" | min={col['min']} max={col['max']} moy={col.get('mean','?')}"
                    if "sample_values" in col:
                        line += f" | ex: {', '.join(str(v) for v in col['sample_values'][:3])}"
                    lines.append(line)
                return "\n".join(lines)
        except Exception as e:
            return f"Erreur : {e}"

    async def data_query(self, url: str, question: str, __user__: dict = {}) -> str:
        """
        Interroge un fichier de données en langage naturel.
        Supporte CSV, Excel, JSON, Parquet.

        :param url: URL du fichier de données
        :param question: Question en langage naturel (ex: "les 10 plus grandes communes")
        :return: Résultat de la requête sous forme de tableau
        """
        import httpx
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.post(
                    f"{self.valves.data_query_api_url}/query",
                    json={"url": url, "question": question, "max_rows": 50}
                )
                resp.raise_for_status()
                data = resp.json()

                if "error" in data:
                    return f"Erreur : {data['message']}\n{data.get('suggestion', '')}"

                lines = [f"**Opération** : `{data.get('operation', '?')}`"]
                lines.append(f"**Résultats** : {data.get('row_count', '?')} lignes")
                if data.get("truncated"):
                    lines.append("_(résultats tronqués)_")
                lines.append("")

                results = data.get("result", [])
                if results:
                    # Format as markdown table
                    headers = list(results[0].keys())
                    lines.append("| " + " | ".join(headers) + " |")
                    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
                    for row in results:
                        lines.append("| " + " | ".join(str(row.get(h, "")) for h in headers) + " |")

                return "\n".join(lines)
        except Exception as e:
            return f"Erreur lors de la requête : {e}"
```

**Points critiques pour que OWUI reconnaisse le tool :**
- La classe DOIT s'appeler `Tools` (pas `Tool`)
- Le docstring du module (en haut du fichier) DOIT contenir `title`, `author`, `version`
- Chaque méthode publique DOIT avoir un docstring (c'est la description visible dans l'UI)
- Les paramètres DOIVENT avoir des annotations de type
- `__user__` DOIT avoir une valeur par défaut (`= {}`) pour que OWUI puisse l'appeler
- `self.valves` DOIT être initialisé dans `__init__`

### owui-plugin.yaml

```yaml
name: data-query-owui
version: "1.0.0"

services:
  - name: data-query
    port: 8093

pipelines:
  files: []

tools:
  entries:
    - id: data_query
      source_file: openwebui/data_query_tool.py
      service_name: data-query
      service_port: 8093

model_tools: []
env_vars: []

k8s:
  namespace: miraiku
  custom_image: true
```

### docker-compose.yaml

```yaml
services:
  data-query:
    build: .
    ports:
      - "8093:8093"
      - "8088:8088"
    env_file:
      - .env
    networks:
      - owui-net
    healthcheck:
      test: ["CMD", "python", "-c", "import httpx; httpx.get('http://localhost:8093/healthz').raise_for_status()"]
      interval: 15s
      timeout: 5s
      retries: 5

networks:
  owui-net:
    external: true
```

### Dockerfile

```dockerfile
FROM python:3.12-slim

RUN groupadd -g 1000 appuser && useradd -u 1000 -g 1000 -r -d /app -s /sbin/nologin appuser
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY openwebui/ ./openwebui/
COPY entrypoint.py .

RUN mkdir -p /app/cache && chown -R appuser:appuser /app/cache
USER appuser

EXPOSE 8093 8088

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://localhost:8093/healthz').raise_for_status()"

CMD ["python", "entrypoint.py"]
```

### requirements.txt

```
fastapi>=0.111
uvicorn[standard]>=0.30
httpx>=0.27
pandas>=2.2
openpyxl>=3.1
pyarrow>=15.0
odfpy>=1.4
pydantic>=2.0
pydantic-settings>=2.0
mcp>=1.26.0
```

## Tests

### test_file_loader.py
- Téléchargement CSV, XLS, JSON, Parquet depuis des URLs de test
- Détection de format automatique
- Gestion des erreurs (404, timeout, fichier trop gros, format inconnu)
- Cache hit/miss
- **Fallback URLs cassées** : simuler un 404 sur l'URL primaire, vérifier que le fallback via l'API data.gouv.fr est tenté
- **Redirect loops** : simuler une boucle de redirections, vérifier l'abandon après 5 hops
- **Détection format par magic bytes** : fichier CSV servi sans extension ni Content-Type correct
- **Retry sur 5xx** : simuler un 503 puis un 200, vérifier que le retry fonctionne

### test_query_engine.py
- Traduction NL → plan pour des questions types :
  - "Quelles sont les 10 communes les plus peuplées ?" → top_n
  - "Combien de communes dans le 93 ?" → filter + count
  - "Population moyenne par département" → group_mean
  - "Cherche toutes les communes contenant 'Saint'" → search
- Exécution du plan sur un DataFrame de test
- Cas limites : colonnes avec accents, valeurs nulles, types mixtes

### test_sandbox.py
- Vérifier que seules les opérations whitelistées sont exécutées
- Timeout sur opération longue
- Limite de mémoire respectée
- Tentative d'injection refusée

### test_api.py
- Endpoints /healthz, /preview, /schema, /query
- Réponses correctes avec un fichier CSV de test embarqué

## Gestion d'erreurs

Tous les endpoints doivent retourner des erreurs exploitables par le LLM :

```json
// 422 — fichier inaccessible
{
  "error": "file_unavailable",
  "message": "Le fichier n'est plus accessible à l'URL indiquée.",
  "tried_urls": ["https://... → 404", "https://... → timeout"],
  "suggestion": "Essayez avec un autre fichier du même dataset, ou vérifiez l'URL."
}

// 415 — format non supporté
{
  "error": "unsupported_format",
  "message": "Format 'pdf' non supporté. Formats acceptés : csv, xls, xlsx, json, parquet, ods."
}

// 413 — fichier trop gros
{
  "error": "file_too_large",
  "message": "Le fichier fait 250 Mo, la limite est 100 Mo.",
  "suggestion": "Essayez avec un fichier plus petit ou une version filtrée du dataset."
}

// 400 — question incompréhensible
{
  "error": "query_failed",
  "message": "Impossible de traduire la question en opérations sur les données.",
  "schema_hint": "Colonnes disponibles : code_commune, nom_commune, population, departement"
}
```

Le LLM pourra ainsi reformuler sa requête ou proposer une alternative à l'utilisateur.

## Sécurité

- **PAS d'exec/eval** de code Python arbitraire
- Opérations pandas whitelistées uniquement
- Timeout sur téléchargement (30s) et sur requête (10s)
- Taille max des fichiers (100 Mo)
- Limite de lignes en sortie (100 par défaut)
- Container non-root (UID 1000)
- Pas de filesystem write sauf le cache (répertoire dédié)

## Exécution

```bash
# Initialiser le repo
cd ~/Documents/GitHub
mkdir data-query-owui && cd data-query-owui
git init

# Créer tous les fichiers
# ... (le prompt crée tout)

# Tester en local
cp .env.example .env
# Remplir LLM_API_URL et LLM_API_KEY
docker compose up -d
curl http://localhost:8093/healthz

# Tester une requête
curl -X POST http://localhost:8093/preview \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.data.gouv.fr/fr/datasets/r/630e7917-02db-4838-8856-09235719551c"}'

# Commit
git add -A && git commit -m "feat: initial data-query-owui service"
```

## Contraintes

- Ne pas utiliser exec(), eval(), compile() ou tout autre mécanisme d'exécution de code dynamique
- Le service doit fonctionner sans GPU
- Le service doit être stateless (sauf le cache fichier sur disque)
- Tous les fichiers doivent être créés, pas de placeholders
- Les tests doivent pouvoir tourner sans réseau (mocks pour les appels HTTP et LLM)
- Le code doit être prêt à être déployé sur K8s namespace miraiku
- Utiliser les mêmes conventions que les autres repos de l'écosystème (tchap-reader, browser-skill-owui)
