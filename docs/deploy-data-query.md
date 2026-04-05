# Prompt : Déployer data-query-owui sur Docker local et K8s Scaleway

Tu es un coding assistant. Le repo `~/Documents/GitHub/data-query-owui` a été créé par un prompt précédent. Tu dois maintenant l'intégrer dans l'écosystème Miraiku (Docker local + K8s Scaleway).

## Contexte

- Socle OWUI : repo `~/Documents/GitHub/experimentation-owui`
- Namespace K8s : `miraiku` sur Scaleway (cluster `k8s-par-brave-bassi`)
- Registry : `rg.fr-par.scw.cloud/funcscwnspricelessmontalcinhiacgnzi`
- Les secrets Scaleway LLM sont dans le secret K8s `miraiku-secrets` (clé `SCW_SECRET_KEY_LLM`)
- OpenWebUI tourne sur le port 80 (service `openwebui`)
- Le réseau Docker est `owui-net` (external)
- Les images doivent être buildées en `--platform linux/amd64` (cluster AMD64, dev Mac ARM)
- Les PVC RWO nécessitent scale 0 → 1 pour changer l'image
- Le container doit tourner en non-root (UID 1000, fsGroup 1000)

## Étapes

### 1. Vérifier que le repo est fonctionnel

```bash
cd ~/Documents/GitHub/data-query-owui
# Vérifier que tous les fichiers existent
ls app/main.py app/api.py app/query_engine.py app/file_loader.py app/mcp_server.py
ls Dockerfile docker-compose.yaml requirements.txt owui-plugin.yaml .env.example
ls openwebui/data_query_tool.py
ls tests/

# Lancer les tests
pip install -r requirements.txt
pytest tests/ -v
```

### 2. Déployer en Docker local

```bash
cd ~/Documents/GitHub/data-query-owui

# Créer le .env à partir de l'example
cp .env.example .env
# Remplir LLM_API_URL et LLM_API_KEY depuis le .env du socle :
#   LLM_API_URL = SCW_LLM_BASE_URL du socle
#   LLM_API_KEY = SCW_SECRET_KEY_LLM du socle

# Builder et lancer
docker compose up -d --build

# Vérifier
curl http://localhost:8093/healthz

# Tester un preview
curl -X POST http://localhost:8093/preview \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.data.gouv.fr/fr/datasets/r/be303501-5c46-48a1-87b4-3d198423ff49"}'

# Tester une query
curl -X POST http://localhost:8093/query \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.data.gouv.fr/fr/datasets/r/be303501-5c46-48a1-87b4-3d198423ff49", "question": "Quelles sont les 5 communes les plus peuplées ?"}'
```

### 3. Enregistrer le tool dans OpenWebUI local

```bash
# Lire le fichier tool OWUI
TOOL_CONTENT=$(cat ~/Documents/GitHub/data-query-owui/openwebui/data_query_tool.py)

# Injecter dans la DB OWUI
docker exec experimentation-owui-openwebui-1 python3 -c "
import sqlite3, json, time, ast

conn = sqlite3.connect('/app/backend/data/webui.db')
now = int(time.time())

content = '''$TOOL_CONTENT'''

# Remplacer l'URL localhost par le nom du service Docker
content = content.replace('http://localhost:8093', 'http://data-query:8093')

specs = []  # Les specs seront générées par OWUI au chargement

meta = json.dumps({
    'description': 'Interrogation de fichiers de données (CSV, Excel, JSON, Parquet) en langage naturel',
    'manifest': {'title': 'Data Query', 'author': 'auto-registered', 'version': '1.0.0'},
})

conn.execute(
    'INSERT OR REPLACE INTO tool (id, user_id, name, content, specs, meta, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
    ('data_query', '', 'Data Query', content, json.dumps(specs), meta, now, now)
)
conn.commit()
print('Tool data_query registered')
conn.close()
"
```

### 4. Build et push pour K8s Scaleway

```bash
cd ~/Documents/GitHub/data-query-owui
REGISTRY=rg.fr-par.scw.cloud/funcscwnspricelessmontalcinhiacgnzi
TAG=$(date +%Y%m%d-%H%M%S)

# Build AMD64 et push
docker buildx build --platform linux/amd64 --push \
  -t "$REGISTRY/data-query:$TAG" .

echo "Image: $REGISTRY/data-query:$TAG"
```

### 5. Déployer sur K8s Scaleway

```bash
REGISTRY=rg.fr-par.scw.cloud/funcscwnspricelessmontalcinhiacgnzi
TAG=<le tag du step 4>

# ConfigMap
kubectl -n miraiku create configmap data-query-config \
  --from-literal=LLM_API_URL="https://api.scaleway.ai/a9158aac-8404-46ea-8bf5-1ca048cd6ab4/v1" \
  --from-literal=LLM_MODEL="mistral-small-3.2-24b-instruct-2506" \
  --from-literal=MAX_FILE_SIZE_MB="100" \
  --from-literal=MAX_ROWS_OUTPUT="100" \
  --from-literal=CACHE_TTL_SECONDS="3600" \
  --dry-run=client -o yaml | kubectl apply -f -

# Deployment
cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: data-query
  namespace: miraiku
  labels:
    app: data-query
spec:
  replicas: 1
  selector:
    matchLabels:
      app: data-query
  template:
    metadata:
      labels:
        app: data-query
    spec:
      securityContext:
        fsGroup: 1000
      imagePullSecrets:
        - name: miraiku-registry
      containers:
        - name: data-query
          image: $REGISTRY/data-query:$TAG
          ports:
            - containerPort: 8093
            - containerPort: 8088
          envFrom:
            - configMapRef:
                name: data-query-config
            - secretRef:
                name: miraiku-secrets
          env:
            - name: LLM_API_KEY
              valueFrom:
                secretKeyRef:
                  name: miraiku-secrets
                  key: SCW_SECRET_KEY_LLM
          resources:
            requests:
              memory: "128Mi"
              cpu: "100m"
            limits:
              memory: "512Mi"
              cpu: "500m"
          readinessProbe:
            httpGet:
              path: /healthz
              port: 8093
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /healthz
              port: 8093
            initialDelaySeconds: 10
            periodSeconds: 30
          securityContext:
            runAsNonRoot: true
            runAsUser: 1000
            allowPrivilegeEscalation: false
          volumeMounts:
            - name: cache
              mountPath: /app/cache
      volumes:
        - name: cache
          emptyDir:
            sizeLimit: 500Mi
EOF

# Service
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Service
metadata:
  name: data-query
  namespace: miraiku
  labels:
    app: data-query
spec:
  type: ClusterIP
  selector:
    app: data-query
  ports:
    - port: 8093
      targetPort: 8093
      protocol: TCP
      name: api
    - port: 8088
      targetPort: 8088
      protocol: TCP
      name: mcp
EOF

# Attendre le déploiement
kubectl rollout status deployment/data-query -n miraiku --timeout=120s
```

### 6. Enregistrer le tool dans OpenWebUI K8s

```bash
# Copier le tool dans le pod OWUI et l'injecter en DB
OWUI_POD=$(kubectl get pods -n miraiku -l app=openwebui -o jsonpath='{.items[0].metadata.name}')

kubectl cp ~/Documents/GitHub/data-query-owui/openwebui/data_query_tool.py miraiku/$OWUI_POD:/tmp/data_query_tool.py

kubectl exec -n miraiku $OWUI_POD -- python3 -c "
import sqlite3, json, time

conn = sqlite3.connect('/app/backend/data/webui.db')
now = int(time.time())

content = open('/tmp/data_query_tool.py').read()
# Remplacer localhost par le service K8s
content = content.replace('http://localhost:8093', 'http://data-query:8093')
content = content.replace('http://host.docker.internal:8093', 'http://data-query:8093')

meta = json.dumps({
    'description': 'Interrogation de fichiers de données en langage naturel',
    'manifest': {'title': 'Data Query', 'author': 'auto-registered', 'version': '1.0.0'},
})

conn.execute(
    'INSERT OR REPLACE INTO tool (id, user_id, name, content, specs, meta, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
    ('data_query', '', 'Data Query', content, '[]', meta, now, now)
)
conn.commit()
print('Tool data_query registered on K8s')
conn.close()
"
```

### 7. Tests de validation

```bash
# --- Docker local ---
echo "=== Docker healthz ==="
curl -s http://localhost:8093/healthz | python3 -m json.tool

echo "=== Docker preview ==="
curl -s -X POST http://localhost:8093/preview \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.data.gouv.fr/fr/datasets/r/be303501-5c46-48a1-87b4-3d198423ff49"}' \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'{d.get(\"rows\",0)} lignes, colonnes: {d.get(\"columns\",[])}') "

echo "=== Docker query ==="
curl -s -X POST http://localhost:8093/query \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.data.gouv.fr/fr/datasets/r/be303501-5c46-48a1-87b4-3d198423ff49", "question": "Les 5 communes les plus peuplées"}' \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Opération: {d.get(\"operation\",\"?\")}'); [print(f'  {r}') for r in d.get(\"result\",[])[:5]]"

# --- K8s Scaleway ---
echo "=== K8s healthz ==="
kubectl exec -n miraiku deployment/openwebui -- curl -s http://data-query:8093/healthz

echo "=== K8s preview ==="
kubectl exec -n miraiku deployment/openwebui -- curl -s -X POST http://data-query:8093/preview \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.data.gouv.fr/fr/datasets/r/be303501-5c46-48a1-87b4-3d198423ff49"}' \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'{d.get(\"rows\",0)} lignes')"

echo "=== K8s connectivity OWUI → data-query ==="
kubectl exec -n miraiku deployment/openwebui -- curl -s http://data-query:8093/healthz
```

### 8. Test end-to-end dans OpenWebUI

Ouvre un chat dans OpenWebUI et teste :

```
Utilisateur : "Donne moi la liste des communes de France"
→ Le modèle appelle le MCP data.gouv.fr → trouve le dataset
→ Suggestions : "Souhaitez-vous explorer ce dataset ensemble ?"

Utilisateur : clique sur la suggestion
→ Le modèle appelle data_preview → affiche les colonnes et un aperçu

Utilisateur : "Quelles sont les 10 communes les plus peuplées du 93 ?"
→ Le modèle appelle data_query → filtre + top_n → résultat formaté
```

### 9. Commit final

```bash
cd ~/Documents/GitHub/data-query-owui
git add -A
git status
git commit -m "feat: data-query-owui service — query tabular files in natural language

- REST API: /preview, /schema, /query endpoints
- Safe query engine: NL → JSON plan → whitelisted pandas ops (no exec/eval)
- File loader with fallback strategy for broken URLs (data.gouv.fr)
- Cache with TTL for downloaded files
- Tool OWUI for OpenWebUI integration
- MCP server (Streamable HTTP) for future MCP integration
- Docker + K8s ready, non-root container

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Contraintes rappel

- Build `--platform linux/amd64` pour Scaleway
- Container non-root (UID 1000)
- URLs dans le tool OWUI : `http://data-query:8093` (pas localhost)
- Le secret `SCW_SECRET_KEY_LLM` est dans `miraiku-secrets`
- Pas d'exec/eval de code
- Cache sur emptyDir (pas de PVC nécessaire)
