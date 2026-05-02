# Kubernetes Manifests pour AdvDB TP2

## Structure

- `namespace.yaml` - Namespace `advdb` pour l'application
- `neo4j-configmap.yaml` - Configuration Neo4j 
- `neo4j-deployment.yaml` - Deployment Neo4j avec probes de santé
- `neo4j-service.yaml` - Services pour Neo4j (ClusterIP et NodePort)
- `app-configmap.yaml` - Configuration pour l'app
- `app-deployment.yaml` - Deployment pour l'app avec init container pour attendre Neo4j
- `kustomization.yaml` - Fichier Kustomize pour déployer tout en une commande

## Déploiement

### Avec kubectl apply

```bash
kubectl apply -k ./k8s
```

### Ou sans Kustomize

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/neo4j-configmap.yaml
kubectl apply -f k8s/neo4j-deployment.yaml
kubectl apply -f k8s/neo4j-service.yaml
kubectl apply -f k8s/app-configmap.yaml
kubectl apply -f k8s/app-deployment.yaml
```

## Accès

### Neo4j Browser (HTTP)
- **ClusterIP**: `neo4j:7474` (depuis les pods)
- **NodePort**: `http://localhost:30474` (depuis l'hôte)
- **Credentials**: `neo4j / test`

### Bolt Driver
- **ClusterIP**: `neo4j:7687` (depuis les pods)
- **NodePort**: `localhost:30687` (depuis l'hôte)

## Vérification

```bash
# Vérifier le statut des deployments
kubectl get deployments -n advdb

# Vérifier les pods
kubectl get pods -n advdb

# Voir les logs Neo4j
kubectl logs -n advdb -l app=neo4j -f

# Voir les logs de l'app
kubectl logs -n advdb -l app=advdb-app -f

# Port-forward Neo4j Browser
kubectl port-forward -n advdb svc/neo4j 7474:7474
# Puis accéder à http://localhost:7474
```

## Notes

- Neo4j utilise `emptyDir` pour les données (n'est pas persistant)
- Pour la persistence, utilisez `PersistentVolumeClaim`
- L'app attend que Neo4j soit prêt avant de démarrer (init container)
- Limites mémoire: Neo4j 2Gi max, App 4Gi max
