# Deploying the ERP service to OpenShift (Red Hat Developer Sandbox)

OpenShift is Kubernetes plus a few extras: **BuildConfig/ImageStream** (build images in the cluster) and **Route** (public HTTPS URL). Everything else here (Deployment, Service, PVC, Secret, ConfigMap, probes) is plain Kubernetes.

```
             Route (edge TLS, public https URL)
                │
          Service erp-service :8080
                │
  Deployment erp-service (1 pod, rolling update)       BuildConfig ── git main ──> ImageStream erp-service:latest
     startup / liveness  -> /actuator/health/liveness             (image trigger rolls the Deployment)
     readiness           -> /actuator/health/readiness (includes db)
                │
          Service mysql :3306
                │
  Deployment mysql (Recreate) ── PVC mysql-data 1Gi
```

| File | Objects |
|------|---------|
| `mysql.yaml` | PVC, Deployment (sclorg MySQL 8, runs with OpenShift's random UID), Service |
| `erp-build.yaml` | ImageStream + BuildConfig (Docker strategy, `erp-app/Dockerfile`, context = repo root) |
| `erp-service.yaml` | ConfigMap, Deployment with probes, Service, Route |
| `kustomization.yaml` | applies all of the above |

## Probe design

| Probe | Endpoint | Why |
|-------|----------|-----|
| startup | `/actuator/health/liveness`, up to 180 s | JVM + Flyway boot is slow on a small CPU share; liveness is not evaluated until this passes, so a slow start is never killed |
| liveness | `/actuator/health/liveness` | Restart only if the app itself is broken. It deliberately does **not** check MySQL: a DB outage would otherwise restart every pod in a loop and fix nothing |
| readiness | `/actuator/health/readiness` (`readinessState` + `db`) | If MySQL is unreachable the pod is taken out of the Service (no traffic) but kept running, and comes back by itself |

`maxUnavailable: 0` plus readiness gives a zero-downtime rollout: the new pod has to be Ready before the old one is removed.

## Steps

1. Get a free sandbox at <https://developers.redhat.com/developer-sandbox> and install the `oc` CLI.
2. In the web console: your name (top right) → **Copy login command** → run the `oc login --token=... --server=...` it shows.
3. Create the database secret (not committed to git):
   ```bash
   oc create secret generic erp-db \
     --from-literal=username=erp \
     --from-literal=password="$(openssl rand -hex 16)" \
     --from-literal=root-password="$(openssl rand -hex 16)"
   ```
4. Apply everything and follow the image build (it builds the `main` branch; takes a few minutes):
   ```bash
   oc apply -k deploy/openshift
   oc logs -f bc/erp-service
   ```
5. Wait for the rollout and get the URL:
   ```bash
   oc rollout status deploy/mysql
   oc rollout status deploy/erp-service
   oc get route erp-service -o jsonpath='https://{.spec.host}{"\n"}'
   ```
6. Check it:
   ```bash
   URL=https://$(oc get route erp-service -o jsonpath='{.spec.host}')
   curl -s $URL/actuator/health/readiness        # {"status":"UP"}
   # Swagger UI: $URL/swagger-ui.html
   ```

Redeploy after merging to `main`: `oc start-build erp-service --follow`. The image trigger rolls the Deployment automatically.

## Things to show in an interview

```bash
oc get pods -w                         # watch it all
oc scale deploy/mysql --replicas=0     # DB down: erp pod goes 0/1 READY (readiness), but RESTARTS stays 0 (liveness)
oc scale deploy/mysql --replicas=1     # it becomes Ready again on its own
oc delete pod -l app=erp-service       # self-healing: the Deployment recreates the pod
oc rollout undo deploy/erp-service     # roll back to the previous ReplicaSet
```

## Sandbox limits and choices
* **No Kafka here.** `ERP_OUTBOX_ENABLED=false`: orders still run through the whole saga, and the ledger events stay `PENDING` in `outbox_event` (the outbox makes this safe; they would be published as soon as a broker is configured). Kafka is too heavy for the sandbox quota.
* **Sandbox scales to zero** after inactivity, and the account expires after 30 days. Run `oc apply -k` again to recreate.
* **The Route is public, and the ERP API has no authentication yet.** Fine for a demo with fake data, nothing more. Delete it when not demoing: `oc delete route erp-service`.
* One replica each. More ERP replicas would work (the outbox publisher uses `SKIP LOCKED`), but sandbox quota is small.
