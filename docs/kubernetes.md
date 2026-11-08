# Kubernetes (kind + Helm)

The chart in `deploy/helm/skybook` deploys every service. With the default values it also deploys dev-only Postgres, Redis and Kafka
**inside the cluster**, so it runs on a laptop with [kind](https://kind.sigs.k8s.io/). On EKS, set `infra.enabled=false` and point
`connections.*` at RDS, ElastiCache and MSK (the CDK outputs).

## Run on kind (Windows PowerShell, Docker Desktop running)

```powershell
winget install Kubernetes.kind; winget install Helm.Helm; winget install Kubernetes.kubectl   # one-time, then reopen the terminal

kind create cluster --name skybook
docker compose build seed                                    # builds skybook-app:latest (same image as Compose)
kind load docker-image skybook-app:latest --name skybook     # side-load the image (no registry needed)

helm lint .\deploy\helm\skybook
helm install skybook .\deploy\helm\skybook --namespace skybook --create-namespace
kubectl -n skybook get pods -w                                # wait until all are Running/Completed (~2 min; services restart until Kafka is ready)

docker compose down                                           # free port 8080 used by the Compose stack
kubectl -n skybook port-forward --address 0.0.0.0 svc/skybook-gateway 8080:8080
# in a second terminal - run the same e2e suite against the cluster:
docker compose --profile tools run --rm --no-deps -e GATEWAY_URL=http://host.docker.internal:8080 e2e
```

Remove it with `kind delete cluster --name skybook`.

## What the chart contains

| Template | Purpose |
|---|---|
| `services.yaml` | Deployment + Service per service (`values.services`), readiness `/ready`, liveness `/health`, Prometheus annotations, non-root |
| `secret.yaml` | JWT secret, internal token, DB password (skipped when `secrets.existingSecret` is set, e.g. via External Secrets Operator) |
| `serviceaccount.yaml` | `skybook-app` service account (IRSA annotation on EKS) |
| `aws-bridge.yaml` | Bridge Deployment + its own `aws-bridge` service account (IRSA to the send-only role); `required` guard on `queueUrl` |
| `seed-job.yaml` | One-shot Job that seeds flights and inventory |
| `ingress.yaml` | Gateway Ingress (`className: alb` + WAF annotation on EKS) |
| `networkpolicy.yaml` | Internal services accept traffic only from the gateway |
| `infra.yaml` | Dev-only Postgres / Redis / Kafka (KRaft) for kind |

## Verification status

- ✅ **Deployed and verified on kind (owner's machine, 2026-11-07).** `helm lint`: 0 failures. `helm install`: 13 pods Running
  (10 service pods + Kafka, Postgres, Redis), seed Job Completed. The **same e2e suite passed 23/23** through
  `kubectl port-forward svc/skybook-gateway`.
- 🐛 **Fixed during that run:** Kafka advertises `skybook-kafka:9092`, so its Service now sets `publishNotReadyAddresses: true`
  (otherwise neither the readiness check nor any client can reach the broker before it is "ready"). The readiness probe is a TCP check,
  and the Deployment uses `strategy: Recreate`, so two single-node brokers never run side by side during an update.
- Templates are also checked offline with a Go `text/template` renderer + YAML validation (kind profile: 23 manifests, EKS profile: 17).
- Note: kind's default CNI does not enforce NetworkPolicy. On EKS it's enforced with the VPC CNI's network-policy agent, or with Calico.
