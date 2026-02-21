# ProjetoMBras

## Run Project

## Local Infrastructure Ports

The app in this repository uses these local endpoints:

- RabbitMQ AMQP: `localhost:4000`
- RabbitMQ UI: `http://localhost:4001`
- Elasticsearch: `http://localhost:5000`
- Kibana: `http://localhost:5001`
- SQL Server: `localhost:1434`
- Grafana (dashboards): `http://localhost:3000`
- Prometheus (metrics backend): `http://localhost:3001`

Services not consumed by this project runtime (for example Neo4j, Keycloak, Jenkins) are intentionally not configured here.

### Docker

```powershell
docker compose up -d --build
```

### Local

API:

```powershell
python -m app.infrastructure.runtime.start_api
```

Worker:

```powershell
python -m app.infrastructure.messaging.consumers.ingestor_consumer
```

Outbox publisher:

```powershell
python -m app.infrastructure.messaging.consumers.outbox_publisher
```

Run migrations only:

```powershell
python -m app.infrastructure.runtime.migrate
```

## Run Tests

Unit/integration tests:

```powershell
python -m pytest -q
```

Smoke test:

```powershell
python tests/smoke/run_batched_load.py --base-url http://localhost:8080
```

Load test 1x1000:

```powershell
python tests/smoke/run_batched_load.py `
  --base-url http://localhost:8080 `
  --batches 1 `
  --batch-size 1000 `
  --parallel-batches 10 `
  --concurrency-total 200 `
  --seed 123 `
  --include-invalid true
```

## Observability

Grafana dashboard JSON:

- `app/infrastructure/monitoring/grafana/projetombras-dashboard.json`