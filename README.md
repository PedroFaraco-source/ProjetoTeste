# ProjetoMBras

## Run Project

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

### Grafana Explore (Elastic datasource)

This repository does not contain Grafana datasource provisioning files. Configure Elastic datasource manually in Grafana:

1. Go to `Connections -> Data sources -> Add data source -> Elasticsearch`.
2. URL: your Elastic URL (example: `http://localhost:9201`).
3. Index name: `projetombras-logs-*`.
4. Time field: `@timestamp`.
5. Save & test.

Explore query examples:

- `correlation_id:"<id>"`
- `event:"http_request" AND status_code:[500 TO 599]`
- `event:"http_request" AND path:"/analyze-feed"`

## Local Validation Commands (Windows)

Trigger a `202` batch request:

```powershell
$payload = @{
  items = @(
    @{
      user_id = "user_batch_local"
      sentiment_distribution = @{ positive = 20; negative = 10; neutral = 70 }
      engagement_score = 11.2
      trending_topics = @("#mbras")
      influence_ranking = @()
      anomaly_detected = $false
      anomaly_type = $null
      flags = @{ mbras_employee = $false; special_pattern = $false; candidate_awareness = $false }
    }
  )
}
Invoke-RestMethod -Method Post -Uri "http://localhost:8000/analyze-feed" -ContentType "application/json" -Body ($payload | ConvertTo-Json -Depth 10)
```

Trigger a controlled `500`:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8000/debug/force-500"
```

Check metrics:

```powershell
Invoke-WebRequest -Uri "http://localhost:8000/metrics" | Select-Object -ExpandProperty Content
```
