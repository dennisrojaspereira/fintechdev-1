# Fintech Polyglot Demo

Ambiente com múltiplos serviços implementados em diferentes linguagens, todos usando PostgreSQL e expostos via Docker Compose. O k6 roda automaticamente para testar todos os serviços e enviar métricas ao Prometheus/Grafana.

## Pré-requisitos
- Docker Desktop (WSL2 habilitado).
- Portas livres: 5432, 8001-8007, 8088, 9090, 3001.

## Subir o ambiente
```
docker compose up -d --build
```
- O Postgres tem healthcheck; os apps só iniciam após o banco ficar saudável.
- Veja logs em tempo real: `docker compose logs -f`.

## Parar / limpar
- Parar: `docker compose down`
- Parar e limpar volume do banco: `docker compose down -v` (apaga dados).

## Serviços e portas
| Serviço | Stack | URL | Métricas |
| --- | --- | --- | --- |
| go | Go + pgx | http://localhost:8001 | /metrics |
| java | Java 21 + JDBC | http://localhost:8002 | /metrics |
| node | Node 20 + pg | http://localhost:8003 | /metrics |
| python | FastAPI + psycopg | http://localhost:8004 | /metrics |
| php | PHP 8.2 | http://localhost:8005 | (sem métricas Prometheus) |
| java-spring-classic | Spring Boot (tomcat) | http://localhost:8006 | /actuator/prometheus |
| java-spring-virtual | Spring Boot (virtual threads) | http://localhost:8007 | /actuator/prometheus |
| dotnet | .NET 10 + OpenTelemetry | [http://localhost:8008](http://localhost:8008/scalar) | /metrics |
| postgres | PostgreSQL 16 | localhost:5432 | n/a |
| cadvisor | Runtime metrics | http://localhost:8088 | /metrics |
| prometheus | Prometheus UI | http://localhost:9090 | /metrics |
| grafana | Dashboards | http://localhost:3001 (admin/admin) | n/a |

Credenciais do banco (em `docker-compose.yml` e `db/init.sql`):
- host: postgres
- db: fintech
- user: fintech
- pass: fintech

## API de transferência (todos os serviços)
`POST /transfer` com JSON:
```json
{
  "fromAccountId": "A",
  "toAccountId": "B",
  "amount": 10,
  "operationId": "op-123"  // opcional, idempotência
}
```
- Erros de validação retornam HTTP 400.
- `operationId` evita processamento duplicado.
- Métricas de saldo são expostas (onde suportado) para observabilidade.

## Dados iniciais
O `db/init.sql` cria tabelas `accounts`, `ledger`, `processed_ops` e insere:
- Conta A: 1000.00
- Conta B: 500.00

## Teste de carga (k6)
- O serviço `k6` sobe junto e executa o script `k6/loadtest.js` contra todos os serviços.
- Saídas JSON ficam em `k6/summary-*.json` (montado via volume).
- Para rodar manualmente apenas o k6:
```powershell
docker compose run --rm k6 sh -c "set -e; for svc in go:8080 java:8080 node:3000 python:8000 php:8000 java-spring-classic:8080 java-spring-virtual:8080 dotnet:8080; do name=$${svc%%:*}; echo Testing $$svc; BASE_URL=http://$$svc k6 run --vus 20 --duration 30s --tag service=$$name --out experimental-prometheus-rw --summary-export summary-$${name}.json loadtest.js; done"
```

## Diagramas (Mermaid)
- Sequence e C4 (containers) em `DIAGRAMS.md`.

## Observabilidade
- Prometheus scrape configurado em `observability/prometheus/prometheus.yml` (apps e infra). PHP não expõe `/metrics` e por isso gera aviso no Prometheus.
- Grafana provisionada com datasources e dashboards em `observability/grafana`. Login padrão: `admin/admin`.

## Dicas de troubleshooting
- Estado dos contêineres: `docker compose ps`
- Logs de um serviço: `docker compose logs <servico>` (ex.: `go`, `java`, `node`...)
- Reset do banco: `docker compose down -v && docker compose up -d --build`
- Se faltar porta, ajuste mapeamentos em `docker-compose.yml`.
