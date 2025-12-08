# Diagramas do ambiente

## Sequence – fluxo k6, apps e métricas
```mermaid
sequenceDiagram
    autonumber
    participant User as User
    participant Grafana
    participant Prom as Prometheus
    participant cAdv as cAdvisor
    participant k6 as k6 Runner
    participant Go as Go svc
    participant Java as Java svc
    participant Node as Node svc
    participant Py as Python svc
    participant PHP as PHP svc
    participant SBC as Spring Classic
    participant SBV as Spring Virtual
    participant PG as PostgreSQL

    User->>Grafana: Consulta dashboards
    Grafana-->>Prom: Queries
    Prom-->>Grafana: Series

    cAdv-->>Prom: Container CPU/metrics (infra)
    k6->>Go: POST /transfer (loop VUs)
    k6->>Java: POST /transfer
    k6->>Node: POST /transfer
    k6->>Py: POST /transfer
    k6->>PHP: POST /transfer
    k6->>SBC: POST /transfer
    k6->>SBV: POST /transfer

    Go->>PG: begin/lock/read/update/commit
    Java->>PG: begin/lock/read/update/commit
    Node->>PG: begin/lock/read/update/commit
    Py->>PG: begin/lock/read/update/commit
    PHP->>PG: begin/lock/read/update/commit
    SBC->>PG: begin/lock/read/update/commit
    SBV->>PG: begin/lock/read/update/commit

    Go-->>k6: resultado + métricas /metrics
    Java-->>k6: resultado + métricas /metrics
    Node-->>k6: resultado + métricas /metrics
    Py-->>k6: resultado + métricas /metrics
    PHP-->>k6: resultado (sem /metrics Prometheus)
    SBC-->>k6: resultado + /actuator/prometheus
    SBV-->>k6: resultado + /actuator/prometheus

    Go-->>Prom: /metrics scrape
    Java-->>Prom: /metrics scrape
    Node-->>Prom: /metrics scrape
    Py-->>Prom: /metrics scrape
    SBC-->>Prom: /actuator/prometheus scrape
    SBV-->>Prom: /actuator/prometheus scrape
    note right of PHP: PHP não expõe /metrics; Prometheus gera aviso se configurado
```

## C4 (Container) em Mermaid
```mermaid
graph TB
    user([User]):::ext
    grafana[[Grafana UI]]:::web
    prom[(Prometheus)]:::obs
    cadv[[cAdvisor]]:::obs
    k6[[k6 Load Tester]]:::tool
    pg[(PostgreSQL 16)]:::db

    subgraph Apps (Docker Compose)
        goSvc[[Go svc :8080]]:::app
        javaSvc[[Java svc :8080]]:::app
        nodeSvc[[Node svc :3000]]:::app
        pySvc[[Python svc :8000]]:::app
        phpSvc[[PHP svc :8000]]:::app
        sbcSvc[[Spring Classic :8080]]:::app
        sbvSvc[[Spring Virtual :8080]]:::app
    end

    user --> grafana
    grafana --> prom

    k6 --> goSvc
    k6 --> javaSvc
    k6 --> nodeSvc
    k6 --> pySvc
    k6 --> phpSvc
    k6 --> sbcSvc
    k6 --> sbvSvc

    goSvc --> pg
    javaSvc --> pg
    nodeSvc --> pg
    pySvc --> pg
    phpSvc --> pg
    sbcSvc --> pg
    sbvSvc --> pg

    prom --> grafana
    prom <-- cadv

    goSvc --> prom
    javaSvc --> prom
    nodeSvc --> prom
    pySvc --> prom
    sbcSvc --> prom
    sbvSvc --> prom
    phpSvc -.warn.-> prom

    classDef app fill=#1f78c1,stroke=#0f3c60,color=#fff
    classDef db fill=#5c6f26,stroke=#2f3813,color=#fff
    classDef obs fill=#9c4f96,stroke=#4c254a,color=#fff
    classDef web fill=#ef6c00,stroke=#a04700,color=#fff
    classDef tool fill=#00897b,stroke=#00594f,color=#fff
    classDef ext fill=#757575,stroke=#4a4a4a,color=#fff
```
