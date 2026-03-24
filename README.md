# tsdb-aggregate-proxy

Experimental proxy setup for querying ClickHouse metrics data as Prometheus datasource

## Build 

`make build` to build a local binary

```
make build
bin/tsdb-aggregate-proxy --help
```

`make docker` to build a container image

```
make docker
docker run ghcr.io/maratoid/tsdb-aggregate-proxy:1.0.0 -h
```

## docker compose 

Build the container first:

```
make docker
```

Then stand up compose:

```
cd deploy
docker compose up -d
```

* http://localhost:8123 - ClickHouse web (use default/default or ingest/p@ssw0rd as login)
* http://localhost:9090 - Prometheus
* http://localhost:3000 - Grafana (use admin/admin as login)
* http://localhost:9091/metrics - proxy metrics

