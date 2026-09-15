# Site Connector — scaffold (Apache Kafka)

## What's real vs. stubbed

| Piece | Status |
|---|---|
| Repo structure, config loading | Real |
| Hub session (register/heartbeat/push) | Real protocol, HTTP+JSON stand-in for gRPC |
| Store-and-forward buffer | Real |
| Inventory reconciliation loop | Real logic, currently fed by mock/JMX data |
| **Metrics — JMX HTTP collector** | Real: scrapes a Prometheus JMX Exporter endpoint (see `deploy/jmx-exporter.yml`). Set `SC_USE_REAL_COLLECTOR=1` to use it instead of the mock |
| **Logs — tailer** | Real: tails `server.log` + `controller.log`, redacts obvious secrets (password/token/secret/api_key patterns), pushes new lines to the hub |
| **Kafka Connect worker metrics** | Real: `internal/collector/connect.go`, scrapes Connect's JMX exporter (`deploy/jmx-exporter-connect.yml`, scoped to `kafka.connect` domain). Worker-level only (connector/task counts, startup success/failure, rebalance state) — verified against a real running 4.3.1 worker. Opt-in via `SC_CONNECT_JMX_TARGETS`. Per-connector/per-task metrics NOT yet parsed (no real connector has existed to verify the label shape against — the exporter config already scopes broadly enough to pick them up once one exists, the Go code just doesn't read them yet) |
| **Schema Registry (Karapace) metrics** | Real: `internal/collector/schema_registry.go`, scrapes Karapace's native `/metrics` (no JMX needed — Python service). Aggregates request counts/errors across label variants, prefers `http_requests_total` over the deprecated `karapace_http_requests_total` if both present. Opt-in via `SC_SCHEMA_REGISTRY_TARGETS` |
| Topic/consumer-group metadata (AdminClient) | Real — `internal/collector/admin.go`, uses franz-go's `kadm` package. **Not yet build-tested** (authoring sandbox has no Go module proxy access) — first `go build` on your machine may need a small fix if the kadm API differs from what's written. Paste the compiler error back and it'll get patched. |
| mTLS, local guardrail enforcement, credential custody | Not built yet — deferred |
| Helm/Docker/RPM/DEB packaging | Not built yet |

# site-connector.tar

Site Connector — a Go-based telemetry collector for Apache Kafka
(broker/controller JMX, AdminClient, Schema Registry, Kafka Connect
worker metrics, log tailing). Runs inside the customer's network,
pushes telemetry out to a control plane; never accepts inbound
connections.

## Extract

```bash
tar -xf site-connector.tar
cd site-connector
```

## Setting up metrics on your Kafka VM

1. Download the JMX Prometheus Exporter jar (needs internet access on the VM):
   ```bash
   curl -L -o /home/apache_kafka/jmx_prometheus_javaagent.jar \
     https://github.com/prometheus/jmx_exporter/releases/download/1.0.1/jmx_prometheus_javaagent-1.0.1.jar
   ```
2. Copy `deploy/jmx-exporter.yml` to `/home/apache_kafka/kafka_2.13-4.3.1/config/jmx-exporter.yml`.
3. Start the broker with the javaagent attached:
   ```bash
   export KAFKA_OPTS="-javaagent:/home/apache_kafka/jmx_prometheus_javaagent.jar=7071:/home/apache_kafka/kafka_2.13-4.3.1/config/jmx-exporter.yml"
   bin/kafka-server-start.sh config/broker.properties
   ```
4. Verify: `curl http://localhost:7071/metrics` should show `kafka_server_*` lines.
5. Repeat similarly for the controller process if it runs separately (use a different port, e.g. 7072).

## Running the connector against your real VM

```bash
go build -o bin/stubhub ./cmd/stubhub
go build -o bin/connector ./cmd/connector

./bin/stubhub &

SC_USE_REAL_COLLECTOR=1 \
SC_JMX_TARGET=http://<VM_IP>:7071/metrics \
SC_KAFKA_BROKERS=<VM_IP>:9092 \
SC_SERVER_LOG=/home/apache_kafka/kafka_2.13-4.3.1/logs/server.log \
SC_CONTROLLER_LOG=/home/apache_kafka/kafka_2.13-4.3.1/logs/controller.log \
./bin/connector
```

(Run the connector itself on the VM, or wherever it can reach both the
JMX endpoint and the log files on disk — the tailer reads local files,
it doesn't remote-tail.)

## Local dev without a real cluster

```bash
docker compose up -d          # Kafka in KRaft mode + kafka-ui on :8080
./bin/stubhub &
./bin/connector                # mock collector by default, no logs to tail unless you set SC_SERVER_LOG etc.
```

## Setting up the AdminClient collector (topics + consumer lag)

```bash
go get github.com/twmb/franz-go/pkg/kgo github.com/twmb/franz-go/pkg/kadm
go build ./...
```

If `go build` errors inside `internal/collector/admin.go`, it's almost
certainly a kadm API signature mismatch (this file was written without
being able to compile it against the real package) — paste the error and
it'll get fixed against whatever version `go get` resolved.

Run with all three collectors combined:
```bash
SC_USE_ADMIN_COLLECTOR=1 \
SC_JMX_TARGETS="http://localhost:7071/metrics,http://localhost:7072/metrics" \
SC_KAFKA_BROKERS=localhost:9092 \
SC_SERVER_LOG=/home/apache_kafka/kafka_2.13-4.3.1/logs/server.log \
SC_CONTROLLER_LOG=/home/apache_kafka/kafka_2.13-4.3.1/logs/controller.log \
SC_COLLECT_INTERVAL=10s \
./bin/connector
```
(`SC_USE_ADMIN_COLLECTOR=1` takes priority over `SC_USE_REAL_COLLECTOR=1`
and pulls in both JMX metrics and AdminClient topic/lag data.)

## Schema Registry metrics (Karapace)

Opt-in — set `SC_SCHEMA_REGISTRY_TARGETS` and it's automatically included
alongside whatever else is configured:

```bash
SC_SCHEMA_REGISTRY_TARGETS=http://localhost:18081/metrics \
SC_USE_ADMIN_COLLECTOR=1 \
... (other env vars as usual) \
./bin/connector
```

Multiple instances (Karapace's leader/replica HA setup): comma-separate,
same pattern as `SC_JMX_TARGETS`.

This collects: total requests, error requests (4xx/5xx), and schema
inventory counts (subjects, schemas, live/soft-deleted versions) —
straight from Karapace's own `/metrics`, no JMX involved since it's a
Python process. It does NOT collect actual schema content/versions —
that would need Karapace's REST API (`GET /subjects` etc.), which is a
separate, not-yet-built piece (this is metrics only, not full inventory).

## Kafka Connect worker metrics

```bash
# on the VM, one-time setup:
cat > /home/apache_kafka/kafka_2.13-4.3.1/config/jmx-exporter-connect.yml << 'EOF'
lowercaseOutputName: true
rules:
  - pattern: "kafka\\.connect.*"
EOF
# point Connect's KAFKA_OPTS javaagent at this file (see kafka-connect.service),
# systemctl daemon-reload && systemctl restart kafka-connect

SC_CONNECT_JMX_TARGETS=http://localhost:7073/metrics \
... (other env vars as usual) \
./bin/connector
```

Multiple workers (distributed mode, several nodes): comma-separate, same
pattern as `SC_JMX_TARGETS`/`SC_SCHEMA_REGISTRY_TARGETS`.

This collects worker-level state: connector/task counts, startup success/
failure counts, and rebalance status (epoch, currently-rebalancing,
time-since-last-rebalance). It does NOT yet collect per-connector status
(running/paused/failed) or per-task metrics — that needs Connect's REST
API (`GET /connectors`, `GET /connectors/{name}/status`) for status, and
verifying the connector-metrics/task-metrics JMX label shape against a
real running connector for the per-task numbers. Neither has been built
yet since no real connector has been running to verify against (only the
built-in MirrorMaker2 connectors are available in this Kafka install by
default — no file/JDBC/CDC connector plugins installed).

## Next real steps

1. Per-connector status (`GET /connectors/{name}/status`) and per-task
   JMX metrics — needs a real connector running to verify shapes against
   (deferred, see above).
2. Swap the HTTP/JSON Hub client for real gRPC streaming.
3. Add mTLS, local guardrail enforcement, credential custody.
4. Package as Helm chart / Docker image / RPM/DEB.

## Running as systemd services (recommended over manual `&` backgrounding)

Two unit files are in `deploy/systemd/`: one for the stub hub, one for the
connector (with all the env vars baked in). This assumes the repo lives
permanently at `/home/apache_kafka/site-connector` — adjust the paths in
both files first if yours is elsewhere.

```bash
# from inside the repo
mkdir -p logs
cp deploy/systemd/site-connector-hub.service /etc/systemd/system/
cp deploy/systemd/site-connector.service /etc/systemd/system/

systemctl daemon-reload
systemctl enable --now site-connector-hub
systemctl enable --now site-connector

# check status
systemctl status site-connector-hub
systemctl status site-connector

# logs
journalctl -u site-connector-hub -f
journalctl -u site-connector -f
# or, since they also append to files:
tail -f logs/stubhub.log logs/connector.log
```

Both are set to `Restart=on-failure`, so a crash (or a VM reboot, once
enabled) brings them back automatically — no more manually re-typing the
env-var command every time.

To change JMX targets, log paths, or collect interval later, edit the
`Environment=` lines in `/etc/systemd/system/site-connector.service`, then:
```bash
systemctl daemon-reload
systemctl restart site-connector
```

To stop everything:
```bash
systemctl stop site-connector site-connector-hub
```

## Dashboard

`dashboard.html` is a self-contained, no-build-step file that polls
stubhub's `/v1/state` endpoint every 3s and shows connector health,
broker/controller metrics, topics, consumer lag, and a live log stream.

**Where to open it:**
- On the VM itself (if it has a desktop/browser) — just open the file.
- From your own machine's browser — copy `dashboard.html` over, open it,
  and point the "Hub URL" field at `http://<VM_IP>:8081`. This requires
  your browser to actually reach that host/port (same network/VPN), and
  stubhub's CORS headers (already included) to allow the cross-origin fetch.

No install, no build — it's plain HTML/CSS/JS, tested against real
`/v1/state` payload shapes before shipping.
