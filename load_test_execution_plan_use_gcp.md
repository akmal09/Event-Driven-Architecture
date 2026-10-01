# Execution Plan: Direct API vs Kafka Load Test on GCP (Free Trial, 7 VMs)

Step-by-step guide to provision the 7-VM lab on a **Google Cloud Free Trial** account, run the test matrix, collect the results and tear everything down.

- Architecture diagram: [`architecture.puml`](architecture.puml). It is the single source of truth for the topology; this plan does not duplicate it.
- Project background and decisions: [`CONTEXT.md`](CONTEXT.md)

---

## What changed from the previous version, and why

| # | Change | Why |
|---|---|---|
| 1 | Total resized to **8 vCPUs** | The free trial caps vCPUs (reported as 8 per region) and **cannot request quota increases**. The old plan needed ~13–15 vCPUs |
| 2 | **Dedicated-core** machine types only (`n1-standard-1`, `e2-standard-2`) | Shared-core `e2-micro/small/medium` burst unpredictably (e2-small guarantees only ~0.5 vCPU), which makes benchmark results noisy and unrepeatable |
| 3 | Added **Cloud Router + Cloud NAT** | VMs have no public IP, so without NAT they cannot install Docker or pull images |
| 4 | Added **project setup, quota check and budget alert** (Phase 0) | Avoids failing halfway through, or unexpected spend |
| 5 | **Fixed internal IPs**, pinned OS image, `pd-balanced` disks | Kafka's advertised listener and service configs stay stable; disk speed doesn't skew Kafka or Postgres |
| 6 | **Startup script + Docker Compose** per VM instead of manual installs | Repeatable and faster; less time paying for idle VMs |
| 7 | **OTel Collector agent** on every VM | Kafka and Postgres don't emit OTLP natively; consumer lag and DB stats need collector receivers |
| 8 | Load **scaled to fit 1-vCPU services**, using **arrival-rate** executors | Avoids coordinated omission, which hides overload in the direct flow; keeps OS and k6 from becoming the bottleneck instead of PS |
| 9 | **End-to-end time from DB timestamps** (single DB clock) | In flows B and C, k6 only sees the "accepted" response, not payment completion |
| 10 | **Reset step, run matrix, JSON export, validity checks** | Clean, repeatable runs with machine-readable results |
| 11 | Expected results rewritten as **hypotheses** | Avoids confirmation bias in a public write-up |
| 12 | Added **teardown** (Phase 5) | Stops the free-trial credit draining |

---

## Phase 0: Prerequisites and free-trial constraints

### 0.1 Free-trial limits that shape this plan

| Limit | Impact |
|---|---|
| $300 credit, **90 days** | Plenty for this test (estimate below); finish within 90 days |
| **No quota increase requests** | Everything must fit the default quota |
| vCPU cap (commonly **8 per region**) | The whole lab is sized to exactly **8 vCPUs** |
| No GPUs, no Marketplace, no Windows images | Not needed |
| Credit runs out or trial ends → resources stopped, data marked for deletion | Keep results in the repo, not only on VMs |

### 0.2 Use Google Cloud Shell

All `gcloud` commands below are **bash**. Run them in **Cloud Shell** (the `>_` icon in the Cloud Console). It is free, already authenticated, and has `gcloud` installed, so you don't need to install anything on Windows.

### 0.3 Project setup

```bash
export PROJECT_ID="your-project-id"      # <- change this
export REGION="asia-southeast2"          # Jakarta
export ZONE_A="asia-southeast2-a"
export ZONE_B="asia-southeast2-b"

gcloud config set project "$PROJECT_ID"
gcloud config set compute/region "$REGION"

gcloud services enable compute.googleapis.com iap.googleapis.com
```

As the project **Owner** you already have IAP tunnel permission. If you use another account, grant it `roles/iap.tunnelResourceAccessor`.

### 0.4 Check quota and machine availability BEFORE creating anything

```bash
# Regional CPU quota (need limit >= 8, and usage 0)
gcloud compute regions describe "$REGION" \
  --flatten="quotas[]" --format="table(quotas.metric,quotas.limit,quotas.usage)" \
  | grep -E "^CPUS |N1_CPUS|E2_CPUS|IN_USE_ADDRESSES|SSD_TOTAL_GB"

# Global CPU quota (need limit >= 8)
gcloud compute project-info describe \
  --flatten="quotas[]" --format="table(quotas.metric,quotas.limit,quotas.usage)" \
  | grep CPUS_ALL_REGIONS

# Machine types offered in both zones
gcloud compute machine-types list \
  --zones="$ZONE_A,$ZONE_B" \
  --filter="name=(n1-standard-1 e2-standard-2)" \
  --format="table(name,zone,guestCpus,memoryMb)"
```

**Stop here** if `CPUS` or `CPUS_ALL_REGIONS` is below 8, or a machine type is missing in a zone. Then pick another zone or region (e.g. `asia-southeast1`, Singapore) and re-run.

### 0.5 Budget alert (do this once)

Cloud Console → **Billing → Budgets & alerts → Create budget** → amount **$30**, alerts at 50% / 90% / 100%.
This is an email alert only. It does not stop resources, which is why Phase 5 (teardown) matters.

---

## Phase 1: Infrastructure provisioning

### 1.1 Sizing that fits 8 vCPUs

| VM | Role | Machine type | vCPU | RAM | Zone | Internal IP |
|---|---|---|---|---|---|---|
| vm1-loadgen | k6 | `n1-standard-1` | 1 | 3.75 GB | A | 10.0.0.11 |
| vm2-order | Order Service | `n1-standard-1` | 1 | 3.75 GB | A | 10.0.0.12 |
| vm3-payment | Payment Service | `n1-standard-1` | 1 | 3.75 GB | B | 10.0.0.13 |
| vm4-gateway | Payment Gateway Mock | `n1-standard-1` | 1 | 3.75 GB | B | 10.0.0.14 |
| vm5-kafka | Kafka (KRaft) | `n1-standard-1` | 1 | 3.75 GB | A | 10.0.0.15 |
| vm6-database | PostgreSQL | `n1-standard-1` | 1 | 3.75 GB | B | 10.0.0.16 |
| vm7-observability | grafana/otel-lgtm | `e2-standard-2` | 2 | 8 GB | B | 10.0.0.17 |
| **Total** | | | **8** | | | |

**Design notes:**
- Every VM has a **dedicated** vCPU, so there's no burst noise.
- **PS's bottleneck is its concurrency limit, not its CPU.** PS allows at most **10 in-flight payments** (gateway HTTP client pool = 10, DB pool = 10, Kafka consumer concurrency = 10) in **every flow**. With gateway latency 150–300 ms (avg ~225 ms), PS capacity is about **10 / 0.225 s ≈ 44 payments/s**. This is the number the scenarios are built around.
- k6 on 1 vCPU comfortably handles the ≤150 req/s used here. Its CPU is checked on every run (Phase 3.5).
- Observability gets the extra vCPU. Grafana, Tempo, Loki and Mimir in one container are the heaviest component, and they are outside the system under test.

### 1.2 VPC, subnet, firewall

```bash
gcloud compute networks create loadtest-vpc \
  --subnet-mode=custom --bgp-routing-mode=regional

gcloud compute networks subnets create loadtest-subnet \
  --network=loadtest-vpc --region="$REGION" --range=10.0.0.0/16

# All traffic between lab VMs
gcloud compute firewall-rules create loadtest-allow-internal \
  --network=loadtest-vpc --direction=INGRESS --action=ALLOW \
  --source-ranges=10.0.0.0/16 --rules=all

# SSH only through Google IAP (no public SSH)
gcloud compute firewall-rules create loadtest-allow-iap-ssh \
  --network=loadtest-vpc --direction=INGRESS --action=ALLOW \
  --source-ranges=35.235.240.0/20 --rules=tcp:22
```

### 1.3 Cloud NAT (outbound internet for VMs without public IPs)

```bash
gcloud compute routers create loadtest-router \
  --network=loadtest-vpc --region="$REGION"

gcloud compute routers nats create loadtest-nat \
  --router=loadtest-router --region="$REGION" \
  --auto-allocate-nat-external-ips --nat-all-subnet-ip-ranges
```

### 1.4 Startup script (installs Docker on first boot)

```bash
cat > startup.sh <<'EOF'
#!/bin/bash
set -euo pipefail
if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sh
  systemctl enable --now docker
fi
EOF
```

### 1.5 Create the 7 VMs

```bash
create_vm () {
  local name=$1 zone=$2 ip=$3 type=$4
  gcloud compute instances create "$name" \
    --zone="$zone" --machine-type="$type" \
    --network=loadtest-vpc --subnet=loadtest-subnet \
    --private-network-ip="$ip" --no-address \
    --image-family=debian-12 --image-project=debian-cloud \
    --boot-disk-size=20GB --boot-disk-type=pd-balanced \
    --metadata-from-file=startup-script=startup.sh \
    --labels=project=eda-loadtest
}

# Create infrastructure dependencies first
create_vm vm6-database      "$ZONE_B" 10.0.0.16 n1-standard-1
create_vm vm5-kafka         "$ZONE_A" 10.0.0.15 n1-standard-1
create_vm vm7-observability "$ZONE_B" 10.0.0.17 e2-standard-2
create_vm vm4-gateway       "$ZONE_B" 10.0.0.14 n1-standard-1
create_vm vm3-payment       "$ZONE_B" 10.0.0.13 n1-standard-1
create_vm vm2-order         "$ZONE_A" 10.0.0.12 n1-standard-1
create_vm vm1-loadgen       "$ZONE_A" 10.0.0.11 n1-standard-1

# Verify
gcloud compute instances list --filter="labels.project=eda-loadtest" \
  --format="table(name,zone.basename(),machineType.basename(),networkInterfaces[0].networkIP,status)"
```

Check that Docker installed (about 1–2 min after boot):
```bash
gcloud compute ssh vm6-database --zone="$ZONE_B" --tunnel-through-iap --command="docker --version"
```

---

## Phase 2: Application deployment

Every VM runs its component with **Docker Compose**. The repo will hold one folder per VM, `deploy/<vm-name>/compose.yaml` plus a `.env`. *(These files are created in the implementation phase. This section defines what each one must contain.)*

Copy and start each one like this:
```bash
gcloud compute scp --recurse deploy/vm6-database vm6-database:~ --zone="$ZONE_B" --tunnel-through-iap
gcloud compute ssh vm6-database --zone="$ZONE_B" --tunnel-through-iap \
  --command="cd vm6-database && sudo docker compose up -d"
```

Deploy in this order: **VM6 → VM5 → VM7 → VM4 → VM3 → VM2 → VM1.**

### 2.1 Every VM: OTel Collector agent
Container `otel/opentelemetry-collector-contrib`, exporting OTLP to `10.0.0.17:4317`.
- `hostmetrics` receiver (CPU, memory, network) on **all** VMs
- VM5 adds the `kafkametrics` receiver (**consumer lag**, topic offsets)
- VM6 adds the `postgresql` receiver (**connections**, commits, rows)
- VM2/VM3/VM4 apps export their own traces, metrics and logs to the local agent (`localhost:4317`)

### 2.2 VM6: PostgreSQL
- Image `postgres:16`. `listen_addresses='*'`; `pg_hba.conf` allows `10.0.0.0/16`.
- Databases `orders_db` (owned by role `os`) and `payments_db` (owned by role `ps`).
- **All timestamps come from the DB clock** (`clock_timestamp()`), so latency math uses a single clock:

```sql
-- orders_db
CREATE TABLE orders (
  id          BIGINT PRIMARY KEY,
  status      TEXT NOT NULL,                       -- CREATED / PAID / PAYMENT_FAILED / PAYMENT_TIMEOUT
  created_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at  TIMESTAMPTZ
);

-- payments_db
CREATE TABLE payments (
  order_id      BIGINT PRIMARY KEY,                -- also the idempotency key
  status        TEXT NOT NULL,                     -- PROCESSING / SUCCESS / FAILED
  attempts      INT  NOT NULL DEFAULT 1,
  requested_at  TIMESTAMPTZ NOT NULL,              -- = orders.created_at, passed in by OS
  started_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  completed_at  TIMESTAMPTZ
);
```

OS reads `created_at` back with `INSERT ... RETURNING created_at` and passes it to PS in the HTTP body or Kafka message. That way `requested_at`, `started_at` and `completed_at` all come from the same clock.

### 2.3 VM5: Kafka (KRaft, single broker)
Image `apache/kafka` (pin a version). Key settings:

| Setting | Value |
|---|---|
| `KAFKA_PROCESS_ROLES` | `broker,controller` |
| `KAFKA_LISTENERS` | `PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093` |
| `KAFKA_ADVERTISED_LISTENERS` | `PLAINTEXT://10.0.0.15:9092` ← the fixed IP is why we pinned it |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `1@localhost:9093` |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | `1` |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `false` |
| `KAFKA_HEAP_OPTS` | `-Xms1g -Xmx1g` (fits 3.75 GB) |

Topics (replication factor 1 because there's a single broker; state this in the write-up):
```bash
for t in payment.requested payment.completed payment.failed payment.dlq; do
  sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$t" --partitions 10 --replication-factor 1
done
```
10 partitions on `payment.requested` lets PS reach its concurrency limit of 10, the same as in flow A.

### 2.4 VM7: Observability
```bash
sudo docker run -d --name lgtm --restart unless-stopped \
  -p 3000:3000 -p 4317:4317 -p 4318:4318 grafana/otel-lgtm
```
Use **trace sampling of 20%** (parent-based ratio) in the apps, so observability on 2 vCPUs keeps up. Metrics are not sampled.

### 2.5 VM4: Payment Gateway Mock
Small custom HTTP service on `:8082`: random delay **150–300 ms**, **5% HTTP 500**, plus admin endpoints to change the delay or error rate (for a "bank is slow" scenario). A custom mock is preferred over WireMock because random percentage failures and runtime toggles are simpler.

### 2.6 VM3: Payment Service
- `FLOW=A|B|C`
- `MAX_CONCURRENCY=10` (gateway client pool = DB pool = consumer concurrency = 10)
- DB `10.0.0.16`, Kafka `10.0.0.15:9092`, gateway `10.0.0.14:8082`
- **Idempotent on `order_id`**, because Kafka delivers at least once

### 2.7 VM2: Order Service
- `FLOW=A|B|C`
- Flow A: HTTP client timeout to PS **1 s**, **no automatic retry** (record failures as they are)
- Flow B: poll `payments_db` every **200 ms**, give up after **30 s**
- Flow C: consume `payment.completed` / `payment.failed` and update `orders.status`

### 2.8 VM1: k6
Run through Docker (`grafana/k6`, pinned version) with the scenario scripts in `loadtest/`. Export metrics with k6's OpenTelemetry output to `10.0.0.17:4317` (the flag name depends on the k6 version: `experimental-opentelemetry` in older releases).

---

## Phase 3: Executing the load test

### 3.1 Scenarios (derived from PS capacity ≈ 44 payments/s)

| ID | Scenario | k6 executor | Load | Purpose |
|---|---|---|---|---|
| S1 | **PS copes** | `constant-arrival-rate` | 20 req/s × 60 s (~45% capacity) | Baseline: direct is expected to tie or win |
| S2 | **Burst of 100** | `per-vu-iterations` | 100 VUs × 1 iteration, all at once | **The original question** |
| S3 | **PS overloaded** | `constant-arrival-rate` | 100 req/s × 60 s (~2.3× capacity) | Sustained overload |
| S4 | **PS down 10 s** | `constant-arrival-rate` | 20 req/s × 60 s; PS stopped at t=20 s for 10 s | Outage behaviour |
| S5 | **Ramp to breaking point** | `ramping-arrival-rate` | 10 → 150 req/s over 3 min | Find where each flow breaks |

Arrival-rate executors keep sending at the target rate even when responses slow down. This is how real users behave, and it avoids **coordinated omission**, which would make the direct flow's overload look milder than it is.

S4 PS outage, triggered from Cloud Shell while k6 runs:
```bash
sleep 20 && gcloud compute ssh vm3-payment --zone="$ZONE_B" --tunnel-through-iap \
  --command="sudo docker stop payment-service && sleep 10 && sudo docker start payment-service"
```

### 3.2 Run matrix

3 flows × 5 scenarios × **3 runs** = **45 runs**. Use 5 runs for S2 and S3, which are the headline numbers.
At ~3–4 min per run including reset and drain, that's about **3–4 hours**, which can be split over several sessions.

Run order: interleave the flows (A, B, C, A, B, C, …) rather than doing all of A first. This way any drift over time (e.g. noisy neighbours) affects every flow equally.

### 3.3 Before every run: reset

`scripts/reset.sh` (implementation phase) must:
1. Stop the OS and PS containers.
2. `TRUNCATE orders;` and `TRUNCATE payments;`
3. Delete and recreate the 4 Kafka topics, which also resets consumer offsets.
4. Set `FLOW=<A|B|C>` in VM2/VM3 `.env` and start OS and PS.
5. Wait until OS and PS health checks pass and the PS consumer has joined its group (flows B and C).
6. Cool down for 30 s.

### 3.4 Each run

```bash
# on vm1-loadgen
RUN_ID="flowA-S2-run1"
sudo docker run --rm --network host -v "$PWD/loadtest:/scripts" -v "$PWD/results:/results" \
  grafana/k6 run --summary-export="/results/$RUN_ID.json" /scripts/s2-burst.js
```

All `psql` commands run **from vm1** through a throwaway client container. `\copy` writes on the client side, so the CSVs land in vm1's `results/` folder:
```bash
psql_vm6 () {  # usage: psql_vm6 <db> <user> <sql>
  sudo docker run --rm --network host -e PGPASSWORD="$PGPASSWORD" -v "$PWD/results:/results" \
    postgres:16 psql -h 10.0.0.16 -d "$1" -U "$2" -At -c "$3"
}
```

Then **wait for the drain**: flows B and C keep processing after k6 finishes. The run is done when every order has a final payment status, or after a 60 s cap:
```bash
# In flows B/C a payment row only exists once PS consumes the message,
# so compare finished payments against the number of orders, not just NULL rows.
orders=$(psql_vm6 orders_db os "SELECT count(*) FROM orders")
for i in $(seq 1 60); do
  done_n=$(psql_vm6 payments_db ps "SELECT count(*) FROM payments WHERE completed_at IS NOT NULL")
  [ "$done_n" -ge "$orders" ] && break; sleep 1
done
echo "orders=$orders finished_payments=$done_n"   # a gap here = lost or stuck payments
```

Finally **export the raw data** for analysis:
```bash
psql_vm6 orders_db   os "\copy orders   TO '/results/$RUN_ID-orders.csv'   CSV HEADER"
psql_vm6 payments_db ps "\copy payments TO '/results/$RUN_ID-payments.csv' CSV HEADER"
```

Copy results back after each session:
```bash
gcloud compute scp --recurse vm1-loadgen:~/results ./results --zone="$ZONE_A" --tunnel-through-iap
```

### 3.5 Validity checks (the run is invalid if any fails)

| Check | Threshold | Why |
|---|---|---|
| vm1 (k6) CPU | < 70% | Otherwise the load generator is the bottleneck |
| vm2 (OS) CPU | < 80% | Otherwise OS, not PS, is the bottleneck |
| vm4 (gateway) CPU | < 70% | The "external bank" must not be the limit |
| k6 `dropped_iterations` | 0 | k6 actually sent the intended load |
| Orders in DB = requests sent by k6 | equal | No requests lost before reaching OS |

---

## Phase 4: Viewing and analysing results

### 4.1 Grafana

**From Cloud Shell (no local install):**
```bash
gcloud compute ssh vm7-observability --zone="$ZONE_B" --tunnel-through-iap -- -N -L 8080:localhost:3000
```
Then use Cloud Shell **Web Preview → Preview on port 8080**.

**From your laptop** (needs the gcloud CLI installed locally): the same command with `-L 3000:localhost:3000`, then open `http://localhost:3000`.

### 4.2 Metrics per run

| Metric | Source |
|---|---|
| **OS response time** p50/p95/p99 | k6 `http_req_duration` |
| Caller-visible errors and timeouts | k6 `http_req_failed` + status codes |
| **End-to-end payment time** p50/p95/p99 | `payments.completed_at − requested_at` |
| **Queue wait** | `payments.started_at − requested_at` |
| Success / failed / **never completed (lost)** | `payments.status`, orders without a payment |
| **Inconsistent orders**: order marked FAILED/TIMEOUT but payment SUCCESS | join `orders.csv` and `payments.csv` on order id |
| Time to finish all payments | `max(completed_at) − min(requested_at)` |
| Kafka consumer lag over time | Grafana (kafkametrics) |
| DB connections and query rate | Grafana (postgresql receiver) |
| Retries / duplicates | `payments.attempts`, DLQ count |

Quick end-to-end summary (in `payments_db`):
```sql
SELECT status,
       count(*) AS n,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY e2e_ms)) AS p50_ms,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY e2e_ms)) AS p95_ms,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY e2e_ms)) AS p99_ms,
       round(avg(wait_ms))                                         AS avg_queue_wait_ms
FROM (
  SELECT status,
         extract(epoch FROM completed_at - requested_at) * 1000 AS e2e_ms,
         extract(epoch FROM started_at   - requested_at) * 1000 AS wait_ms
  FROM payments
) t
GROUP BY status;
```

Report the **median across runs** for each flow × scenario, plus the min–max spread.

### 4.3 Hypotheses to verify (not results)

| ID | Hypothesis | Confirmed if |
|---|---|---|
| H1 | In **S1**, flow A has **equal or lower** end-to-end p95 than B and C | A's p95 ≤ C's p95 |
| H2 | In **S2/S3**, flow A shows timeouts/errors; B and C lose **0** payments | A's error rate > 0, B/C lost = 0 |
| H3 | Kafka does **not** make payments finish faster: total time to finish all payments is about the same | Within ~10% across flows |
| H4 | B and C give a much lower **OS response time** than A under overload | OS p95: B/C ≪ A |
| H5 | Flow B produces far more DB queries than C, for no benefit | Query rate B ≫ C, e2e similar |
| H6 | In **S4**, A fails during the outage; B/C recover with 0 lost | A errors only during the window; B/C lag spikes then drains |
| H7 | Flow A produces **inconsistent orders** (OS timed out, payment succeeded) | Count > 0 in S2/S3 |

Write the actual outcome next to each hypothesis, **including the ones that turn out false**.

---

## Phase 5: Teardown (do this after every session)

**Pause between sessions** (keeps disks, stops vCPU billing):
```bash
for z in "$ZONE_A" "$ZONE_B"; do
  names=$(gcloud compute instances list --filter="labels.project=eda-loadtest AND zone:$z" --format="value(name)")
  [ -n "$names" ] && gcloud compute instances stop $names --zone="$z"
done
```

**Full teardown** (after the final session; copy `results/` out first):
```bash
for z in "$ZONE_A" "$ZONE_B"; do
  names=$(gcloud compute instances list --filter="labels.project=eda-loadtest AND zone:$z" --format="value(name)")
  [ -n "$names" ] && gcloud compute instances delete $names --zone="$z" --quiet
done
gcloud compute routers nats delete loadtest-nat --router=loadtest-router --region="$REGION" --quiet
gcloud compute routers delete loadtest-router --region="$REGION" --quiet
gcloud compute firewall-rules delete loadtest-allow-internal loadtest-allow-iap-ssh --quiet
gcloud compute networks subnets delete loadtest-subnet --region="$REGION" --quiet
gcloud compute networks delete loadtest-vpc --quiet

# Confirm nothing is left
gcloud compute instances list --filter="labels.project=eda-loadtest"
```

### Cost estimate (verify in the GCP Pricing Calculator; Jakarta is pricier than US regions)

| Item | Approx. per hour |
|---|---|
| 6 × `n1-standard-1` | ~$0.30–0.40 |
| 1 × `e2-standard-2` | ~$0.08–0.10 |
| Cloud NAT gateway + data | ~$0.05 |
| 7 × 20 GB `pd-balanced` | ~$0.02 (also charged while VMs are stopped) |
| **Total** | **~$0.5 per running hour** |

The full matrix (~4 h) plus setup and debugging (~6 h) comes to roughly **$5–10**, well within the $300 credit.

---

## Implementation checklist (what still has to be built)

- [ ] `deploy/<vm>/compose.yaml` + `.env` for each of the 7 VMs, including the OTel agent configs
- [ ] Order Service, Payment Service, Gateway Mock (language still to be decided)
- [ ] DB schema migration (Phase 2.2)
- [ ] k6 scripts `loadtest/s1-copes.js` … `s5-ramp.js`
- [ ] `scripts/reset.sh`, `scripts/run.sh` (reset → run → drain → export)
- [ ] Analysis script: CSVs → summary tables + charts
- [ ] Rehearse everything **locally with Docker Compose first** (same images, same configs) before spending cloud credit
