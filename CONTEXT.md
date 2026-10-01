# Project Context: Direct API vs Kafka Load Test

> Read this first. It explains what this repository is for, the decisions already made and why, and what is still open.
> The architecture diagram is [`architecture.puml`](architecture.puml) (PlantUML deployment diagram).

## 1. Purpose

This is a **portfolio experiment** that will be shared publicly (LinkedIn, Threads, GitHub). It tests a common belief:

> "If an Order Service sends 100 concurrent requests directly to a Payment Service, the Payment Service will time out or error.
> If the Order Service publishes those 100 requests to a Kafka topic instead, and the Payment Service consumes them,
> errors will be lower and payment will be faster."

The goal is to **test the claim, not to prove Kafka is better**. The conclusion must be honest and nuanced, because the audience is engineers who will look for flaws in the method.

### Expected outcome (hypothesis, to be verified by the test)
- **"Fewer errors"**: expected to be **true under overload or outage**. Kafka buffers the burst and PS pulls at its own pace.
- **"Faster payment"**: expected to be **mostly false**. Kafka does not add processing capacity: if PS handles X payments/s, 100 payments take the same time either way, and Kafka adds a hop. What gets faster is **the response to the caller (OS)**, not the payment itself.
- When PS has **enough capacity**, the **direct call is expected to be equal or faster**. This scenario must be included, so the test is not rigged in Kafka's favour.
- Candidate headline: *"Kafka didn't make payments faster. It made them not fail."* Only use it if the data supports it.

## 2. System under test

| Abbr. | Component | Role |
|---|---|---|
| OS | Order Service | Receives orders from k6 and triggers payment |
| PS | Payment Service | Processes the payment by calling the gateway and writing the status. **The deliberate bottleneck.** |
| — | Payment Gateway Mock | Stands in for an external bank or gateway (Midtrans, Xendit, Stripe): 150–300 ms latency, 5% errors, can be switched off |
| KB | Kafka (KRaft mode, no ZooKeeper) | Message broker |
| DB | PostgreSQL | `orders_db` (owned by OS) and `payments_db` (owned by PS) |

**Why the gateway mock exists:** without a slow external call, PS only writes a DB row (~2 ms), never saturates, and all flows look identical. The external call is the realistic source of PS's bottleneck. A mock is used instead of a real sandbox because of rate limits, non-repeatable latency, no fault control, and terms-of-service concerns.

## 3. The three flows compared

Only **one flow is active per test run**, selected by config.

**Flow A: Direct HTTP (synchronous)**
```
k6 -> OS: POST /orders
OS -> PS: POST /payments  (OS blocks, client timeout 1 s)
PS -> Gateway: charge
PS -> payments_db: write status
PS -> OS: response with the result (OS does NOT need to query the DB afterwards)
```

**Flow B: Kafka + DB polling (the user's original idea, kept as a comparison)**
```
OS -> Kafka: publish payment.requested
PS <- Kafka: consume (pull at own pace)
PS -> Gateway: charge
PS -> payments_db: write status
OS -> payments_db: poll status repeatedly
```
Known weaknesses, which are part of what the test should show:
- The first poll almost always returns "pending".
- Polling adds heavy DB load (100 orders polling every 200 ms is ~500 queries/s).
- OS reading PS's database is a **shared-database anti-pattern** that couples the services.

**Flow C: Kafka + result event (the event-driven version)**
```
OS -> Kafka: publish payment.requested
OS -> caller: "order received, payment pending"
PS <- Kafka: consume
PS -> Gateway: charge
PS -> payments_db: write status
PS -> Kafka: publish payment.completed / payment.failed
OS <- Kafka: consume result -> update order in orders_db
```

Topics: `payment.requested` (N partitions), `payment.completed`, `payment.failed`, `payment.dlq`.

## 4. Infrastructure: 7 VMs on Google Cloud (Free Trial)

**Google Cloud, Free Trial account, region `asia-southeast2` (Jakarta), one VPC, private subnet**, spread across zones a and b. It is deliberately **not multi-cloud**: traffic between providers crosses the public internet, and the high latency and jitter would hide the architectural difference. It also adds egress cost and is not how production OS/PS/Kafka are deployed.

**Free-trial constraints drive the sizing:** the trial **cannot request quota increases** and is commonly capped at **8 vCPUs per region**. So the lab totals exactly 8 vCPUs, using **dedicated-core** types only. Shared-core e2-micro/small/medium burst unpredictably and would make results noisy.

| VM | Component | Machine type | Zone | Internal IP |
|---|---|---|---|---|
| VM1 | k6 load generator | n1-standard-1 (1 vCPU / 3.75 GB) | a | 10.0.0.11 |
| VM2 | Order Service | n1-standard-1 | a | 10.0.0.12 |
| VM3 | Payment Service | n1-standard-1 | b | 10.0.0.13 |
| VM4 | Payment Gateway Mock | n1-standard-1 | b | 10.0.0.14 |
| VM5 | Kafka (KRaft) | n1-standard-1 | a | 10.0.0.15 |
| VM6 | PostgreSQL (`orders_db`, `payments_db`) | n1-standard-1 | b | 10.0.0.16 |
| VM7 | Observability: `grafana/otel-lgtm` | e2-standard-2 (2 vCPU / 8 GB) | b | 10.0.0.17 |

**PS's bottleneck is a concurrency limit, not CPU:** at most **10 in-flight payments** in every flow (gateway client pool = DB pool = Kafka consumer concurrency = 10; `payment.requested` has 10 partitions). With 150–300 ms gateway latency, that's **≈ 44 payments/s** capacity. Load levels are derived from this number.

Networking: no public IPs, outbound through **Cloud NAT**, SSH and the Grafana tunnel through **IAP**, operated from **Cloud Shell**. The full runbook is in [`load_test_execution_plan_use_gcp.md`](load_test_execution_plan_use_gcp.md).

**Why every component has its own VM:**
- **Fairness:** if OS and PS shared a VM, the direct flow would skip the real network hop while the Kafka flows would not. That would bias the result towards Flow A.
- **Bottleneck attribution:** no CPU contention between services and the DB.
- **Independent failure:** "PS down" means stopping only PS.
- **Clean per-component metrics** for dashboards and screenshots.
- **The load generator is always isolated,** so it never steals CPU from the system under test.

Provisioning uses **gcloud CLI commands from Cloud Shell** (see the execution plan); Terraform is a possible later upgrade. Each VM gets a startup script that installs Docker, and every component runs via Docker Compose. Expected cost is about **$0.5 per running hour**, roughly $5–10 for the whole test, well within the $300 credit.

## 5. Test design

**Scenarios, run for each flow** (k6 **arrival-rate** executors, to avoid coordinated omission):

| ID | Scenario | Load |
|---|---|---|
| S1 | PS copes | 20 req/s × 60 s (~45% of capacity). Direct is expected to tie or win |
| S2 | **Burst of 100** (the original question) | 100 VUs × 1 iteration, all at once |
| S3 | PS overloaded | 100 req/s × 60 s (~2.3× capacity) |
| S4 | PS down 10 s | 20 req/s × 60 s, PS stopped at t=20 s |
| S5 | Ramp to breaking point | 10 → 150 req/s over 3 min |

OS settings: in flow A, a 1 s timeout to PS with no automatic retry; in flow B, polling every 200 ms, giving up after 30 s.
Every run starts with a reset (truncate tables, recreate topics, restart services, 30 s cool-down). Flows are interleaved across runs.
**Validity checks:** k6 CPU < 70%, OS CPU < 80%, gateway CPU < 70%, k6 `dropped_iterations` = 0. Otherwise the run is invalid.

Discussed as optional extras: Kafka with 1 partition vs N partitions (partitions cap consumer parallelism), and duplicate delivery with vs without an idempotency key (at-least-once delivery can double-charge).

**Metrics:**
- **OS response time** (caller latency): p50, p95, p99.
- **End-to-end payment time** (request until status final), from DB timestamps `requested_at`, `started_at` and `completed_at`.
- **Queue wait time:** `started_at − requested_at`.
- All timestamps come from the **DB clock** (`clock_timestamp()`), with `requested_at` = `orders.created_at`, passed from OS to PS. One clock means no skew between VMs.
- **Inconsistent orders:** OS marked the order failed or timed out, but the payment actually succeeded (expected in flow A).
- Success, timeout and error counts; lost payments; total time for all N; duplicate charges.
- Kafka consumer lag over time, PS CPU, DB connections and query rate.

**Rigour rules:**
- Each scenario is repeated **3–5 times** and the **median** reported.
- Hypothesis and pass criteria are written **before** running.
- Instrumentation is identical across flows.
- **Source of truth:** k6 results plus DB timestamps. Observability is used to explain *why*.

**Observability:** OpenTelemetry → `grafana/otel-lgtm` (Tempo for traces, Loki for logs, Mimir for metrics, Grafana for dashboards). Trace context is propagated through Kafka headers so a single trace spans OS → Kafka → PS → DB.

## 6. Deliverables (for the public post)

- A public GitHub repo with Terraform plus a one-command run.
- A README write-up with full data.
- One headline chart: success rate and p95, direct vs Kafka, across load levels.
- A Grafana screenshot of Kafka consumer lag absorbing the burst.
- A Tempo trace waterfall: queue wait vs processing, direct timeout vs Kafka.
- The architecture diagram (`architecture.puml`).
- LinkedIn post (short story plus 1–3 images, linking to the write-up) and a Threads thread (belief → test → surprise → takeaway).

## 7. Current status

- **Phase: design.** No service code for the Kafka plan has been written yet.
- `architecture.puml` is **final for the 7-VM layout**. It renders cleanly and is ASCII-only, to avoid encoding problems.
- **Obsolete files:** `src/`, `docker-compose.yml` and `package.json` are an earlier **RabbitMQ + Node.js** draft, written before the design discussion. They no longer match the plan and should be deleted or replaced. They were never run.
- `ai_chat.md` is the user's scratch notes file.

## 8. Open decisions (ask the user; do not assume)

1. **Language/framework** for OS, PS and the gateway mock: Node.js, Java/Spring Boot or Go. Preferably the user's work stack.
2. **Region is confirmed only after the quota check:** `asia-southeast2` is the plan, but Phase 0.4 of the execution plan may force another region if the free-trial quota or machine types don't fit.
3. **Scope:** the full 3 flows × 5 scenarios × 3–5 runs, or start smaller.
4. Whether to delete the obsolete RabbitMQ draft files.

Decided: **Google Cloud Free Trial** (2026-10-01). Budget is the $300 trial credit, with a $30 budget alert.

## 9. Working agreements with the user

- **Discuss the approach and give your point of view before writing any code.** Wait for agreement. The user explicitly stopped premature coding once.
- Explain concepts plainly (the user asked what "jitter" means). Tables and short examples work well.
- Be honest about trade-offs, including where Kafka is worse (extra latency, extra infrastructure, duplicate delivery, async result handling).
- Keep public-facing claims backed by the data.
