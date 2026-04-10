# Market Data Aggregator

**A production-style, multi-exchange market data engine in Rust:** live order books from several cryptocurrency venues, normalized into one domain model, queryable from a terminal CLI, with structured tracing persisted to ScyllaDB.

## Project overview

**What it is.** A workspace of crates that connects to public WebSocket and REST market-data APIs, keeps in-memory L2 books per exchange and instrument, and exposes cross-venue views (best bid/ask, spreads, aggregated depth) through an interactive CLI.

**What it is for.** Comparing how the same instrument trades across venues, inspecting liquidity and microstructure in real time, and serving as a reference for how to handle flaky networks, inconsistent APIs, and snapshot/update synchronization without sacrificing clarity or observability.


---

## Architecture

### Data flow

At runtime, data moves in one direction for market events and the opposite for control/resync. The diagram below is the mental model:

![MarketDataAggregator Data_flow](https://github.com/PietroValente/MarketDataAggregator/blob/main/images/Data_flow.png)

The system ingests real-time market data from multiple exchanges via WebSocket streams and REST snapshots.
Each exchange is handled independently through an async Connector, responsible for managing connections, subscriptions, and reconnections.

Incoming data is sent through mpsc channels to a dedicated Adapter (Parser + Sync) running on its own thread. The adapter normalizes exchange-specific messages and ensures correct synchronization between snapshots and incremental updates.

All normalized events are forwarded through a central engine message channel to a single-threaded Engine, which owns the global state. The engine maintains per-exchange state and updates the in-memory instrument order books, ensuring consistency without shared mutable state or locking.
In addition, the engine performs data integrity checks (e.g. checksum validation) to detect inconsistencies or data loss and trigger resynchronization when needed.

A separate CLI thread interacts with the engine via oneshot channels, allowing synchronous queries (e.g. best bid/ask, spreads) without blocking the data pipeline.

This design leverages message passing over shared state, providing strong isolation, predictable concurrency, and scalability across multiple exchanges.

### Core components

| Crate / area | Role |
|--------------|------|
| **`app`** | Loads `config/config.toml`, initializes tracing + DB logging, spawns connectors (Tokio), adapters and engine (`std::thread`), and runs the query CLI until exit. |
| **`md_core`** | Shared types (`Instrument`, `Exchange`, book primitives), `NormalizedEvent` / `EngineMessage` / `ControlEvent`, `ExchangeConnector` and `ExchangeAdapter` traits, connector helpers (reader/writer tasks, ping/pong), logging types and Scylla writer. |
| **`engine`** | Single long-lived consumer of `EngineMessage`: applies snapshots and updates per exchange, serves queries via `oneshot` replies, triggers **resync** when consistency checks fail. |
| **`query`** | Interactive CLI: live views and one-shot commands (list, search, status). |
| **`exchanges/*`** | One crate per venue: connector (trait `ExchangeConnector`), **stateful** adapter (trait `ExchangeAdapter`), venue-specific types and JSON parsing. |

### Connectors

The connector is the **async** side of each exchange: everything that touches sockets, HTTP for discovery and snapshots, and backoff lives here, so the rest of the pipeline never blocks on the network.

![MarketDataAggregator Connector](https://github.com/PietroValente/MarketDataAggregator/blob/main/images/Connector.png)

The figure above lines up with `md_core::connector::tasks` and the per-venue connector types. A **`control_manager_task`** subscribes to **`ControlEvent`** from the engine—for instance **`Resync`** when a book can no longer be trusted. On resync it injects **`InboundEvent::ClearBookState`** (so the connector’s main loop can forward a **reset** to the adapter on the **raw** channel) and sends **`ManagerCommand::RecreateWithSnapshots`** so the **connection manager** tears down sockets, reconnects, and runs **snapshot-driven** recovery where the exchange supports it. Separately, the **`connection_manager_task`** owns **all WebSockets** for that venue: each stream is split into a **reader** and a **writer**. Readers enqueue **text/binary frames**, **pings**, and **closes/errors** as **`InboundEvent`s**; writers dequeue **`WriteCommand`s** (subscriptions, **pongs**, other raw **`Message`s**) so the socket is touched from one place only. The connector’s **`start`** loop consumes that inbound stream and maps it to the venue **`mpsc`** the adapter reads—market payloads as wire messages, **`ResetBookState`** when resyncing, **`pong`** routed back through the manager as **`ManagerCommand::Pong`**, and **`ConnectionClosed`** triggering a controlled resubscribe.

Subscriptions are often **spread across several connections** when **`max_subscription_per_ws`** caps how many symbols fit on one socket; REST **`exchange_info`** (and venue-specific snapshot URLs, e.g. Binance depth) feed planning and recovery. Retries use **exponential backoff** so transient failures do not become a reconnect storm.

Each exchange implements **`ExchangeConnector`** (`md_core::traits::connector`): describe how to build batches, subscribe, and **`start`** this graph. That boundary matches the **Data flow** picture: **connector = async I/O and connection lifecycle**, **adapter = synchronous parsing and per-instrument sync** on its own thread.

### Engine

The **`Engine`** owns `HashMap<Exchange, ExchangeState>` and processes:

- **`EngineMessage::Apply`**: normalized status and book events (`BookEventType::Snapshot` / `Update`). On validation errors that imply a corrupt or gap-filled book (e.g. checksum / sequence semantics), it issues **resync** for that exchange only.
- **`EngineMessage::Query`**: top-of-book, lists, cross-venue best and spread, aggregated depth, exchange status—each response is returned on a **`oneshot`** channel.

There is **no** shared `Arc<Mutex<…>>` around the books: the engine thread is the sole mutator of consolidated state.

### Adapters (from parsers to stateful components)

Early designs used **stateless parsers**: turn a frame into a normalized event and forward it. That breaks down as soon as you need **per-instrument sync state**—for example last applied update id, buffering depth updates until a REST snapshot arrives, or venue-specific sequence rules. Those concerns do not belong in a pure parse function, so the codebase uses **`ExchangeAdapter`** instead: a long-lived loop that **owns** that state.

Each exchange crate still defines **wire-format types** and deserializes JSON/text into them. The adapter then:

- Updates **internal maps** (e.g. per-`Instrument` book sync status, pending updates, counters).
- **`validate_snapshot` / `validate_update`** (`md_core::traits::adapter`) enforce continuity before anything reaches the engine.
- Emits **`NormalizedEvent`** inside `EventEnvelope` on the engine channel, and reports **initialization status** so the CLI can show progress.

Low-level deserialization remains local to each venue; the **named component** in the architecture is the adapter, not a separate parser crate.

Shared logic (sending normalized events and status with correct exchange tags, clearing book state on resync helpers, etc.) lives in **`md_core::helpers::adapter`** as generic helpers over `ExchangeAdapter`, which limits duplication across venues without hiding the fact that each adapter is **stateful**.

### Concurrency model

The design favors **message passing over locking**:

- **`tokio::sync::mpsc`** for pipelines: raw messages, normalized engine messages, control to connectors, log events.
- **`tokio::sync::oneshot`** for request/response (queries and other one-shot replies).

**Effects:**

- **Actor-like boundaries.** Connectors, adapters, engine, and query UI each consume their own inbox; hot paths do not pay hidden mutex contention.
- **Backpressure.** Bounded channels make overload visible: tuning buffer sizes is a deliberate trade-off instead of an emergent lock fight.
- **Isolation per exchange.** A slow or broken connector affects mostly that venue’s streams; others keep progressing.
- **Hybrid runtime.** Connectors use **Tokio**; engine, adapters, and the CLI use **blocking** `std::thread` loops with `blocking_send` / `blocking_recv`. The important part is still **no shared mutable state** across those threads—only messages.

---

## Engineering approach

The implementation started by making the **full path work end-to-end** for one exchange, then replicating the same pattern for others. As components were introduced and stabilized, targeted **unit tests** were added incrementally to validate **behavior** (especially around engine flows such as snapshots, updates, resyncs, unknown instruments, and query edge cases); those tests later became the safety net for **iterative refactors**, confirming that behavior remained correct while simplifying duplicated code with **generic helpers** in `md_core`. **Observability** was introduced early (structured `tracing` + durable logs), while **profiling** was treated as a later optimization pass once the architecture was stable.

---

## Design principles

| Principle | Rationale |
|-----------|-----------|
| **Low latency** | **Avoid contended locks** on the book hot path; keep adapter validation and normalization tight; use **bounded queues** so you can reason about lag. |
| **Fault tolerance** | Reconnect + backoff; engine-driven **resync** when local books cannot be trusted; unknown exchanges or instruments do not take down the process. |
| **Scalability** | More venues mean more connector+adapter pairs and more WebSocket fan-out—the model is **sharded by exchange** rather than one giant shared state. |
| **Observability** | Structured fields (`component`, `exchange`, `instrument`) and ScyllaDB storage support operational questions (“what failed for OKX in the last hour?”). |
| **Concurrency philosophy** | Prefer **explicit messages** and ownership over `Arc<Mutex<…>>`; use **bounded** channels for **backpressure**; keep **isolation per exchange** so one venue cannot silently stall the whole system. |

---

## Observability

- **`tracing`** is used throughout connectors, adapters, engine, and query code with consistent metadata so logs are filterable in production tooling.
- **`DbLoggingLayer`** forwards events into a **non-blocking** bounded channel; if the buffer is full, events may be dropped so logging never blocks the market path.
- **`DbLoggingWriter`** applies `config/init.cql` (if needed) and writes to ScyllaDB tables keyed for typical queries (e.g. by time, component, exchange, instrument).

**Why ScyllaDB.** Log volume is append-heavy and time-ordered—similar to time-series and operational telemetry. ScyllaDB fits wide, partition-friendly access patterns and can scale out if you keep the log path separate from the in-memory hot path. For local development, a single-node container is enough.

**Optional: DbVisualizer** (or any CQL-capable client) can connect to `127.0.0.1:9042` to inspect schemas and run ad hoc queries on stored events—useful when debugging resync storms or connector errors.

---

## CLI Usage

Start the app (`cargo run --release` after configuration). The CLI prints a command menu. Exchange names are **case-insensitive** (`binance`, `okx`, …).

| Command | Description |
|---------|-------------|
| `book <exchange> <instrument> <depth>` | Live exchange-specific book (for the selected venue): asks, bids, mid, spread. |
| `best <instrument>` | Live best bid/ask per exchange. |
| `spread <instrument>` | Live cross-exchange spread view. |
| `depth <instrument> <depth>` | Live aggregated depth across venues. |
| `status <exchange>` | One-shot connectivity / initialization status for one venue. |
| `status --all` or `status_all` | Live status for all exchanges. |
| `list [exchange]` | One-shot list of instruments (optional filter by exchange). |
| `search …` | Prefix search; or `--contains`, `--suffix`, `--glob` with optional `--limit N`. |
| `clear` | Clear the terminal screen. |
| `exit` | Quit the application. |

**Examples:**

```text
book binance BTCUSDT 10
best ETHUSDT
spread BTCUSDT
depth SOLUSDT 15
status okx
list bybit
search BTC
```

---

## Installation and setup

**Requirements**

- Rust toolchain (edition compatible with the workspace `Cargo.toml`).
- Network access to the exchanges’ public endpoints.
- **Docker** (optional but recommended) for ScyllaDB logging.

**1. ScyllaDB (optional logging)**

```bash
docker run -d --name scylla -p 9042:9042 scylladb/scylla
```

If the DB writer fails to start, the app prints a warning and **continues without persisting logs** (see `app` startup).

**2. Run**

From the repository root:

```bash
cargo run --release
```

---

## Challenges

- **Exchange inconsistency** — Different naming, precision, message shapes, and depth semantics; normalization hides this from the engine but each connector/adapter pair must encode the quirks.
- **Snapshot / update synchronization** — You must align REST snapshots with WebSocket update IDs (where applicable) and buffer or drop updates correctly until the book is valid; mistakes show up as checksum or sequence errors and **resync** storms.
- **WebSocket behavior** — Multi-connection fan-out, subscription limits, ping/pong deadlines, and abrupt disconnects; requires dedicated reader/writer tasks and backoff, not a single naive loop.
- **State management** — **Adapters** hold venue-local sync state (sequences, buffers until snapshot); the **engine** holds consolidated L2 books and drives resync. Deciding whether an anomaly is a **sync** problem (resync) vs. a **data** problem (log and continue) spans both layers.

---

## Profiling

Structured logging and tracing are in place; **CPU and async profiling** (e.g. flamegraphs, Tokio console, deeper connector latency histograms) are planned as a next step once workloads and deployment targets are fixed.

