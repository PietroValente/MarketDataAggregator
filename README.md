# Market Data Aggregator

**A realistic, production‑style market data engine for multi‑exchange order book aggregation, built in Rust with a strong focus on performance, reliability, and observability.**

This repository is not a toy example or a collection of isolated snippets. It is a complete, end‑to‑end system that connects to multiple cryptocurrency exchanges, streams order book data in real time, normalizes everything into a common format, and lets you query and compare instruments across venues from a single, consistent interface.  

The goal is twofold:

- **Solve a real problem**: make it easy to understand how the same instrument behaves on different exchanges – who has the best price, which venue is more liquid, how markets react to events across venues.
- **Showcase solid engineering practices**: clean architecture, explicit concurrency decisions, strong error handling, structured tracing, and durable logging via ScyllaDB.

At the moment, Binance is fully integrated and used as the reference implementation; the architecture, traits, and control flows are designed so that other exchanges (Coinbase, OKX, Bybit, Bitget, …) plug into the same pattern.

---

### Why this project exists

Real‑world market data systems are messy. Connections drop, updates arrive out of order, REST endpoints are slower than you expect, and different exchanges use completely different payloads and semantics.  

This project was born from the desire to:

- Work with **real market data** instead of artificial fixtures.
- Design an architecture that can **survive real exchange behaviour**, not just the happy path.
- Gain full control over the **trade‑offs between performance, correctness, and complexity**.

In other words, it is meant to look and feel like the core of a production‑grade market data service, but small enough to fit in a single repository and be fully understandable.

---

### What the system does

At a high level, `MarketDataAggregator`:

- Connects to multiple exchanges through **WebSockets** (for streaming depth updates) and **REST** (for snapshots).
- Translates each venue’s specific payloads into a **shared, normalized model** (`Instrument`, `BookSnapshot`, `BookUpdate`, `NormalizedEvent`, …).
- Maintains, in memory, a **per‑exchange view of every instrument’s order book**.
- Exposes a local query interface so that you can:
  - Ask for the **top N bids/asks** for a given instrument on a given exchange.
  - Visually follow how a book evolves in real time.
- Stores structured logs into **ScyllaDB**, so that every event in the system is traceable and easy to filter by component, exchange, or instrument.

The long‑term vision is to make it trivial to compare the same symbol across venues – for example, to study spreads between Binance and Coinbase on `BTCUSDT`, or to inspect how liquidity concentrates differently on different books.

---

### Architectural overview

The repository is organized as a Rust workspace, with a clear separation of responsibilities:

- **`app`** – the main binary. It wires together connectors, parsers, the central engine, the interactive query interface, and the logging pipeline.
- **`exchanges/*`** – one crate per exchange (Binance, Coinbase, OKX, Bybit, Bitget, …). Each exchange implements a common `ExchangeConnector` trait and defines its own parser and message types.
- **`core` (`md_core`)** – shared domain model and infrastructure:
  - Order book types (`BookSnapshot`, `BookUpdate`, `BookLevel`).
  - Normalized events and queries (`NormalizedEvent`, `EventEnvelope`, `ControlEvent`, `NormalizedQuery`, …).
  - The `ExchangeConnector` trait and reusable connection helpers (reader/writer tasks, ping/pong handling, etc.).
  - Logging abstractions and types used by the custom `tracing` layer and the ScyllaDB writer.
- **`engine`** – the in‑memory “brain” of the system:
  - Maintains per‑exchange state.
  - Applies snapshots and updates to local books.
  - Answers top‑of‑book queries.
  - Detects inconsistencies and drives resynchronization via control events.
- **`query`** – a small interactive CLI. It sends normalized queries to the engine and renders the response in the terminal (e.g. continuous top‑of‑book view).

![MarketDataAggregator Architecture](https://github.com/PietroValente/MarketDataAggregator/blob/main/images/Architecture.png)

---

### Concurrency model: message passing over locking

One of the most important design decisions in this project is the **explicit choice to build concurrency around message passing instead of shared locks**.

Rust makes it relatively safe to share state with `Arc<Mutex<…>>`, but using locks everywhere is not automatically good architecture. For this kind of system – many IO‑bound tasks, high‑frequency messages, and clear ownership boundaries – message passing turned out to be a better fit.

The system relies heavily on:

- `tokio::sync::mpsc` channels for **asynchronous pipelines**:
  - Connectors → Parsers → Engine.
  - Engine → Connectors (control channels).
  - Query interface → Engine.
- `tokio::sync::oneshot` channels for **request/response style** interactions, especially for queries (e.g. “give me the top 10 bids on Binance for BTCUSDT”).

This approach has several concrete advantages:

- **No hidden locking costs** on hot paths. State lives inside well‑defined components (engine, connector managers, parsers) instead of being shared behind locks.
- **Backpressure is explicit**. If a channel is full, you can reason about what it means (downstream is slower than upstream) and tune buffer sizes or processing speed, instead of discovering contention through random lock timings.
- **Failure isolation**. If one connector misbehaves or slows down, it only affects the messages it owns. The rest of the system, and other exchanges, keep running.
- **Easier mental model**. Each component is essentially a single‑threaded event loop over its own inbox, which is a very robust model for systems with lots of IO.

Avoiding locking here was a deliberate choice. It required a bit more thinking up front about **which component owns which responsibility**, but it pays off in predictable performance and a clear flow of data.

---

### Performance and reliability focus

From the start, the project was shaped by three main qualities:

- **Performance**
  - Minimal and intentional use of `clone`, especially for heavy data structures.
  - No global `Mutex` in the critical path – state is owned by long‑lived components and accessed through channels.
  - Bounded concurrency for REST snapshot fetching, to utilize network resources without overwhelming the exchange or the machine.
- **Reliability**
  - Every exchange connector implements **reconnect and resubscribe logic** with exponential backoff.
  - The engine actively checks for **gaps and inconsistencies** in local order books; when it detects an issue, it doesn’t guess – it asks the connector to resynchronize from snapshots.
  - Queries are built on top of one‑shot channels, so each request has a clear lifecycle and either receives a response or fails in a controlled way.
- **Observability**
  - Deep integration with `tracing` provides structured logs with context about **which component, which exchange, and which instrument** is involved.
  - All logs are persisted to ScyllaDB, making it easy to ask: *“What happened to Binance’s connector in the last 5 minutes?”* or *“How often did we resync BTCUSDT today?”*

These constraints forced the architecture to evolve beyond the naive idea that “each request only takes a few milliseconds”. In practice, **network and exchange behaviour can be much worse**, and the code is written for that reality, not for the ideal case.

---

### The most challenging part: exchange connectors

The hardest and most interesting part of this project is the family of **exchange connectors**.

On paper, they seem straightforward:

1. Open a WebSocket.
2. Subscribe to some streams.
3. Read messages and push them downstream.

In practice:

- Depth updates are very frequent and can arrive in bursts.
- REST endpoints for snapshots can be slow, rate‑limited, or temporarily unavailable.
- Connections drop, and re‑subscribing takes longer than expected.
- A request that “should” take a few milliseconds can suddenly take hundreds of milliseconds or more.

If the architecture assumes everything is always fast, **a single slow operation can stall the entire system**.

To deal with this, connectors are built around a dedicated **connection manager**:

- Manages **multiple WebSocket connections** per exchange, each responsible for a subset of instruments.
- Spawns a **reader task** and a **writer task** for each connection:
  - Readers push inbound messages into a unified channel (`InboundEvent`), including pings, binary/text frames, and connection‑closed events.
  - Writers receive commands (`WriteCommand`) such as “send this raw subscription payload” or “respond with a Pong”.
- Handles **ping/pong** fully via message passing. The application never touches the raw socket from multiple places.
- Coordinates **recreate with snapshots**:
  - On a resync request from the engine, it aborts all current connections.
  - Opens fresh WebSockets and resubscribes to all streams.
  - Fetches snapshots via REST and feeds them into the system so the books are consistent again.
- Applies **exponential backoff** when something goes wrong, instead of hammering the exchange.

This design emerged from hitting exactly the problem you would expect in a real system: a supposedly “fast” operation becoming slow enough to **slow down the whole pipeline**, which forced a rethink of responsibility boundaries and retry strategies.

---

### Querying the aggregated books

Once the connectors and engine are running, the **query manager** gives you a way to interact with the data from the terminal.

The primary command is:

```text
top <exchange> <instrument> <n>
```

For example:

- `top BINANCE BTCUSDT 10`
- `top COINBASE ETHUSD 5`

When you run this command:

1. The query manager switches into a special “top mode” for that instrument.
2. On a fixed interval (e.g. every 500ms), it:
   - Sends a `TopAsk` and a `TopBid` query to the engine.
   - Waits for the responses via one‑shot channels.
   - Renders the resulting asks and bids in a compact visual view.
3. You can follow how the book moves in real time; pressing `Esc` exits the mode and returns you to the CLI prompt.

This is intentionally minimal, but powerful enough to **inspect and compare the shape of order books across exchanges** in the same process.

---

### Tracing, error handling, and ScyllaDB logging

A big part of making this project “production‑style” is the emphasis on **tracing and persistent logs**.

The logging pipeline has three layers:

1. **`tracing` integration**
   - All significant components (connectors, engine, parsers, query manager) emit `tracing` events.
   - Each event carries structured fields:
     - `component` (e.g. `Connector`, `Engine`).
     - `exchange` (e.g. `Binance`).
     - `instrument` (when applicable).
   - Very low‑level `TRACE` logs are filtered out to keep the signal-to-noise ratio high.

2. **`DbLoggingLayer`**
   - This is a custom `tracing_subscriber::Layer` that converts `tracing` events into domain‑specific `LogEvent` values.
   - It extracts metadata (level, target, file, line) and the message, plus the custom fields (`component`, `exchange`, `instrument`).
   - It sends `LogEvent` instances through a non‑blocking channel. If the channel is full, the event is silently dropped rather than slowing down the main path.

3. **`DbLoggingWriter` + ScyllaDB**
   - Runs in a dedicated async task started by the main app.
   - On startup, it reads a CQL initialization file and creates the necessary keyspace and tables if they are missing.
   - It persists logs into multiple tables optimized for common queries:
     - Per‑instrument logs.
     - Per‑(component, exchange) logs.
     - Generic logs without exchange/instrument.

Why ScyllaDB?

- Market data systems generate **append‑only, time‑series data** – exactly the kind of workload ScyllaDB is optimized for.
- You typically want to ask questions like:
  - “Show me all ERROR logs for Binance’s connector in the last N minutes.”
  - “Show me all logs related to this particular instrument around a specific timestamp.”
- With ScyllaDB’s data model and clustering keys, these queries can be made efficient and horizontally scalable.

For development, ScyllaDB runs locally in Docker. This keeps the setup easy while still reflecting how a real deployment would treat logging as a separate, scalable concern.

---

### Testing and edge cases

Special attention is given to **tests that mirror realistic edge cases**, especially around the engine:

- **Snapshot + update correctness**
  - Tests validate that a snapshot followed by multiple updates leads to the expected final order book.
  - The engine’s `top_n_bid` and `top_n_ask` queries are checked for both price and quantity.
- **Unknown exchanges**
  - If an event comes in tagged with an exchange the engine doesn’t know, the engine simply logs the situation and keeps going. There is no panic.
- **Resync semantics**
  - When the engine detects a gap in update IDs (meaning its local book is no longer guaranteed to be consistent), it emits a `Resync` control event for that exchange.
  - Separate tests ensure that not every error leads to resync. For example, an update for an instrument that was never initialized should **not** trigger a resync; it is treated as a data‑quality issue, not a synchronization problem.
- **Missing instruments in queries**
  - Queries for instruments that are not known (or not yet synchronized) are handled gracefully. The engine either returns empty results or lets the one‑shot sender drop, but it does not crash.

Taken together, these tests ensure that the system behaves sensibly not just when everything is perfect, but also when data arrives late, out of order, or for instruments you didn’t expect.

---

### AI’s role in the project

AI played an important part in this project, but not as an autopilot. Instead, it was used as a **design partner and refactoring assistant**:

- During the early stages, AI helped explore different architectural options:
  - How to structure the connector trait.
  - How to decouple the engine from the connectors via control channels.
  - How to build a logging pipeline that doesn’t block the critical path.
- AI was used to draft and refine individual components, tests, and documentation, always followed by **manual review, adjustment, and integration**.
- When performance or complexity issues appeared (for example, realizing that some operations took much longer than expected and slowed down the whole system), AI suggestions were used as input to rethink the architecture, not as the final word.

The final codebase reflects this collaboration: AI accelerated the exploration of ideas, while the final architecture, trade‑offs, and critical decisions are the result of deliberate human control.

---

### Rough guide to the code

A quick (non‑exhaustive) overview of where to look:

- **`crates/app/src/main.rs`**
  - Entry point. Loads configuration, sets up tracing and the ScyllaDB logging pipeline, creates channels, and wires together connectors, parsers, engine, and query manager.
- **`crates/core/src`**
  - `types.rs`: core domain types (exchanges, instruments, prices, quantities, etc.).
  - `book.rs`: structures and utilities for maintaining order books.
  - `events.rs`: normalized events and queries; the language the engine speaks.
  - `connector_trait.rs`: the `ExchangeConnector` trait and its helper tasks for readers/writers.
  - `logging/*`: the types, layer, and writer that connect `tracing` to ScyllaDB.
- **`crates/engine/src`**
  - `engine.rs`: the central loop that receives normalized events, updates state, and replies to queries. Contains the bulk of the engine tests.
  - `exchange_state.rs`: per‑exchange local book state and related logic.
  - `client.rs`: convenience utilities for talking to the engine.
- **`crates/exchanges/binance/src`**
  - `connector.rs`: the reference implementation of an exchange connector, including the connection manager, resync protocol, and backoff logic.
  - `parser.rs`: transforms raw Binance messages into normalized events.
  - `types.rs`: Binance‑specific message representations.
- **`crates/query/src/query_manager.rs`**
  - The interactive CLI responsible for `top` commands and the “top mode” visualization.

The other exchange crates follow the same pattern and are designed to be filled in following the Binance implementation.

---

### Running the project locally

To get the full experience (including ScyllaDB logging), you will need:

- A recent Rust toolchain.
- Docker, to spin up ScyllaDB.
- Network access to the exchanges’ public market data endpoints.

**1. Start ScyllaDB via Docker**

```bash
docker run -d --name scylla \
  -p 9042:9042 \
  scylladb/scylla
```

Make sure the URI in your configuration (e.g. `127.0.0.1:9042`) matches this.

**2. Configure the application**

Edit your configuration file (for example `config/config.toml`) to specify:

- Exchange URLs (REST and WebSocket) for each venue.
- Channel buffer sizes (raw, normalized, control, logs).
- ScyllaDB connection details and the path to the CQL initialization script.

**3. Run the main app**

From the workspace root:

```bash
cargo run -p app
```

You should see initialization logs, followed by the query prompt. From there you can use commands like:

```text
top BINANCE BTCUSDT 10
```

to inspect the live order book.

---

### Future directions

This codebase is intentionally structured so that it can grow in several directions:

- **Cross‑exchange comparison tools**
  - Turn the current per‑exchange `top` view into true cross‑venue analytics (spreads, synthetic best bid/ask across all venues, per‑venue imbalance).
- **Historical storage of normalized books**
  - Use a similar ScyllaDB‑style approach, or another time‑series store, to persist normalized books for backtesting and research.
- **Alerting and monitoring**
  - Build alerting on top of the logging and tracing data, e.g. “alert if an exchange resyncs more than N times in M minutes”.
- **Richer user interfaces**
  - Expose an HTTP API or a web UI on top of the engine while keeping the same internal message‑passing and logging architecture.

Even in its current state, the project represents a realistic, carefully‑designed foundation for a market data service, with a strong emphasis on concurrency, correctness, and observability – the same qualities that matter in production systems.
