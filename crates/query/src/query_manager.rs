use std::{cmp::max, io::{self, Write}, time::{Duration, Instant}};

use crossterm::{cursor::MoveTo, event::{self, Event, KeyCode, KeyEventKind}, execute, terminal::{disable_raw_mode, enable_raw_mode, Clear, ClearType}};
use md_core::{events::EngineMessage, query::{AggregatedDepthView, BestLevelPerExchange, BookView, EngineQuery, ExchangeStatusView, SpreadView}, types::{Exchange, Instrument}};
use tokio::sync::{mpsc::Sender, oneshot};

pub struct QueryManager {
    normalized_tx: Sender<EngineMessage>,
}

impl QueryManager {
    pub fn new(normalized_tx: Sender<EngineMessage>) -> Self {
        Self { normalized_tx }
    }

    pub fn run(&self) {
        self.print_welcome();
        loop {
            self.print_menu();
            print!("> ");
            io::stdout().flush().unwrap();

            let mut input = String::new();
            io::stdin().read_line(&mut input).unwrap();

            let mut line = input.trim().split_whitespace();
            let cmd = line.next().unwrap_or("");

            match cmd {
                "book" => {
                    let Some(exchange_str) = line.next() else {
                        eprintln!("Error: missing exchange");
                        continue;
                    };

                    let exchange = Exchange::from(exchange_str);
                    if exchange == Exchange::Unknown {
                        eprintln!("Error: unrecognized exchange");
                        continue;
                    }

                    let Some(instrument) = line.next() else {
                        eprintln!("Error: missing instrument");
                        continue;
                    };

                    let Some(n_str) = line.next() else {
                        eprintln!("Error: missing depth");
                        continue;
                    };

                    let Ok(n) = n_str.parse::<usize>() else {
                        eprintln!("Error: invalid depth `{n_str}`");
                        continue;
                    };

                    let instrument = Instrument(instrument.to_uppercase());
                    if let Err(e) = self.run_book_watch(exchange, instrument, n) {
                        eprintln!("{e}");
                    }
                }
                "status" => {
                    let Some(exchange_str) = line.next() else {
                        eprintln!("Error: missing exchange (or use --all)");
                        continue;
                    };
                    if exchange_str == "--all" {
                        if let Err(e) = self.run_all_status_watch() {
                            eprintln!("{e}");
                        }
                        continue;
                    }

                    let exchange = Exchange::from(exchange_str);
                    if exchange == Exchange::Unknown {
                        eprintln!("Error: unrecognized exchange");
                        continue;
                    }

                    let (status_tx, status_rx) = oneshot::channel();

                    let status_event = EngineMessage::Query(EngineQuery::ExchangeStatus {
                        exchange,
                        reply_to: status_tx
                    });

                    if let Err(e) = self.normalized_tx.blocking_send(status_event) {
                        eprintln!("Error while sending the status request: {}", e);
                        continue;
                    }

                    match status_rx.blocking_recv() {
                        Ok(msg) => {
                            match msg {
                                Ok(status) => {
                                    println!("Exchange status");
                                    println!("  {} -> {}", exchange, status);
                                },
                                Err(e) => {
                                    println!("error: {}", e);
                                }
                            }
                        },
                        Err(e) => {
                            eprintln!("Error while receiving the status data: {}", e);
                            continue;
                        }
                    }
                }
                "status_all" => {
                    if let Err(e) = self.run_all_status_watch() {
                        eprintln!("{e}");
                    }
                }
                "list" => {
                    let mut exchange = None;
                    if let Some(exchange_str) = line.next() {
                        let exchange_specific = Exchange::from(exchange_str);
                        if exchange_specific == Exchange::Unknown {
                            eprintln!("Error: unrecognized exchange");
                            continue;
                        }
                        exchange = Some(exchange_specific);
                    };

                    let (list_tx, list_rx) = oneshot::channel();

                    let list_event = EngineMessage::Query(EngineQuery::List { 
                        exchange, 
                        reply_to: list_tx
                    });

                    if let Err(e) = self.normalized_tx.blocking_send(list_event) {
                        eprintln!("Error while sending the status request: {}", e);
                        continue;
                    }

                    match list_rx.blocking_recv() {
                        Ok(msg) => {
                            match msg {
                                Ok(list) => {
                                    let instruments: Vec<String> = list.into_iter().map(|i| i.to_string()).collect();
                                    println!("Instruments ({}):", instruments.len());
                                    self.render_columns(&instruments, 4);
                                },
                                Err(e) => {
                                    println!("error: {}", e);
                                }
                            }
                        },
                        Err(e) => {
                            eprintln!("Error while receiving the status data: {}", e);
                            continue;
                        }
                    }
                }
                "best" => {
                    let Some(instrument) = line.next() else {
                        eprintln!("Error: missing instrument");
                        continue;
                    };
                    let instrument = Instrument(instrument.to_uppercase());
                    if let Err(e) = self.run_best_watch(instrument) {
                        eprintln!("{e}");
                    }
                },
                "spread" => {
                    let Some(instrument) = line.next() else {
                        eprintln!("Error: missing instrument");
                        continue;
                    };
                    let instrument = Instrument(instrument.to_uppercase());
                    if let Err(e) = self.run_spread_watch(instrument) {
                        eprintln!("{e}");
                    }
                }
                "search" => {
                    let args: Vec<&str> = line.collect();
                    if let Err(e) = self.handle_search(args) {
                        eprintln!("{e}");
                    }
                }
                "depth" => {
                    let Some(instrument) = line.next() else {
                        eprintln!("Error: missing instrument");
                        continue;
                    };
                    let instrument = Instrument(instrument.to_uppercase());

                    let Some(depth_str) = line.next() else {
                        eprintln!("Error: missing depth");
                        continue;
                    };

                    let Ok(depth) = depth_str.parse::<usize>() else {
                        eprintln!("Error: invalid number for n");
                        continue;
                    };
                    if let Err(e) = self.run_depth_watch(instrument, depth) {
                        eprintln!("{e}");
                    }
                }
                "clear" => {
                    self.clear_screen();
                }
                "exit" => {
                    return;
                }
                _ => {
                    eprintln!("Error: command not recognised");
                }
            }
        }
    }

    fn print_welcome(&self) {
        println!("MarketDataAggregator CLI");
        println!("Live views update continuously; press `Esc` to return to the menu.");
        println!();
    }

    fn print_menu(&self) {
        let rows = [
            ("book <exchange> <instrument> <depth>", "Live order book (asks above, bids below, mid center)"),
            ("best <instrument>", "Live best bid/ask per exchange"),
            ("spread <instrument>", "Live cross-exchange spread"),
            ("depth <instrument> <depth>", "Live aggregated depth"),
            ("status <exchange>", "Exchange status (one-shot)"),
            ("status --all", "Exchange statuses (live)"),
            ("list [exchange]", "Instruments list (one-shot)"),
            ("search <pattern>", "Prefix search (one-shot)"),
            ("search --contains <text> [--limit N]", "Substring search (one-shot)"),
            ("search --suffix <text> [--limit N]", "Suffix search (one-shot)"),
            ("search --glob <pattern> [--limit N]", "Glob search using `*` (one-shot)"),
            ("clear", "Clear screen"),
            ("exit", "Exit"),
        ];

        let cmd_w = rows.iter().map(|(cmd, _)| cmd.len()).max().unwrap_or(0);
        println!("Commands:");
        for (cmd, desc) in rows {
            println!("  {:cmd_w$}  {}", cmd, desc, cmd_w = cmd_w);
        }
        println!();
    }

    fn fmt_with_2_decimals(v: impl ToString) -> String {
        let s = v.to_string();
        let Some(dot_idx) = s.find('.') else {
            return s;
        };
        let keep = (dot_idx + 1 + 2).min(s.len());
        s[..keep].to_string()
    }

    fn run_live_mode<T, Req, Render>(&self, mut request: Req, mut render: Render) -> Result<(), String>
    where
        Req: FnMut() -> Result<T, String>,
        Render: FnMut(Option<&T>, Option<&str>),
    {
        if let Err(e) = enable_raw_mode() {
            return Err(format!("Error enabling raw mode: {e}"));
        }

        let refresh_interval = Duration::from_millis(500);
        let poll_interval = Duration::from_millis(20);
        let mut last_refresh = Instant::now() - refresh_interval;

        let mut last_data: Option<T> = None;

        'outer: loop {
            while event::poll(poll_interval).unwrap_or(false) {
                let Ok(ev) = event::read() else {
                    continue;
                };
                let Event::Key(key) = ev else {
                    continue;
                };
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                if key.code == KeyCode::Esc {
                    break 'outer;
                }
            }

            if last_refresh.elapsed() < refresh_interval {
                continue;
            }

            let status: Option<String> = match request() {
                Ok(data) => {
                    last_data = Some(data);
                    None
                }
                Err(e) => Some(e),
            };

            self.clear_screen();
            render(last_data.as_ref(), status.as_deref());
            println!();
            println!("Press Esc to return to menu");
            io::stdout().flush().unwrap();
            last_refresh = Instant::now();
        }

        let _ = disable_raw_mode();
        self.clear_screen();
        Ok(())
    }

    fn run_book_watch(&self, exchange: Exchange, instrument: Instrument, depth: usize) -> Result<(), String> {
        let tx = self.normalized_tx.clone();
        let instrument_clone = instrument.clone();
        self.run_live_mode(
            || {
                let (reply_tx, reply_rx) = oneshot::channel();
                let msg = EngineMessage::Query(EngineQuery::Book {
                    exchange,
                    instrument: instrument_clone.clone(),
                    depth,
                    reply_to: reply_tx,
                });

                tx.blocking_send(msg)
                    .map_err(|e| format!("Error sending book request: {e}"))?;

                let reply = reply_rx
                    .blocking_recv()
                    .map_err(|e| format!("Error receiving book reply: {e}"))?;

                reply.map_err(|e| format!("Book query failed: {e}"))
            },
            |data, status| self.render_book_view(data, status),
        )
    }

    fn run_best_watch(&self, instrument: Instrument) -> Result<(), String> {
        let tx = self.normalized_tx.clone();
        let instrument_clone = instrument.clone();
        self.run_live_mode(
            || {
                let (reply_tx, reply_rx) = oneshot::channel();
                let msg = EngineMessage::Query(EngineQuery::Best {
                    instrument: instrument_clone.clone(),
                    reply_to: reply_tx,
                });

                tx.blocking_send(msg)
                    .map_err(|e| format!("Error sending best request: {e}"))?;

                reply_rx
                    .blocking_recv()
                    .map_err(|e| format!("Error receiving best reply: {e}"))
            },
            |data, status| self.render_best_view(data, status),
        )
    }

    fn run_spread_watch(&self, instrument: Instrument) -> Result<(), String> {
        let tx = self.normalized_tx.clone();
        let instrument_clone = instrument.clone();
        self.run_live_mode(
            || {
                let (reply_tx, reply_rx) = oneshot::channel();
                let msg = EngineMessage::Query(EngineQuery::Spread {
                    instrument: instrument_clone.clone(),
                    reply_to: reply_tx,
                });

                tx.blocking_send(msg)
                    .map_err(|e| format!("Error sending spread request: {e}"))?;

                reply_rx
                    .blocking_recv()
                    .map_err(|e| format!("Error receiving spread reply: {e}"))
            },
            |data, status| self.render_spread_view(data, status),
        )
    }

    fn run_depth_watch(&self, instrument: Instrument, depth: usize) -> Result<(), String> {
        let tx = self.normalized_tx.clone();
        let instrument_clone = instrument.clone();
        self.run_live_mode(
            || {
                let (reply_tx, reply_rx) = oneshot::channel();
                let msg = EngineMessage::Query(EngineQuery::Depth {
                    instrument: instrument_clone.clone(),
                    depth,
                    reply_to: reply_tx,
                });

                tx.blocking_send(msg)
                    .map_err(|e| format!("Error sending depth request: {e}"))?;

                reply_rx
                    .blocking_recv()
                    .map_err(|e| format!("Error receiving depth reply: {e}"))
            },
            |data, status| self.render_depth_view(data, status),
        )
    }

    fn run_all_status_watch(&self) -> Result<(), String> {
        let tx = self.normalized_tx.clone();
        self.run_live_mode(
            || {
                let (reply_tx, reply_rx) = oneshot::channel();
                let msg = EngineMessage::Query(EngineQuery::AllStatuses { reply_to: reply_tx });

                tx.blocking_send(msg)
                    .map_err(|e| format!("Error sending all-status request: {e}"))?;

                reply_rx
                    .blocking_recv()
                    .map_err(|e| format!("Error receiving all-status reply: {e}"))
            },
            |data, status| self.render_all_status_view(data, status),
        )
    }

    fn handle_search(&self, args: Vec<&str>) -> Result<(), String> {
        if args.is_empty() {
            return Err("Usage: search <pattern> | search --contains <text> [--limit N] | search --suffix <text> [--limit N] | search --glob <pattern> [--limit N]".to_string());
        }

        enum SearchMode {
            Prefix,
            Contains,
            Suffix,
            Glob,
        }

        let mut mode = SearchMode::Prefix;
        let mut pattern: Option<String> = None;
        let mut limit: usize = 50;

        let mut i = 0usize;
        while i < args.len() {
            match args[i] {
                "--contains" => {
                    mode = SearchMode::Contains;
                    pattern = args.get(i + 1).map(|s| (*s).to_string());
                    i += 2;
                }
                "--suffix" => {
                    mode = SearchMode::Suffix;
                    pattern = args.get(i + 1).map(|s| (*s).to_string());
                    i += 2;
                }
                "--glob" => {
                    mode = SearchMode::Glob;
                    pattern = args.get(i + 1).map(|s| (*s).to_string());
                    i += 2;
                }
                "--limit" => {
                    let value = args
                        .get(i + 1)
                        .ok_or_else(|| "Error: missing --limit value".to_string())?;
                    limit = value
                        .parse::<usize>()
                        .map_err(|_| format!("Error: invalid --limit value `{value}`"))?;
                    i += 2;
                }
                other => {
                    if pattern.is_none() && matches!(mode, SearchMode::Prefix) {
                        pattern = Some(other.to_string());
                        i += 1;
                    } else {
                        return Err(format!("Error: unexpected argument `{other}`"));
                    }
                }
            }
        }

        let Some(pattern) = pattern else {
            return Err("Error: missing search pattern".to_string());
        };
        let pattern = pattern.to_uppercase();

        match mode {
            SearchMode::Prefix => {
                let (tx, rx) = oneshot::channel();
                let msg = EngineMessage::Query(EngineQuery::Search {
                    query: pattern.clone(),
                    reply_to: tx,
                });

                self.normalized_tx
                    .blocking_send(msg)
                    .map_err(|e| format!("Error sending search request: {e}"))?;

                let results = rx
                    .blocking_recv()
                    .map_err(|e| format!("Error receiving search reply: {e}"))?;

                self.render_search_results(&results, &format!("prefix `{}`", pattern));
                Ok(())
            }
            SearchMode::Contains | SearchMode::Suffix | SearchMode::Glob => {
                let (tx, rx) = oneshot::channel();
                let msg = match mode {
                    SearchMode::Contains => EngineMessage::Query(EngineQuery::SearchContains {
                        query: pattern.clone(),
                        limit,
                        reply_to: tx,
                    }),
                    SearchMode::Suffix => EngineMessage::Query(EngineQuery::SearchSuffix {
                        query: pattern.clone(),
                        limit,
                        reply_to: tx,
                    }),
                    SearchMode::Glob => EngineMessage::Query(EngineQuery::SearchGlob {
                        query: pattern.clone(),
                        limit,
                        reply_to: tx,
                    }),
                    SearchMode::Prefix => unreachable!(),
                };

                self.normalized_tx
                    .blocking_send(msg)
                    .map_err(|e| format!("Error sending search request: {e}"))?;

                let merged = rx
                    .blocking_recv()
                    .map_err(|e| format!("Error receiving search reply: {e}"))?;

                let label = match mode {
                    SearchMode::Contains => format!("contains `{pattern}` (limit {limit})"),
                    SearchMode::Suffix => format!("suffix `{pattern}` (limit {limit})"),
                    SearchMode::Glob => format!("glob `{pattern}` (limit {limit})"),
                    SearchMode::Prefix => format!("prefix `{pattern}` (limit {limit})"),
                };

                self.render_search_results(&merged, &label);
                Ok(())
            }
        }
    }

    fn render_search_results(
        &self,
        results: &std::collections::BTreeMap<Instrument, Vec<Exchange>>,
        label: &str,
    ) {
        println!("Search results ({label}):");
        if results.is_empty() {
            println!("  No matches.");
            return;
        }

        let instrument_w = results.keys().map(|k| k.to_string().len()).max().unwrap_or(10);
        let exchanges_w = results
            .values()
            .map(|v| v.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ").len())
            .max()
            .unwrap_or(10);

        println!(
            "{:instrument_w$}  {:exchanges_w$}",
            "INSTRUMENT",
            "EXCHANGES",
            instrument_w = instrument_w,
            exchanges_w = exchanges_w
        );
        println!("{}", "-".repeat(instrument_w + exchanges_w + 4));

        for (inst, exs) in results {
            let joined = exs
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            println!(
                "{:instrument_w$}  {:exchanges_w$}",
                inst,
                joined,
                instrument_w = instrument_w,
                exchanges_w = exchanges_w
            );
        }
    }

    fn render_columns(&self, items: &[String], cols: usize) {
        if items.is_empty() {
            return;
        }

        let max_w = items.iter().map(|s| s.len()).max().unwrap_or(0);
        let cell_w = max_w + 2;
        for (i, item) in items.iter().enumerate() {
            print!("{:width$}", item, width = cell_w);
            if (i + 1) % cols == 0 {
                println!();
            }
        }
        if items.len() % cols != 0 {
            println!();
        }
    }

    fn render_book_view(&self, data: Option<&BookView>, status: Option<&str>) {
        if let Some(view) = data {
            println!("Book: {} {}", view.exchange, view.instrument);
            println!(
                "Exchange status: {}",
                view.status,
            );
        } else {
            println!("Book: waiting for data...");
        }

        if let Some(msg) = status {
            println!();
            println!("Error: {msg}");
        }

        let Some(view) = data else {
            return;
        };

        let level_w = view
            .asks
            .iter()
            .chain(view.bids.iter())
            .map(|l| l.to_string().len())
            .max()
            .unwrap_or(27);

        println!();
        let spread_text = view
            .spread
            .as_ref()
            .map(|p| Self::fmt_with_2_decimals(p))
            .unwrap_or_else(|| "-".to_string());
        println!("Spread: {}", spread_text);
        println!("{}", "-".repeat(level_w + 10));

        println!("ASKS");
        for ask in view.asks.iter().rev() {
            println!("  {}", ask);
        }

        println!("{}", "-".repeat(level_w + 10));
        let mid_text = view
            .mid
            .as_ref()
            .map(|p| Self::fmt_with_2_decimals(p))
            .unwrap_or_else(|| "-".to_string());
        println!("MID:  {:>8}", mid_text);
        println!("{}", "-".repeat(level_w + 10));

        println!("BIDS");
        for bid in &view.bids {
            println!("  {}", bid);
        }
    }

    fn render_best_view(&self, data: Option<&Vec<BestLevelPerExchange>>, status: Option<&str>) {
        if let Some(levels) = data {
            let instrument = levels
                .first()
                .map(|l| l.instrument.to_string())
                .unwrap_or_else(|| "-".to_string());
            println!("Best: {} (per exchange)", instrument);
        } else {
            println!("Best: waiting for data...");
        }

        if let Some(msg) = status {
            println!();
            println!("Error: {msg}");
        }

        let Some(levels) = data else {
            return;
        };

        let exchange_w = max(
            "EXCHANGE".len(),
            levels.iter().map(|l| l.exchange.to_string().len()).max().unwrap_or(8),
        );
        let status_w = max(
            "STATUS".len(),
            levels.iter().map(|l| l.status.to_string().len()).max().unwrap_or(10),
        );
        let cell_w = max(
            "BEST_BID".len(),
            levels
                .iter()
                .flat_map(|l| [l.best_bid.as_ref(), l.best_ask.as_ref()])
                .map(|opt| opt.map(|x| x.to_string().len()).unwrap_or(1))
                .max()
                .unwrap_or(1),
        );

        let sep = format!(
            "+-{}-+-{}-+-{}-+-{}-+",
            "-".repeat(exchange_w),
            "-".repeat(status_w),
            "-".repeat(cell_w),
            "-".repeat(cell_w)
        );

        println!();
        println!("{sep}");
        println!(
            "| {:exchange_w$} | {:status_w$} | {:cell_w$} | {:cell_w$} |",
            "EXCHANGE",
            "STATUS",
            "BEST_BID",
            "BEST_ASK",
            exchange_w = exchange_w,
            status_w = status_w,
            cell_w = cell_w
        );
        println!("{sep}");

        for lvl in levels {
            let exchange = lvl.exchange.to_string();
            let ex_status = lvl.status.to_string();
            let best_bid = lvl
                .best_bid
                .as_ref()
                .map(|l| l.to_string())
                .unwrap_or_else(|| "-".to_string());
            let best_ask = lvl
                .best_ask
                .as_ref()
                .map(|l| l.to_string())
                .unwrap_or_else(|| "-".to_string());

            println!(
                "| {:exchange_w$} | {:status_w$} | {:cell_w$} | {:cell_w$} |",
                exchange,
                ex_status,
                best_bid,
                best_ask,
                exchange_w = exchange_w,
                status_w = status_w,
                cell_w = cell_w
            );
        }
        println!("{sep}");
    }

    fn render_spread_view(&self, data: Option<&Option<SpreadView>>, status: Option<&str>) {
        println!("Spread (cross-exchange):");

        if let Some(msg) = status {
            println!();
            println!("Error: {msg}");
        }

        match data {
            None => println!("Waiting for data..."),
            Some(None) => println!("No exchange has the instrument (yet)."),
            Some(Some(view)) => {
                let rows = vec![
                    ("Instrument".to_string(), view.instrument.to_string()),
                    ("Best ask exchange".to_string(), view.best_ask_exchange.to_string()),
                    ("Best bid exchange".to_string(), view.best_bid_exchange.to_string()),
                    ("Best ask".to_string(), view.best_ask.to_string()),
                    ("Best bid".to_string(), view.best_bid.to_string()),
                    ("Absolute spread".to_string(), Self::fmt_with_2_decimals(view.absolute_spread)),
                    ("Relative spread (bps)".to_string(), format!("{:.2}", view.relative_spread_bps)),
                ];
                let key_w = rows.iter().map(|(k, _)| k.len()).max().unwrap_or(10);
                let val_w = rows.iter().map(|(_, v)| v.len()).max().unwrap_or(10);

                println!();
                println!("{:key_w$} | {:val_w$}", "METRIC", "VALUE", key_w = key_w, val_w = val_w);
                println!("{}", "-".repeat(key_w + val_w + 3));
                for (k, v) in rows {
                    println!("{:key_w$} | {:val_w$}", k, v, key_w = key_w, val_w = val_w);
                }
            }
        }
    }

    fn render_depth_view(&self, data: Option<&AggregatedDepthView>, status: Option<&str>) {
        if let Some(view) = data {
            println!("Depth (aggregated): {}  (asks + bids)", view.instrument);
        } else {
            println!("Depth (aggregated): waiting for data...");
        }

        if let Some(msg) = status {
            println!();
            println!("Error: {msg}");
        }

        let Some(view) = data else {
            return;
        };

        let level_w = view
            .asks
            .iter()
            .chain(view.bids.iter())
            .map(|l| l.to_string().len())
            .max()
            .unwrap_or(27);

        println!();
        println!("ASKS");
        println!("{}", "-".repeat(level_w + 10));
        for ask in view.asks.iter().rev() {
            println!("  {}", ask);
        }

        println!("{}", "-".repeat(level_w + 10));
        println!("BIDS");
        for bid in &view.bids {
            println!("  {}", bid);
        }
    }

    fn render_all_status_view(&self, data: Option<&Vec<ExchangeStatusView>>, status: Option<&str>) {
        println!("All exchange status (live):");

        if let Some(msg) = status {
            println!();
            println!("Error: {msg}");
        }

        let Some(levels) = data else {
            return;
        };

        let exchange_w = max(
            "EXCHANGE".len(),
            levels.iter().map(|l| l.exchange.to_string().len()).max().unwrap_or(8),
        );
        let status_w = max(
            "STATUS".len(),
            levels.iter().map(|l| l.status.to_string().len()).max().unwrap_or(10),
        );
        let inst_w = max(
            "INSTR".len(),
            levels.iter().map(|l| l.instruments.to_string().len()).max().unwrap_or(9),
        );

        let sep = format!(
            "+-{}-+-{}-+-{}-+",
            "-".repeat(exchange_w),
            "-".repeat(status_w),
            "-".repeat(inst_w)
        );

        println!();
        println!("{sep}");
        println!(
            "| {:exchange_w$} | {:status_w$} | {:inst_w$} |",
            "EXCHANGE",
            "STATUS",
            "INSTR",
            exchange_w = exchange_w,
            status_w = status_w,
            inst_w = inst_w
        );
        println!("{sep}");

        for lvl in levels {
            let exchange = lvl.exchange.to_string();
            let ex_status = lvl.status.to_string();
            println!(
                "| {:exchange_w$} | {:status_w$} | {:inst_w$} |",
                exchange,
                ex_status,
                lvl.instruments,
                exchange_w = exchange_w,
                status_w = status_w,
                inst_w = inst_w
            );
        }
        println!("{sep}");
    }

    #[allow(dead_code)]
    fn run_top_mode(&self, exchange: Exchange, instrument: String, depth: usize) {
        if let Err(e) = enable_raw_mode() {
            eprintln!("Error enabling raw mode: {}", e);
            return;
        }

        let refresh_interval = Duration::from_millis(500);
        let poll_interval = Duration::from_millis(20);
        let mut last_refresh = Instant::now() - refresh_interval;

        let mut last_asks: Vec<String> = Vec::new();
        let mut last_bids: Vec<String> = Vec::new();
        let mut last_status: Option<String> = None;

        let result = (|| {
            loop {
                while event::poll(poll_interval).unwrap_or(false) {
                    let Ok(ev) = event::read() else {
                        continue;
                    };

                    let Event::Key(key) = ev else {
                        continue;
                    };

                    if key.kind != KeyEventKind::Press {
                        continue;
                    }

                    if key.code == KeyCode::Esc {
                        return;
                    }
                }

                if last_refresh.elapsed() < refresh_interval {
                    continue;
                }

                let (book_tx, book_rx) = oneshot::channel();

                let book = EngineMessage::Query(EngineQuery::Book { 
                    exchange, 
                    instrument: Instrument::from(instrument.clone()), 
                    depth, 
                    reply_to: book_tx
                });

                if let Err(e) = self.normalized_tx.blocking_send(book) {
                    last_status = Some(format!("Error while sending the book request: {}", e));
                    self.render_top_mode(&instrument, &last_asks, &last_bids, last_status.as_deref());
                    last_refresh = Instant::now();
                    continue;
                }

                let book = match book_rx.blocking_recv() {
                    Ok(msg) => {
                        match msg {
                            Err(e) => {
                                last_status = Some(format!("Error while receiving the book data: {}", e));
                                self.render_top_mode(&instrument, &last_asks, &last_bids, last_status.as_deref());
                                last_refresh = Instant::now();
                                continue;
                            }
                            Ok(msg) => msg
                        }
                    },
                    Err(e) => {
                        last_status = Some(format!("Error while receiving the book data: {}", e));
                        self.render_top_mode(&instrument, &last_asks, &last_bids, last_status.as_deref());
                        last_refresh = Instant::now();
                        continue;
                    }
                };

                last_asks = book.asks.into_iter().map(|x| x.to_string()).collect();
                last_bids = book.bids.into_iter().map(|x| x.to_string()).collect();

                if last_asks.is_empty() || last_bids.is_empty() {
                    last_status = Some("Instrument not found".to_string());
                } else {
                    last_status = None;
                }

                self.render_top_mode(&instrument, &last_asks, &last_bids, last_status.as_deref());
                last_refresh = Instant::now();
            }
        })();

        let _ = disable_raw_mode();
        self.clear_screen();
        println!("Exited top mode");

        result
    }
    
    fn render_top_mode(
        &self,
        instrument: &str,
        asks: &[String],
        bids: &[String],
        status: Option<&str>,
    ) {
        self.clear_screen();

        println!("{}", instrument);
        println!();

        if let Some(msg) = status {
            println!("{}", msg);
        } else {
            for a in asks.iter().rev() {
                println!("{}", a);
            }

            println!("      --------------------");

            for b in bids.iter() {
                println!("{}", b);
            }
        }

        println!();
        println!("Press Esc to exit");
        io::stdout().flush().unwrap();
    }

    fn clear_screen(&self) {
        let mut stdout = io::stdout();
        let _ = execute!(stdout, Clear(ClearType::All), MoveTo(0, 0));
    }
}