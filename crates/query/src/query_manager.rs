use std::{io::{self, Write}, time::{Duration, Instant}};

use crossterm::{cursor::MoveTo, event::{self, Event, KeyCode, KeyEventKind}, execute, terminal::{disable_raw_mode, enable_raw_mode, Clear, ClearType}};
use md_core::{events::EngineMessage, query::EngineQuery, types::{Exchange, Instrument}};
use tokio::sync::{mpsc::Sender, oneshot};

pub struct QueryManager {
    normalized_tx: Sender<EngineMessage>,
}

impl QueryManager {
    pub fn new(normalized_tx: Sender<EngineMessage>) -> Self {
        Self { normalized_tx }
    }

    pub fn run(&self) {
        loop {
            print!("> ");
            io::stdout().flush().unwrap();

            let mut input = String::new();
            io::stdin().read_line(&mut input).unwrap();

            let mut line = input.trim().split_whitespace();
            let cmd = line.next().unwrap_or("");

            match cmd {
                "top" => {
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
                        eprintln!("Error: missing top number");
                        continue;
                    };

                    let Ok(n) = n_str.parse::<usize>() else {
                        eprintln!("Error: invalid number for n");
                        continue;
                    };

                    let instrument = instrument.to_uppercase();
                    self.run_top_mode(exchange, instrument, n);
                }
                "status" => {
                    let Some(exchange_str) = line.next() else {
                        eprintln!("Error: missing exchange");
                        continue;
                    };
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
                                    println!("{} status: {}", exchange, status);
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
                    let (status_tx, status_rx) = oneshot::channel();

                    let status_event = EngineMessage::Query(EngineQuery::AllStatuses {
                        reply_to: status_tx
                    });

                    if let Err(e) = self.normalized_tx.blocking_send(status_event) {
                        eprintln!("Error while sending the status request: {}", e);
                        continue;
                    }

                    match status_rx.blocking_recv() {
                        Ok(all_status) => {
                            for exchange_status_view in all_status {
                                println!("{}    {}    {}", exchange_status_view.exchange, exchange_status_view.status, exchange_status_view.instruments);
                            }
                        },
                        Err(e) => {
                            eprintln!("Error while receiving the status data: {}", e);
                            continue;
                        }
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
                                    for instrument in list {
                                        println!("{instrument}");
                                    }
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

                    let (best_tx, best_rx) = oneshot::channel();

                    let best_event = EngineMessage::Query(EngineQuery::Best { 
                        instrument, 
                        reply_to: best_tx
                    });

                    if let Err(e) = self.normalized_tx.blocking_send(best_event) {
                        eprintln!("Error while sending the best request: {}", e);
                        continue;
                    }

                    match best_rx.blocking_recv() {
                        Ok(best_levels) => {
                            for best in best_levels {
                                println!("{}    {}    {}    best_bid: {:?}    best_ask: {:?}", best.exchange, best.status, best.instrument, best.best_bid, best.best_ask);
                            }
                        },
                        Err(e) => {
                            eprintln!("Error while receiving the best data: {}", e);
                            continue;
                        }
                    }
                },
                "spread" => {
                    let Some(instrument) = line.next() else {
                        eprintln!("Error: missing instrument");
                        continue;
                    };
                    let instrument = Instrument(instrument.to_uppercase());

                    let (spread_tx, spread_rx) = oneshot::channel();

                    let spread_event = EngineMessage::Query(EngineQuery::Spread { 
                        instrument, 
                        reply_to: spread_tx 
                    });

                    if let Err(e) = self.normalized_tx.blocking_send(spread_event) {
                        eprintln!("Error while sending the spread request: {}", e);
                        continue;
                    }

                    match spread_rx.blocking_recv() {
                        Ok(msg) => {
                            match msg {
                                Some(spread_view) => {
                                    println!("{}", spread_view.instrument);
                                    println!("best_bid_exchange: {}", spread_view.best_bid_exchange);
                                    println!("best_bid: {}", spread_view.best_bid);
                                    println!("best_ask_exchange: {}", spread_view.best_ask_exchange);
                                    println!("best_ask: {}", spread_view.best_ask);
                                    println!("absolute_spread: {}", spread_view.absolute_spread);
                                    println!("relative_spread_bps: {}", spread_view.relative_spread_bps);
                                },
                                None => {
                                    println!("No exchange has the instrument");
                                }
                            }
                        },
                        Err(e) => {
                            eprintln!("Error while receiving the spread data: {}", e);
                            continue;
                        }
                    }
                }
                "search" => {
                    let Some(query) = line.next() else {
                        eprintln!("Error: missing query");
                        continue;
                    };

                    let (search_tx, search_rx) = oneshot::channel();

                    let search_event = EngineMessage::Query(EngineQuery::Search { 
                        query: query.to_string().to_uppercase(),
                        reply_to: search_tx
                    });

                    if let Err(e) = self.normalized_tx.blocking_send(search_event) {
                        eprintln!("Error while sending the search request: {}", e);
                        continue;
                    }

                    match search_rx.blocking_recv() {
                        Ok(list) => {
                            for (instrument, vec) in list {
                                println!("{}    {:?}", instrument, vec);
                            }
                        },
                        Err(e) => {
                            eprintln!("Error while receiving the search data: {}", e);
                            continue;
                        }
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

                    let (depth_tx, depth_rx) = oneshot::channel();

                    let depth_event = EngineMessage::Query(EngineQuery::Depth { 
                        instrument, 
                        depth, 
                        reply_to: depth_tx
                    });

                    if let Err(e) = self.normalized_tx.blocking_send(depth_event) {
                        eprintln!("Error while sending the search request: {}", e);
                        continue;
                    }

                    match depth_rx.blocking_recv() {
                        Ok(aggregate_view) => {
                            let last_asks: Vec<String> = aggregate_view.asks.into_iter().map(|x| x.to_string()).collect();
                            let last_bids: Vec<String> = aggregate_view.bids.into_iter().map(|x| x.to_string()).collect();
                            self.render_top_mode(&aggregate_view.instrument, &last_asks, &last_bids, None);
                        },
                        Err(e) => {
                            eprintln!("Error while receiving the depth data: {}", e);
                            continue;
                        }
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