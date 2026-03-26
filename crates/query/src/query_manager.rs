use std::{
    io::{self, Write},
    time::{Duration, Instant},
};

use crossterm::{
    cursor::MoveTo,
    event::{self, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, Clear, ClearType},
};
use md_core::{
    events::{EventEnvelope, NormalizedEvent, NormalizedQuery, NormalizedTop},
    types::{Exchange, Instrument},
};
use tokio::sync::{mpsc::Sender, oneshot};

pub struct QueryManager {
    normalized_tx: Sender<EventEnvelope>,
}

impl QueryManager {
    pub fn new(normalized_tx: Sender<EventEnvelope>) -> Self {
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

                    let status_event = EventEnvelope {
                        exchange,
                        event: NormalizedEvent::Query(NormalizedQuery::GetStatus(status_tx)),
                    };
                    if let Err(e) = self.normalized_tx.blocking_send(status_event) {
                        eprintln!("Error while sending the status request: {}", e);
                        continue;
                    }

                    match status_rx.blocking_recv() {
                        Ok(msg) => {
                            println!("{} status: {}", exchange, msg);
                        },
                        Err(e) => {
                            eprintln!("Error while receiving the status data: {}", e);
                            continue;
                        }
                    }
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

    fn run_top_mode(&self, exchange: Exchange, instrument: String, n: usize) {
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

                let (asks_tx, asks_rx) = oneshot::channel();
                let (bids_tx, bids_rx) = oneshot::channel();

                let asks_event = EventEnvelope {
                    exchange,
                    event: NormalizedEvent::Query(NormalizedQuery::TopAsk(asks_tx, NormalizedTop {
                        instrument: Instrument::from(instrument.clone()),
                        n
                    })),
                };

                let bids_event = EventEnvelope {
                    exchange,
                    event: NormalizedEvent::Query(NormalizedQuery::TopBid(bids_tx, NormalizedTop {
                        instrument: Instrument::from(instrument.clone()),
                        n
                    })),
                };

                if let Err(e) = self.normalized_tx.blocking_send(asks_event) {
                    last_status = Some(format!("Error while sending the asks request: {}", e));
                    self.render_top_mode(&instrument, &last_asks, &last_bids, last_status.as_deref());
                    last_refresh = Instant::now();
                    continue;
                }

                if let Err(e) = self.normalized_tx.blocking_send(bids_event) {
                    last_status = Some(format!("Error while sending the bids request: {}", e));
                    self.render_top_mode(&instrument, &last_asks, &last_bids, last_status.as_deref());
                    last_refresh = Instant::now();
                    continue;
                }

                let asks = match asks_rx.blocking_recv() {
                    Ok(msg) => msg,
                    Err(e) => {
                        last_status = Some(format!("Error while receiving the asks data: {}", e));
                        self.render_top_mode(&instrument, &last_asks, &last_bids, last_status.as_deref());
                        last_refresh = Instant::now();
                        continue;
                    }
                };

                let bids = match bids_rx.blocking_recv() {
                    Ok(msg) => msg,
                    Err(e) => {
                        last_status = Some(format!("Error while receiving the bids data: {}", e));
                        self.render_top_mode(&instrument, &last_asks, &last_bids, last_status.as_deref());
                        last_refresh = Instant::now();
                        continue;
                    }
                };

                last_asks = asks.into_iter().map(|x| x.to_string()).collect();
                last_bids = bids.into_iter().map(|x| x.to_string()).collect();

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

            println!("---------------------------");

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