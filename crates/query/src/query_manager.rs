use std::io::{self, Write};

use md_core::{events::{EventEnvelope, NormalizedEvent, NormalizedQuery, NormalizedTop}, types::{Exchange, Instrument}};
use tokio::sync::{mpsc::Sender, oneshot};

pub struct QueryManager {
    normalized_tx: Sender<EventEnvelope>
}

impl QueryManager {
    pub fn new(normalized_tx: Sender<EventEnvelope>) -> Self {
        Self {
            normalized_tx
        }
    }

    pub fn run(&self) {
        println!("Ready");
        let mut input;
        loop {
            print!(">");
            io::stdout().flush().unwrap();
            
            input = String::new();
            io::stdin().read_line(&mut input).unwrap();
    
            let line = &mut input[..].trim().split_whitespace();
    
            let cmd = line.next().unwrap_or("command missing");
    
            match cmd {
                "topAsk" => {
                    let Some(exchange_str) = line.next() else {
                        eprintln!("Error: missing exchanger");
                        continue;
                    };
                    let exchange = Exchange::from(exchange_str);
                    if exchange == Exchange::Unknown {
                        eprintln!("Error: not recognized exchanger");
                        continue;
                    }
                    let Some(instrument) = line.next() else {
                        eprintln!("Error: missing instrument");
                        continue;
                    };
                    let Some(n) = line.next() else {
                        eprintln!("Error: missing top number");
                        continue;
                    };
                    
                    let (tx, rx) = oneshot::channel();
                    let event = NormalizedEvent::Query(NormalizedQuery::TopAsk(NormalizedTop {
                        instrument: Instrument::from(instrument.to_string()),
                        n: 10,
                        reply_to: tx
                    }));
                    let event = EventEnvelope {
                        exchange,
                        event
                    };

                    if let Err(e) = self.normalized_tx.blocking_send(event) {

                    }

                    println!("{:?}", rx.blocking_recv());
                    println!("top executed");
                },
                "exit" => {
                    return;
                }
                _ => {
                    eprintln!("Error: command not recognised");
                }
            }
    
        }
    }
}