use std::{collections::HashMap, thread};

use binance::{connector::BinanceConnector, parser::BinanceParser, types::{BinanceMdMsg, BinanceUrls}};
use engine::Engine;
use tokio::sync::mpsc::channel;
use url::Url;
use md_core::{connector_trait::ExchangeConnector, events::{ControlEvent, EventEnvelope}, types::Exchange};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let urls = BinanceUrls {
        exchange_info: Url::parse("https://api2.binance.com/api/v3/exchangeInfo?permissions=SPOT").unwrap(),
        snapshot: Url::parse("https://api.binance.com/api/v3/depth").unwrap(),
        ws: Url::parse("wss://stream.binance.com:443/ws").unwrap()
    };

    let (raw_tx, raw_rx) = channel::<BinanceMdMsg>(4096);
    let (normalized_tx, normalized_rx) = channel::<EventEnvelope>(4096);
    let (control_tx, control_rx) = channel::<ControlEvent>(8);
    let mut control_senders = HashMap::new();
    control_senders.insert(Exchange::Binance, control_tx);

    let mut engine = Engine::new(normalized_rx, control_senders);
    let handler = thread::spawn(move || {
        engine.run();
    });

    let mut connector = BinanceConnector::new(urls, 50, raw_tx, control_rx).await.unwrap();
    tokio::spawn(async move {
        connector.start().await;
    });

    let mut parser = BinanceParser::new(raw_rx, normalized_tx);
    tokio::spawn(async move {
        parser.start().await;
    });

    handler.join().unwrap();

    Ok(())
}
