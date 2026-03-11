use binance::{connector::BinanceConnector, parser::BinanceParser, types::{BinanceMdMsg, BinanceUrls}};
use tokio::sync::mpsc::channel;
use url::Url;
use md_core::{connector_trait::ExchangeConnector, events::EventEnvelope};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let urls = BinanceUrls {
        exchange_info: Url::parse("https://api2.binance.com/api/v3/exchangeInfo?permissions=SPOT").unwrap(),
        snapshot: Url::parse("https://api.binance.com/api/v3/depth").unwrap(),
        ws: Url::parse("wss://stream.binance.com:443/ws").unwrap()
    };
    let (raw_tx, raw_rx) = channel::<BinanceMdMsg>(4096);
    let (normalized_tx, mut normalized_rx) = channel::<EventEnvelope>(4096);
    let mut connector = BinanceConnector::new(urls, 50, raw_tx).await.unwrap();
    tokio::spawn(async move {
        connector.start().await;
    });

    let mut parser = BinanceParser::new(raw_rx, normalized_tx);
    tokio::spawn(async move {
        parser.start().await;
    });

    while let Some(_msg) = normalized_rx.recv().await {
        //println!("{:?}", msg);
    }

    Ok(())
}
