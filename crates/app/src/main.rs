use binance::{connector::BinanceConnector, types::{BinanceMdMsg, BinanceUrls}};
use tokio::sync::mpsc::channel;
use url::Url;
use md_core::connector_trait::ExchangeConnector;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let urls = BinanceUrls {
        exchange_info: Url::parse("https://api2.binance.com/api/v3/exchangeInfo?permissions=SPOT").unwrap(),
        snapshot: Url::parse("https://api.binance.com/api/v3/depth").unwrap(),
        ws: Url::parse("wss://stream.binance.com:443/ws").unwrap()
    };
    let (tx, mut rx) = channel::<BinanceMdMsg>(100);
    let mut connector = BinanceConnector::new(urls, 50, tx).await.unwrap();
    tokio::spawn(async move {
        connector.start().await;
    });

    for _ in 0..5 {
        let msg = rx.recv().await.unwrap();
        match msg {
            BinanceMdMsg::Snapshot(snapshot) => {
                println!("{}", String::from_utf8(snapshot.payload.payload).unwrap());
            },
            BinanceMdMsg::Update(payload) => {
                println!("{}", String::from_utf8(payload.payload).unwrap());
            }
        }
    }

    Ok(())
}
