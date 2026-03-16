use std::{collections::HashMap, thread};
use binance::{connector::BinanceConnector, parser::BinanceParser, types::{BinanceMdMsg, BinanceUrls}};
use engine::Engine;
use query::query_manager::QueryManager;
use tokio::sync::mpsc::channel;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use md_core::{connector_trait::ExchangeConnector, events::{ControlEvent, EventEnvelope}, logging::{layer::DbLoggingLayer, writer::DbLoggingWriter}, types::Exchange};

mod config;
use config::load_config;
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Initializing...");
    let config = load_config("config/config.toml").expect("Load config error");

    /* LOGS */
    let (log_tx, log_rx) = tokio::sync::mpsc::channel(config.channels.log_buffer);
    let log_layer = DbLoggingLayer::new(log_tx);
    
    tracing_subscriber::registry()
        .with(log_layer)
        .init();

    tokio::spawn(async move {
        let mut log_writer =  match DbLoggingWriter::new(&config.scylladb.uri, &config.scylladb.init_path, log_rx).await {
            Ok(writer) => {writer},
            Err(e) => {
                eprintln!("Error while creating log writer: {:?}", e);
                return;
            }
        };
        if let Err(e) = log_writer.start().await {
            eprintln!("Error while running the log writer: {:?}", e);
        };
    });

    /* CONTROL CHANNELS */
    let mut control_senders = HashMap::new();

    let (binance_control_tx, binance_control_rx) = channel::<ControlEvent>(config.channels.control_buffer);
    // let (bitget_control_tx, bitget_control_rx) = channel::<ControlEvent>(config.channels.control_buffer);
    // let (bybit_control_tx, bybit_control_rx) = channel::<ControlEvent>(config.channels.control_buffer);
    // let (coinbase_control_tx, coinbase_control_rx) = channel::<ControlEvent>(config.channels.control_buffer);
    // let (kraken_control_tx, kraken_control_rx) = channel::<ControlEvent>(config.channels.control_buffer);
    // let (okx_control_tx, okx_control_rx) = channel::<ControlEvent>(config.channels.control_buffer);

    control_senders.insert(Exchange::Binance, binance_control_tx);
    // control_senders.insert(Exchange::Bitget, bitget_control_tx);
    // control_senders.insert(Exchange::Bybit, bybit_control_tx);
    // control_senders.insert(Exchange::Coinbase, coinbase_control_tx);
    // control_senders.insert(Exchange::Kraken, kraken_control_tx);
    // control_senders.insert(Exchange::Okx, okx_control_tx);

    /* ENGINE */
    let (normalized_tx, normalized_rx) = channel::<EventEnvelope>(config.channels.normalized_buffer);
    let mut engine = Engine::new(normalized_rx, control_senders);
    thread::spawn(move || {
        engine.run();
    });

    /* PARSERS */
    let (binance_raw_tx, binance_raw_rx) = channel::<BinanceMdMsg>(config.channels.raw_buffer);
    // let (bitget_raw_tx, bitget_raw_rx) = channel::<BinanceMdMsg>(config.channels.raw_buffer);
    // let (bybit_raw_tx, bybit_raw_rx) = channel::<BinanceMdMsg>(config.channels.raw_buffer);
    // let (coinbase_raw_tx, coinbase_raw_rx) = channel::<BinanceMdMsg>(config.channels.raw_buffer);
    // let (kraken_raw_tx, kraken_raw_rx) = channel::<BinanceMdMsg>(config.channels.raw_buffer);
    // let (okx_raw_tx, okx_raw_rx) = channel::<BinanceMdMsg>(config.channels.raw_buffer);

    let mut binance_parser = BinanceParser::new(binance_raw_rx, normalized_tx.clone());
    // let mut bitget_parser = BitgetParser::new(bitget_raw_rx, normalized_tx);
    // let mut bybit_parser = BybitParser::new(bybit_raw_rx, normalized_tx);
    // let mut coinbase_parser = CoinbaseParser::new(coinbase_raw_rx, normalized_tx);
    // let mut kraken_parser = KrakenParser::new(kraken_raw_rx, normalized_tx);
    // let mut okx_parser = OkxParser::new(okx_raw_rx, normalized_tx);
    
    thread::spawn(move || {
        binance_parser.run();
    });
    // thread::spawn(move || {
    //     bitget_parser.start();
    // });
    // thread::spawn(move || {
    //     bybit_parser.start();
    // });
    // thread::spawn(move || {
    //     coinbase_parser.start();
    // });
    // thread::spawn(move || {
    //     kraken_parser.start();
    // });
    // thread::spawn(move || {
    //     okx_parser.start();
    // });
    
    /* CONNECTORS */
    let binance_urls = BinanceUrls { 
        exchange_info: Url::parse(&config.binance.exchange_info)?,
        snapshot: Url::parse(&config.binance.snapshot)?, 
        ws: Url::parse(&config.binance.ws)? 
    };
    // let bitget_urls = BitgetUrls { 
    //     exchange_info: todo!(), 
    //     snapshot: todo!(), 
    //     ws: todo!() 
    // };
    // let bybit_urls = BybitUrls { 
    //     exchange_info: todo!(), 
    //     snapshot: todo!(), 
    //     ws: todo!() 
    // };
    // let coinbase_urls = CoinbaseUrls { 
    //     exchange_info: todo!(), 
    //     snapshot: todo!(), 
    //     ws: todo!() 
    // };
    // let kraken_urls = KrakenUrls { 
    //     exchange_info: todo!(), 
    //     snapshot: todo!(), 
    //     ws: todo!() 
    // };
    // let okx_urls = OkxUrls { 
    //     exchange_info: todo!(), 
    //     snapshot: todo!(), 
    //     ws: todo!() 
    // };

    let mut binance_connector = BinanceConnector::new(binance_urls, config.binance.max_subscription_per_ws, binance_raw_tx, binance_control_rx).await.unwrap();
    //let mut bitget_connector  = BitgetConnector::new(bitget_urls, config.bitget.max_subscription_per_ws, bitget_raw_tx, bitget_control_rx).await.unwrap();
    //let mut bybit_connector = BybitConnector::new(bybit_urls, config.bybit.max_subscription_per_ws, bybit_raw_tx, bybit_control_rx).await.unwrap();
    //let mut coinbase_connector = CoinbaseConnector::new(coinbase_urls, config.coinbase.max_subscription_per_ws, coinbase_raw_tx, coinbase_control_rx).await.unwrap();
    //let mut kraken_connector = KrakenConnector::new(kraken_urls, config.kraken.max_subscription_per_ws, kraken_raw_tx, kraken_control_rx).await.unwrap();
    //let mut okx_connector = OkxConnector::new(okx_urls, config.okx.max_subscription_per_ws, okx_raw_tx, okx_control_rx).await.unwrap();
    
    tokio::spawn(async move {
        binance_connector.start().await;
    });
    // tokio::spawn(async move {
    //     bitget_connector.start().await;
    // });
    // tokio::spawn(async move {
    //     bybit_connector.start().await;
    // });
    // tokio::spawn(async move {
    //     coinbase_connector.start().await;
    // });
    // tokio::spawn(async move {
    //     kraken_connector.start().await;
    // });
    // tokio::spawn(async move {
    //     okx_connector.start().await;
    // });

    /* QUERY MANAGER */
    let query_manager = QueryManager::new(normalized_tx);
    thread::spawn(move || {
        query_manager.run();
    }).join().unwrap();

    Ok(())
}
