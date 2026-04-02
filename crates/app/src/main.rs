use std::{collections::HashMap, thread, time::Duration};

use reqwest::Client;
use tokio::sync::mpsc::channel;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use url::Url;

use binance::{adapter::BinanceAdapter, connector::BinanceConnector, types::{BinanceMdMsg, BinanceUrls}};
use bitget::{adapter::BitgetAdapter, connector::BitgetConnector, types::{BitgetMdMsg, BitgetUrls}};
use bybit::{adapter::BybitAdapter, connector::BybitConnector, types::{BybitMdMsg, BybitUrls}};
use coinbase::{adapter::CoinbaseAdapter, connector::CoinbaseConnector, types::{CoinbaseMdMsg, CoinbaseUrls}};
use okx::{adapter::OkxAdapter, connector::OkxConnector, types::{OkxMdMsg, OkxUrls}};

use engine::Engine;
use md_core::{events::{ControlEvent, EngineMessage}, logging::{layer::DbLoggingLayer, writer::DbLoggingWriter}, traits::connector::ExchangeConnector, types::Exchange};
use query::query_manager::QueryManager;

mod config;
use config::load_config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config("config/config.toml").expect("Load config error");

    /* LOGS */
    let (log_tx, log_rx) = tokio::sync::mpsc::channel(config.channels.log_buffer);
    let log_layer = DbLoggingLayer::new(log_tx);
    
    tracing_subscriber::registry()
        .with(log_layer)
        .init();

    let _ = tokio::spawn(async move {
        let mut log_writer =  match DbLoggingWriter::new(&config.scylladb.uri, &config.scylladb.init_path, log_rx).await {
            Ok(writer) => {writer},
            Err(e) => {
                eprintln!("Logging disabled: failed to initialize DB writer");
                eprintln!("Error: {:?}", e);
                eprintln!("-------------------------------------");
                return;
            }
        };
        tokio::spawn(async move {
            if let Err(e) = log_writer.start().await {
                eprintln!("Error while running the log writer: {:?}", e);
            }
        });
    }).await;

    /* CONTROL CHANNELS */
    let (binance_control_tx, binance_control_rx) = channel::<ControlEvent>(config.channels.control_buffer);
    let (bitget_control_tx, bitget_control_rx) = channel::<ControlEvent>(config.channels.control_buffer);
    let (bybit_control_tx, bybit_control_rx) = channel::<ControlEvent>(config.channels.control_buffer);
    let (coinbase_control_tx, coinbase_control_rx) = channel::<ControlEvent>(config.channels.control_buffer);
    let (okx_control_tx, okx_control_rx) = channel::<ControlEvent>(config.channels.control_buffer);

    /* ENGINE */
    let exchanges = HashMap::from([
        (Exchange::Binance, binance_control_tx.clone()),
        (Exchange::Bitget, bitget_control_tx.clone()),
        (Exchange::Bybit, bybit_control_tx.clone()),
        (Exchange::Coinbase, coinbase_control_tx.clone()),
        (Exchange::Okx, okx_control_tx.clone())
    ]);
    let (normalized_tx, normalized_rx) = channel::<EngineMessage>(config.channels.normalized_buffer);
    let mut engine = Engine::new(normalized_rx, exchanges);
    thread::spawn(move || {
        engine.run();
    });

    /* ADAPTERS */
    let (binance_raw_tx, binance_raw_rx) = channel::<BinanceMdMsg>(config.channels.raw_buffer);
    let (bitget_raw_tx, bitget_raw_rx) = channel::<BitgetMdMsg>(config.channels.raw_buffer);
    let (bybit_raw_tx, bybit_raw_rx) = channel::<BybitMdMsg>(config.channels.raw_buffer);
    let (coinbase_raw_tx, coinbase_raw_rx) = channel::<CoinbaseMdMsg>(config.channels.raw_buffer);
    let (okx_raw_tx, okx_raw_rx) = channel::<OkxMdMsg>(config.channels.raw_buffer);

    let mut binance_adapter = BinanceAdapter::new(binance_raw_tx.clone(), binance_raw_rx, normalized_tx.clone(), binance_control_tx);
    let mut bitget_adapter = BitgetAdapter::new(bitget_raw_rx, normalized_tx.clone(), bitget_control_tx);
    let mut bybit_adapter = BybitAdapter::new(bybit_raw_rx, normalized_tx.clone(), bybit_control_tx);
    let mut coinbase_adapter = CoinbaseAdapter::new(coinbase_raw_rx, normalized_tx.clone(), coinbase_control_tx);
    let mut okx_adapter = OkxAdapter::new(okx_raw_rx, normalized_tx.clone(), okx_control_tx);
    
    thread::spawn(move || {
        binance_adapter.run();
    });
    thread::spawn(move || {
        bitget_adapter.run();
    });
    thread::spawn(move || {
        bybit_adapter.run();
    });
    thread::spawn(move || {
        coinbase_adapter.run();
    });
    thread::spawn(move || {
        okx_adapter.run();
    });
    
    /* CONNECTORS */
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(8))
        .timeout(Duration::from_secs(20))
        .build()?;
    let binance_client = client.clone();
    let bitget_client = client.clone();
    let bybit_client = client.clone();
    let coinbase_client = client.clone();
    let okx_client = client.clone();

    let binance_urls = BinanceUrls { 
        exchange_info: Url::parse(&config.binance.exchange_info)?,
        snapshot: Url::parse(&config.binance.snapshot)?, 
        ws: Url::parse(&config.binance.ws)? 
    };
    let bitget_urls = BitgetUrls { 
        exchange_info: Url::parse(&config.bitget.exchange_info)?,
        ws: Url::parse(&config.bitget.ws)? 
    };
    let bybit_urls = BybitUrls { 
        exchange_info: Url::parse(&config.bybit.exchange_info)?,
        ws: Url::parse(&config.bybit.ws)? 
    };
    let coinbase_urls = CoinbaseUrls { 
        exchange_info: Url::parse(&config.coinbase.exchange_info)?,
        ws: Url::parse(&config.coinbase.ws)? 
    };
    let okx_urls = OkxUrls { 
        exchange_info: Url::parse(&config.okx.exchange_info)?,
        ws: Url::parse(&config.okx.ws)? 
    };
    
    tokio::spawn(async move {
        let mut binance_connector = BinanceConnector::new(binance_client, binance_urls, config.binance.max_subscription_per_ws, binance_raw_tx, binance_control_rx).await.unwrap();
        binance_connector.start().await;
    });
    tokio::spawn(async move {
        let mut bitget_connector  = BitgetConnector::new(bitget_client, bitget_urls, config.bitget.max_subscription_per_ws, bitget_raw_tx, bitget_control_rx).await.unwrap();
        bitget_connector.start().await;
    });
    tokio::spawn(async move {
        let mut bybit_connector = BybitConnector::new(bybit_client, bybit_urls, config.bybit.max_subscription_per_ws, bybit_raw_tx, bybit_control_rx).await.unwrap();
        bybit_connector.start().await;
    });
    tokio::spawn(async move {
        let mut coinbase_connector = CoinbaseConnector::new(coinbase_client, coinbase_urls, config.coinbase.max_subscription_per_ws, coinbase_raw_tx, coinbase_control_rx).await.unwrap();
        coinbase_connector.start().await;
    });
    tokio::spawn(async move {
        let mut okx_connector = OkxConnector::new(okx_client, okx_urls, config.okx.max_subscription_per_ws, okx_raw_tx, okx_control_rx).await.unwrap();
        okx_connector.start().await;
    });

    /* QUERY MANAGER */
    let query_manager = QueryManager::new(normalized_tx);
    thread::spawn(move || {
        query_manager.run();
    }).join().unwrap();

    Ok(())
}
