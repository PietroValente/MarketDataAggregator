use std::{error::Error, sync::Arc};

use futures_util::StreamExt;
use md_core::{connector_trait::{ConnectionTasks, ExchangeConnector, WriteCommand}, events::InboundEvent, types::RawMdMsg};
use reqwest::Client;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info};
use url::Url;
use crate::types::{ApiResponse, BinanceMdMsg, BinanceSnapshotMsg, BinanceSubscriptionMsg, BinanceUrls, DepthQuery, SubscriptionRequest};

pub struct BinanceConnector {
    snapshot_url: Arc<Url>,
    ws_url: Arc<Url>,
    subscriptions_payloads: Vec<BinanceSubscriptionMsg>,
    subscriptions: Vec<ConnectionTasks>,
    raw_tx: Sender<BinanceMdMsg>,
    inbound_tx: Sender<InboundEvent>,
    inbound_rx: Receiver<InboundEvent>
}

impl BinanceConnector {
    pub async fn new(urls: BinanceUrls, max_subscription_per_ws: usize, raw_tx: Sender<BinanceMdMsg>) -> Result<Self, Box<dyn Error>> 
    where 
        Self: Sized
    {
        let (inbound_tx, inbound_rx) = channel::<InboundEvent>(100);
        Ok(Self {
            raw_tx,
            inbound_tx,
            inbound_rx,
            snapshot_url: Arc::from(urls.snapshot),
            ws_url: Arc::from(urls.ws),
            subscriptions: Vec::new(),
            subscriptions_payloads: BinanceConnector::build_subscriptions(&urls.exchange_info, max_subscription_per_ws).await?
        })
    }
}

impl ExchangeConnector for BinanceConnector {
    type SubscriptionPayload = BinanceSubscriptionMsg;

    async fn get_subscriptions_list(rest_url: &Url) -> Result<Vec<String>, Box<dyn Error>> {
        let client = Client::new();
        let resp = client.get(rest_url.as_str())
                            .send().await?
                            .json::<ApiResponse>().await?;
        let mut list = Vec::new();
        for symbol in resp.symbols {
            if symbol.status == "TRADING" {
                list.push(symbol.symbol);
            }
        }
        Ok(list)
    }

    async fn build_subscriptions(rest_url: &Url, max_subscription_per_ws: usize) -> Result<Vec<BinanceSubscriptionMsg>, Box<dyn Error>>{
        let mut list = BinanceConnector::get_subscriptions_list(rest_url).await?;
        let subscriptions_payloads_len = (list.len()/max_subscription_per_ws) + 1;
        let mut result = Vec::new();
        let update_settings = "@depth@100ms";
        for i in 0..subscriptions_payloads_len {
            let mut params_len = max_subscription_per_ws;
            if i == subscriptions_payloads_len - 1 { 
                params_len = list.len();
            }
            let symbols: Vec<String> = list
                .drain(..params_len)
                .collect();
            let params: Vec<String> = 
                symbols
                .clone()
                .into_iter()
                .map(|mut x| {
                    x.push_str(&update_settings);
                    x.to_lowercase()
                })
                .collect();
            let sub_req = SubscriptionRequest {
                method: String::from("SUBSCRIBE"),
                params: params.clone(),
                id: i as i64
            };
            let json = serde_json::to_string(&sub_req).expect("Failed to serialize SubscribePayload to JSON");
            let message = Message::text(json);
            result.push( BinanceSubscriptionMsg{
                symbols: symbols,
                payload: message
            });
        }
        Ok(result)
    }

    /* The core idea is to subscribe to ws to get the update but let them stay in the ws buffer.
        Request the snapshot through rest api and only after that start reading the updates.
     */
    async fn subscribe_streams(&mut self) -> Result<(), Box<dyn Error>> {
        self.abort_streams();
        for message in &self.subscriptions_payloads {
            let (writer_tx, writer_rx) = channel::<WriteCommand>(100);
            let (ws_stream, _) = connect_async(self.ws_url.as_str()).await?;
            let (write, read) = ws_stream.split();

            let reader_url = self.ws_url.clone();
            let writer_url = self.ws_url.clone();

            let writer_handle = tokio::spawn(async move {
                BinanceConnector::writer_task(reader_url, write, writer_rx).await;
            });

            let w = WriteCommand::Raw(message.payload.clone());
            writer_tx.send(w).await?;

            let client = Client::new();
            for symbol in &message.symbols {
                let params = DepthQuery {
                    symbol: &symbol,
                    limit: 5000,
                };

                let response = client
                    .get(self.snapshot_url.as_str())
                    .query(&params)
                    .send()
                    .await?;

                self.raw_tx.send(BinanceMdMsg::Snapshot(BinanceSnapshotMsg {
                    symbol: symbol.clone(),
                    payload: RawMdMsg {
                        payload: response.bytes().await?.to_vec()
                    }
                })).await?;
            }
            
            let reader_tx_clone = self.inbound_tx.clone();
            let reader_handle = tokio::spawn(async move {
                BinanceConnector::reader_task(writer_url, read, reader_tx_clone).await;
            });

            if let Some(event) = self.inbound_rx.recv().await {
                match event {
                    InboundEvent::WsMessage(payload) => {
                        let msg = String::from_utf8(payload.payload)?;
                        if !msg.contains(r#""result": null,"#) {
                            error!(snapshot_url = ?self.snapshot_url, symbols= ?message.symbols, "subscription not confirmed from the server");
                        }
                    },
                    _ => {
                        error!(snapshot_url = ?self.snapshot_url, symbols= ?message.symbols, "InboundEvent isn't type of WsMessage while waiting for subscription confirmation");
                    }
                }
            }

            let connection = ConnectionTasks {
                reader_handle,
                writer_handle,
                writer_tx
            };
            self.subscriptions.push(connection);
        }
        Ok(())
    }

    fn abort_streams(&mut self) {
        for subscription in &self.subscriptions {
            subscription.reader_handle.abort();
            subscription.writer_handle.abort();
        }
        self.subscriptions.clear();
    }

    async fn pong_all(&self, payload: Vec<u8>) -> Result<(), Box<dyn Error>> {
        for subscription in &self.subscriptions {
            subscription.writer_tx.send(WriteCommand::Pong(payload.clone())).await?;
        }
        Ok(())
    }

    async fn start(&mut self) {
        // TODO: manage problem connecting with backoff + jitter
        self.subscribe_streams().await.unwrap();
        while let Some(msg) = self.inbound_rx.recv().await {
            match msg {
                InboundEvent::WsMessage(payload) => {
                    self.raw_tx.send(BinanceMdMsg::Update(payload)).await.unwrap();
                },
                InboundEvent::Ping(payload) => {
                    // TODO: fix, should pong only the correct ws
                    self.pong_all(payload).await.unwrap();
                },
                InboundEvent::ConnectionClosed => {
                    info!(url = ?self.ws_url, "connection close, reconnecting");
                    self.subscribe_streams().await.unwrap();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use md_core::connector_trait::ExchangeConnector;
    use tokio::sync::mpsc::channel;
    use url::Url;

    use crate::{connector::{self, BinanceConnector}, types::{BinanceMdMsg, BinanceUrls}};

    #[tokio::test]
    async fn create_new_binance_connector() {
        let urls = BinanceUrls {
            exchange_info: Url::parse("https://api2.binance.com/api/v3/exchangeInfo?permissions=SPOT").unwrap(),
            snapshot: Url::parse("https://api.binance.com/api/v3/depth").unwrap(),
            ws: Url::parse("wss://stream.binance.com:443/ws").unwrap()
        };
        let (tx, mut rx) = channel::<BinanceMdMsg>(100);
        let mut connector = BinanceConnector::new(urls, 50, tx).await.unwrap();
        connector.start().await;

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
    }
}