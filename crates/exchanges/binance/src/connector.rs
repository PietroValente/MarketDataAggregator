use std::{error::Error, sync::Arc};

use md_core::{connector::{tasks::{connection_manager_task, control_manager_task}, types::{ConnectorError, ManagerCommand, BACKOFF_SECS}}, events::{ControlEvent, InboundEvent, PingMsg}, helpers::connector::{fetch_json, retry_with_backoff}, traits::connector::ExchangeConnector, types::{Exchange, Instrument}};
use reqwest::Client;
use tokio::{sync::mpsc::{channel, Receiver, Sender}, time::Duration};
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, warn};
use url::Url;

use crate::{helpers::recreate_with_snapshots, types::{ApiResponse, BinanceMdMsg, BinanceUrls, SubscriptionBatch, SubscriptionRequest, Subscriptions}};

pub struct BinanceConnector {
    manager_tx: Sender<ManagerCommand>,
    raw_tx: Sender<BinanceMdMsg>,
    inbound_rx: Receiver<InboundEvent>
}

impl BinanceConnector {
    pub async fn new(client:Client, urls: BinanceUrls, max_subscription_per_ws: usize, raw_tx: Sender<BinanceMdMsg>, control_rx: Receiver<ControlEvent>) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> 
    where 
        Self: Sized
    {
        let (inbound_tx, inbound_rx) = channel::<InboundEvent>(4096);
        let snapshot_url = Arc::from(urls.snapshot);
        let ws_url = Arc::from(urls.ws);
        
        let subscriptions = BinanceConnector::build_subscriptions(client, &urls.exchange_info, max_subscription_per_ws).await?;
        raw_tx
            .send(BinanceMdMsg::Instruments(subscriptions.symbols))
            .await?;
        let subscriptions_payloads = Arc::new(subscriptions.batches);

        let (manager_tx, manager_rx) = channel::<ManagerCommand>(128);

        tokio::spawn(connection_manager_task::<BinanceConnector, Vec<SubscriptionBatch>, BinanceMdMsg, _, _>(
            ws_url.clone(),
            Some(snapshot_url),
            subscriptions_payloads,
            inbound_tx.clone(),
            Some(raw_tx.clone()),
            manager_tx.clone(),
            manager_rx,
            recreate_with_snapshots
        ));

        tokio::spawn(control_manager_task::<BinanceConnector>(
            control_rx, 
            manager_tx.clone(),
            inbound_tx
        ));

        Ok(Self {
            raw_tx,
            inbound_rx,
            manager_tx
        })
    } 

    async fn get_subscriptions_list(client: Client, rest_url: &Url) -> Result<Vec<Instrument>, Box<dyn Error + Send + Sync + 'static>> {     
        let resp  = fetch_json::<ApiResponse>( &client, rest_url, Some(Duration::from_secs(60))).await?;
        
        let list: Vec<Instrument> = resp.symbols
            .into_iter()
            .filter(|s| s.status == "TRADING")
            .map(|s| s.symbol)
            .collect();

        Ok(list)
    }

    async fn build_subscriptions(client: Client, rest_url: &Url, max_subscription_per_ws: usize) -> Result<Subscriptions, Box<dyn Error + Send + Sync + 'static>> {
        if max_subscription_per_ws == 0 {
            return Err(ConnectorError::InvalidMaxSubscriptionPerWs.into());
        }

        let symbols = retry_with_backoff(
            &BACKOFF_SECS,
            || BinanceConnector::get_subscriptions_list(client.clone(), rest_url),
            |e, attempt: usize, delay: Duration| {
                error!(
                    exchange = ?BinanceConnector::exchange(),
                    component = ?BinanceConnector::component(),
                    error = ?e,
                    attempt = attempt + 1,
                    delay_ms = delay.as_millis() as u64,
                    "error while building subscriptions"
                );
            },
        ).await;

        let mut batches = Vec::new();
        let update_settings = "@depth@100ms";
    
        for (i, chunk) in symbols.chunks(max_subscription_per_ws).enumerate() {
            let batch_symbols: Vec<Instrument> = chunk.to_vec();
    
            let params: Vec<String> = batch_symbols
                .iter()
                .map(|x| {
                    let mut s = x.to_string().to_lowercase();
                    s.push_str(update_settings);
                    s
                })
                .collect();
    
            let sub_req = SubscriptionRequest {
                method: String::from("SUBSCRIBE"),
                params,
                id: i as i64,
            };
    
            let json = serde_json::to_string(&sub_req)?;
            let message = Message::text(json);
    
            batches.push(SubscriptionBatch {
                symbols: batch_symbols,
                message,
            });
        }
        Ok(Subscriptions { symbols, batches })
    }

    async fn subscribe_streams(&mut self) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        self.manager_tx
            .send(ManagerCommand::RecreateWithSnapshots)
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    async fn pong(&self, msg: PingMsg) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        self.manager_tx
            .send(ManagerCommand::Pong(msg))
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    async fn start(&mut self) {
        if let Err(e) = self.subscribe_streams().await {
            error!(exchange = ?BinanceConnector::exchange(), component = ?BinanceConnector::component(), error = ?e, "error while subscribing to streams");
            return;
        }
        while let Some(msg) = self.inbound_rx.recv().await {
            match msg {
                InboundEvent::ClearBookState => {
                    if let Err(e) = self.raw_tx.send(BinanceMdMsg::ResetBookState).await {
                        error!(exchange = ?BinanceConnector::exchange(), component = ?BinanceConnector::component(), error = ?e, "error while sending the ClearBookState command");
                        continue;
                    }
                },
                InboundEvent::WsMessage(payload) => {
                    if let Err(e) = self.raw_tx.send(BinanceMdMsg::WsMessage(payload)).await {
                        error!(exchange = ?BinanceConnector::exchange(), component = ?BinanceConnector::component(), error = ?e, "error while sending the update message");
                        continue;
                    }
                },
                InboundEvent::Ping(ping_msg) => {
                    if let Err(e) = self.pong(ping_msg).await {
                        error!(exchange = ?BinanceConnector::exchange(), component = ?BinanceConnector::component(), error = ?e, "error while sending the pong command");
                        continue;
                    }
                },
                InboundEvent::ConnectionClosed => {
                    warn!(
                        exchange = ?BinanceConnector::exchange(),
                        component = ?BinanceConnector::component(),
                        reason = "connection_closed",
                        "connection closed, scheduling reconnect"
                    );
                    if let Err(e) = self.subscribe_streams().await {
                        error!(exchange = ?BinanceConnector::exchange(), component = ?BinanceConnector::component(), error = ?e, "error while subscribing to streams");
                        continue;
                    }
                }
            }
        }
    }

}

impl ExchangeConnector for BinanceConnector {
    type SubscriptionsInfo = Subscriptions;

    fn exchange() -> Exchange {
        Exchange::Binance
    }

    async fn build_subscriptions(
        client: Client,
        rest_url: &Url,
        max_subscription_per_ws: usize,
    ) -> Result<Self::SubscriptionsInfo, Box<dyn Error + Send + Sync + 'static>> {
        BinanceConnector::build_subscriptions(client, rest_url, max_subscription_per_ws).await
    }

    async fn subscribe_streams(
        &mut self,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        BinanceConnector::subscribe_streams(self).await
    }

    async fn pong(
        &self,
        msg: PingMsg,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        BinanceConnector::pong(self, msg).await
    }

    async fn start(&mut self) {
        BinanceConnector::start(self).await
    }
}