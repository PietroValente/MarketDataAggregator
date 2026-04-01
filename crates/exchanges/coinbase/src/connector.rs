use std::{error::Error, sync::Arc};

use md_core::{connector::{tasks::{connection_manager_task, control_manager_task}, types::{ConnectorError, ManagerCommand, BACKOFF_SECS}}, events::{ControlEvent, InboundEvent, PingMsg}, helpers::connector::{fetch_json, recreate_with_snapshots, retry_with_backoff}, traits::connector::ExchangeConnector, types::{Exchange, Instrument}};
use reqwest::Client;
use tokio::{sync::mpsc::{channel, Receiver, Sender}, time::Duration};
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info};
use url::Url;

use crate::types::{ApiResponse, CoinbaseMdMsg, CoinbaseUrls, SubscriptionMsg, Subscriptions, SymbolParam};

pub struct CoinbaseConnector {
    manager_tx: Sender<ManagerCommand>,
    raw_tx: Sender<CoinbaseMdMsg>,
    inbound_rx: Receiver<InboundEvent>
}

impl CoinbaseConnector {
    pub async fn new(client:Client, urls: CoinbaseUrls, max_subscription_per_ws: usize, raw_tx: Sender<CoinbaseMdMsg>, control_rx: Receiver<ControlEvent>) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> 
    where 
        Self: Sized
    {
        let (inbound_tx, inbound_rx) = channel::<InboundEvent>(4096);
        let ws_url = Arc::from(urls.ws);
        
        let subscriptions = CoinbaseConnector::build_subscriptions(client, &urls.exchange_info, max_subscription_per_ws).await?;
        raw_tx
            .send(CoinbaseMdMsg::Instruments(subscriptions.symbols))
            .await?;
        let subscriptions_payloads = Arc::new(subscriptions.messages);

        let (manager_tx, manager_rx) = channel::<ManagerCommand>(128);

        tokio::spawn(connection_manager_task::<CoinbaseConnector, Vec<Message>, CoinbaseMdMsg, _, _>(
            ws_url.clone(),
            None,
            subscriptions_payloads,
            inbound_tx.clone(),
            None,
            manager_tx.clone(),
            manager_rx,
            recreate_with_snapshots::<CoinbaseConnector, CoinbaseMdMsg>
        ));

        tokio::spawn(control_manager_task::<CoinbaseConnector>(
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

    async fn get_subscriptions_list(client:Client, rest_url: &Url) -> Result<Vec<Instrument>, Box<dyn Error + Send + Sync + 'static>> {
        let resp  = fetch_json::<ApiResponse>( &client, rest_url, None).await?;
        
        let list: Vec<Instrument> = resp.products
            .into_iter()
            .filter(|s| s.status == "online" && !s.product_id.contains("USDC"))
            .map(|s| s.product_id)
            .collect();

        Ok(list)
    }

    async fn build_subscriptions(client: Client, rest_url: &Url, max_subscription_per_ws: usize) -> Result<Subscriptions, Box<dyn Error + Send + Sync + 'static>> {
        if max_subscription_per_ws == 0 {
            return Err(ConnectorError::InvalidMaxSubscriptionPerWs.into());
        }

        let symbols = retry_with_backoff(
            &BACKOFF_SECS,
            || CoinbaseConnector::get_subscriptions_list(client.clone(), rest_url),
            |e, attempt: usize, delay: Duration| {
                error!(exchange = ?CoinbaseConnector::exchange(), component = ?CoinbaseConnector::component(), error = ?e, attempt = ?attempt, delay = ?delay, "error while building subscriptions");
            },
        ).await;
        
        let mut messages = Vec::new();
    
        for chunk in symbols.chunks(max_subscription_per_ws) {
            let product_ids: Vec<Instrument> = chunk.to_vec();
    
            let sub_req = SubscriptionMsg {
                op_type: String::from("subscribe"),
                channels: vec![SymbolParam {
                    name: String::from("level2_50"),
                    product_ids,
                }],
            };
    
            let json = serde_json::to_string(&sub_req)?;
            let message = Message::text(json);
    
            messages.push(message);
        }
    
        Ok(Subscriptions { symbols, messages })
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
            error!(exchange = ?CoinbaseConnector::exchange(), component = ?CoinbaseConnector::component(), error = ?e, "error while subscribing to streams");
            return;
        }
        while let Some(msg) = self.inbound_rx.recv().await {
            match msg {
                InboundEvent::ClearBookState => {
                    if let Err(e) = self.raw_tx.send(CoinbaseMdMsg::ResetBookState).await {
                        error!(exchange = ?CoinbaseConnector::exchange(), component = ?CoinbaseConnector::component(), error = ?e, "error while sending the ClearBookState command");
                        continue;
                    }
                },
                InboundEvent::WsMessage(payload) => {
                    if let Err(e) = self.raw_tx.send(CoinbaseMdMsg::Raw(payload)).await {
                        error!(exchange = ?CoinbaseConnector::exchange(), component = ?CoinbaseConnector::component(), error = ?e, "error while sending the ws message");
                        continue;
                    }
                },
                InboundEvent::Ping(ping_msg) => {
                    if let Err(e) = self.pong(ping_msg).await {
                        error!(exchange = ?CoinbaseConnector::exchange(), component = ?CoinbaseConnector::component(), error = ?e, "error while sending the pong command");
                        continue;
                    }
                },
                InboundEvent::ConnectionClosed => {
                    info!(exchange = ?CoinbaseConnector::exchange(), component = ?CoinbaseConnector::component(), "connection close, reconnecting");
                    if let Err(e) = self.subscribe_streams().await {
                        error!(exchange = ?CoinbaseConnector::exchange(), component = ?CoinbaseConnector::component(), error = ?e, "error while subscribing to streams");
                        continue;
                    }
                }
            }
        }
    }

}

impl ExchangeConnector for CoinbaseConnector {
    type SubscriptionsInfo = Subscriptions;

    fn exchange() -> Exchange {
        Exchange::Coinbase
    }

    async fn build_subscriptions(
        client: Client,
        rest_url: &Url,
        max_subscription_per_ws: usize,
    ) -> Result<Self::SubscriptionsInfo, Box<dyn Error + Send + Sync + 'static>> {
        CoinbaseConnector::build_subscriptions(client, rest_url, max_subscription_per_ws).await
    }

    async fn subscribe_streams(
        &mut self,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        CoinbaseConnector::subscribe_streams(self).await
    }

    async fn pong(
        &self,
        msg: PingMsg,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        CoinbaseConnector::pong(self, msg).await
    }

    async fn start(&mut self) {
        CoinbaseConnector::start(self).await
    }
}