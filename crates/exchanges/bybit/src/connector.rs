use std::{error::Error, sync::Arc};

use md_core::{
    connector::{
        tasks::{ConnectionManagerParams, connection_manager_task, control_manager_task},
        types::{BACKOFF_SECS, ConnectorError, ManagerCommand},
    },
    events::{ControlEvent, InboundEvent, PingMsg},
    helpers::connector::{fetch_json, recreate_with_snapshots, retry_with_backoff},
    traits::connector::ExchangeConnector,
    types::{Exchange, Instrument},
};
use reqwest::Client;
use tokio::{
    sync::mpsc::{Receiver, Sender, channel},
    time::Duration,
};
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, warn};
use url::Url;

use crate::types::{ApiResponse, BybitMdMsg, BybitUrls, SubscriptionRequest, Subscriptions};

pub struct BybitConnector {
    manager_tx: Sender<ManagerCommand>,
    raw_tx: Sender<BybitMdMsg>,
    inbound_rx: Receiver<InboundEvent>,
}

impl BybitConnector {
    pub async fn new(
        client: Client,
        urls: BybitUrls,
        max_subscription_per_ws: usize,
        raw_tx: Sender<BybitMdMsg>,
        control_rx: Receiver<ControlEvent>,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>>
    where
        Self: Sized,
    {
        let (inbound_tx, inbound_rx) = channel::<InboundEvent>(4096);
        let ws_url = Arc::from(urls.ws);

        let subscriptions = BybitConnector::build_subscriptions(
            client,
            &urls.exchange_info,
            max_subscription_per_ws,
        )
        .await?;
        raw_tx
            .send(BybitMdMsg::Instruments(subscriptions.symbols))
            .await?;
        let subscriptions_payloads = Arc::new(subscriptions.messages);

        let (manager_tx, manager_rx) = channel::<ManagerCommand>(128);

        tokio::spawn(connection_manager_task::<
            BybitConnector,
            Vec<Message>,
            BybitMdMsg,
            _,
            _,
        >(
            ConnectionManagerParams {
                ws_url: ws_url.clone(),
                snapshot_url: None,
                subscriptions_payloads,
                inbound_tx: inbound_tx.clone(),
                raw_tx: None,
                cmd_tx: manager_tx.clone(),
                cmd_rx: manager_rx,
            },
            recreate_with_snapshots::<BybitConnector, BybitMdMsg>,
        ));

        tokio::spawn(control_manager_task::<BybitConnector>(
            control_rx,
            manager_tx.clone(),
        ));

        Ok(Self {
            raw_tx,
            inbound_rx,
            manager_tx,
        })
    }

    async fn get_subscriptions_list(
        client: Client,
        rest_url: &Url,
    ) -> Result<Vec<Instrument>, Box<dyn Error + Send + Sync + 'static>> {
        let resp = fetch_json::<ApiResponse>(&client, rest_url, None).await?;

        let list: Vec<Instrument> = resp
            .result
            .list
            .into_iter()
            .filter(|s| s.status == "Trading")
            .map(|s| s.symbol)
            .collect();

        Ok(list)
    }

    async fn build_subscriptions(
        client: Client,
        rest_url: &Url,
        max_subscription_per_ws: usize,
    ) -> Result<Subscriptions, Box<dyn Error + Send + Sync + 'static>> {
        if max_subscription_per_ws == 0 {
            return Err(ConnectorError::InvalidMaxSubscriptionPerWs.into());
        }

        let symbols = retry_with_backoff(
            &BACKOFF_SECS,
            || BybitConnector::get_subscriptions_list(client.clone(), rest_url),
            |e, attempt: usize, delay: Duration| {
                error!(
                    exchange = ?BybitConnector::exchange(),
                    component = ?BybitConnector::component(),
                    error = ?e,
                    attempt = attempt + 1,
                    delay_ms = delay.as_millis() as u64,
                    "error while building subscriptions"
                );
            },
        )
        .await;

        let mut messages = Vec::new();
        let update_prefix = "orderbook.200.";

        for chunk in symbols.chunks(max_subscription_per_ws) {
            let args: Vec<String> = chunk
                .iter()
                .map(|x| {
                    let mut s = x.0.clone();
                    s.insert_str(0, update_prefix);
                    s
                })
                .collect();

            let sub_req = SubscriptionRequest {
                op: String::from("subscribe"),
                args,
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
            error!(exchange = ?BybitConnector::exchange(), component = ?BybitConnector::component(), error = ?e, "error while subscribing to streams");
            return;
        }
        while let Some(msg) = self.inbound_rx.recv().await {
            match msg {
                InboundEvent::ClearBookState => {
                    if let Err(e) = self.raw_tx.send(BybitMdMsg::ResetBookState).await {
                        error!(exchange = ?BybitConnector::exchange(), component = ?BybitConnector::component(), error = ?e, "error while sending the ClearBookState command");
                        continue;
                    }
                }
                InboundEvent::WsMessage(payload) => {
                    if let Err(e) = self.raw_tx.send(BybitMdMsg::Raw(payload)).await {
                        error!(exchange = ?BybitConnector::exchange(), component = ?BybitConnector::component(), error = ?e, "error while sending the ws message");
                        continue;
                    }
                }
                InboundEvent::Ping(ping_msg) => {
                    if let Err(e) = self.pong(ping_msg).await {
                        error!(exchange = ?BybitConnector::exchange(), component = ?BybitConnector::component(), error = ?e, "error while sending the pong command");
                        continue;
                    }
                }
                InboundEvent::ConnectionClosed => {
                    warn!(
                        exchange = ?BybitConnector::exchange(),
                        component = ?BybitConnector::component(),
                        reason = "connection_closed",
                        "connection closed, scheduling reconnect"
                    );
                    if let Err(e) = self.subscribe_streams().await {
                        error!(exchange = ?BybitConnector::exchange(), component = ?BybitConnector::component(), error = ?e, "error while subscribing to streams");
                        continue;
                    }
                }
            }
        }
    }
}

impl ExchangeConnector for BybitConnector {
    type SubscriptionsInfo = Subscriptions;

    fn exchange() -> Exchange {
        Exchange::Bybit
    }

    async fn build_subscriptions(
        client: Client,
        rest_url: &Url,
        max_subscription_per_ws: usize,
    ) -> Result<Self::SubscriptionsInfo, Box<dyn Error + Send + Sync + 'static>> {
        BybitConnector::build_subscriptions(client, rest_url, max_subscription_per_ws).await
    }

    async fn subscribe_streams(&mut self) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        BybitConnector::subscribe_streams(self).await
    }

    async fn pong(&self, msg: PingMsg) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        BybitConnector::pong(self, msg).await
    }

    async fn start(&mut self) {
        BybitConnector::start(self).await
    }
}
