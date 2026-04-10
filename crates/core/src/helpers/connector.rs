use std::{collections::HashMap, error::Error, sync::Arc, time::Duration};

use bytes::BytesMut;
use futures_util::StreamExt;
use reqwest::Client;
use serde::de::DeserializeOwned;
use tokio::{
    sync::mpsc::{Sender, channel},
    time::sleep,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::error;
use url::Url;

use crate::{
    connector::{
        tasks::{reader_task, writer_task},
        types::{ConnectionTasks, ConnectorError, ManagerCommand, WriteCommand},
    },
    events::{InboundEvent, PingMsg},
    traits::connector::ExchangeConnector,
};

pub fn ws_id_capacity() -> usize {
    usize::from(u8::MAX) + 1
}

pub fn ws_id_from_index(index: usize) -> Option<u8> {
    u8::try_from(index).ok()
}

pub async fn fetch_json<T>(
    client: &Client,
    url: &Url,
    timeout: Option<Duration>,
) -> Result<T, Box<dyn Error + Send + Sync + 'static>>
where
    T: DeserializeOwned,
{
    let mut req = client.get(url.as_str());

    if let Some(t) = timeout {
        req = req.timeout(t);
    }

    let resp = req.send().await?.error_for_status()?;

    let mut stream = resp.bytes_stream();
    let mut body = BytesMut::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        body.extend_from_slice(&chunk);
    }

    let parsed = serde_json::from_slice::<T>(&body)?;
    Ok(parsed)
}

pub async fn retry_with_backoff<T, E, F, Fut, OnError>(
    backoff_delays: &[Duration],
    mut op: F,
    mut on_error: OnError,
) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    OnError: FnMut(&E, usize, Duration),
{
    let mut attempt = 0;

    loop {
        match op().await {
            Ok(value) => return value,
            Err(err) => {
                let delay = backoff_delays
                    .get(attempt)
                    .copied()
                    .or_else(|| backoff_delays.last().copied())
                    .unwrap_or(Duration::from_secs(1));

                on_error(&err, attempt, delay);

                attempt = attempt.saturating_add(1);
                sleep(delay).await;
            }
        }
    }
}

pub fn abort_all_connections(connections: &mut HashMap<u8, ConnectionTasks>) {
    for (_, c) in connections.drain() {
        c.reader_handle.abort();
        c.writer_handle.abort();
    }
}

pub async fn pong_ws<T>(connections: &HashMap<u8, ConnectionTasks>, msg: PingMsg)
where
    T: ExchangeConnector,
{
    if let Some(conn) = connections.get(&msg.ws_id) {
        if let Err(e) = conn.writer_tx.send(WriteCommand::Pong(msg.payload)).await {
            error!(exchange = ?T::exchange(), component = ?T::component(), error = ?e, "error while sending the pong command");
        }
    } else {
        error!(exchange = ?T::exchange(), component = ?T::component(), "pong requested for unknown connection");
    }
}

//used by all except binance
pub async fn recreate_with_snapshots<T, Raw>(
    ws_url: Arc<Url>,
    _snapshot_url: Option<Arc<Url>>,
    subscriptions_payloads: Arc<Vec<Message>>,
    inbound_tx: Sender<InboundEvent>,
    _raw_tx: Option<Sender<Raw>>,
    cmd_tx: Sender<ManagerCommand>,
) -> Result<(), Box<dyn Error + Send + Sync + 'static>>
where
    T: ExchangeConnector,
    Raw: Send + 'static,
{
    if subscriptions_payloads.len() > ws_id_capacity() {
        return Err(ConnectorError::TooManyWsBatchesForU8Id {
            batches: subscriptions_payloads.len(),
            max_supported: ws_id_capacity(),
        }
        .into());
    }

    for (i, message) in subscriptions_payloads.iter().enumerate() {
        let (writer_tx, writer_rx) = channel::<WriteCommand>(64);
        let (ws_stream, _) = connect_async(ws_url.as_str()).await?;
        let (write, read) = ws_stream.split();

        let reader_url = ws_url.clone();
        let writer_url = ws_url.clone();

        let reader_tx_clone = inbound_tx.clone();
        let Some(ws_id) = ws_id_from_index(i) else {
            return Err(ConnectorError::TooManyWsBatchesForU8Id {
                batches: subscriptions_payloads.len(),
                max_supported: ws_id_capacity(),
            }
            .into());
        };
        let reader_handle = tokio::spawn(async move {
            reader_task(ws_id, writer_url, read, reader_tx_clone).await;
        });

        let writer_handle = tokio::spawn(async move {
            writer_task(reader_url, write, writer_rx).await;
        });

        cmd_tx
            .send(ManagerCommand::InsertSubscription(
                ws_id,
                ConnectionTasks {
                    reader_handle,
                    writer_handle,
                    writer_tx: writer_tx.clone(),
                },
            ))
            .await?;
        writer_tx.send(WriteCommand::Raw(message.clone())).await?;
    }
    Ok(())
}
