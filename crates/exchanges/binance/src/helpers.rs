use std::{error::Error, sync::Arc};

use futures_util::{StreamExt, TryStreamExt, stream};
use md_core::{
    connector::{
        tasks::{reader_task, writer_task},
        types::{
            ConnectionTasks, ConnectorError, ManagerCommand, WS_CONNECT_TIMEOUT_SECS, WriteCommand,
        },
    },
    events::InboundEvent,
    helpers::connector::{ws_id_capacity, ws_id_from_index},
    types::RawMdMsg,
};
use tokio::sync::mpsc::{Sender, channel};
use tokio_tungstenite::connect_async;
use url::Url;

use crate::types::{BinanceMdMsg, DepthQuery, SnapshotMsg, SubscriptionBatch};

pub async fn recreate_with_snapshots(
    ws_url: Arc<Url>,
    snapshot_url: Option<Arc<Url>>,
    subscriptions_payloads: Arc<Vec<SubscriptionBatch>>,
    inbound_tx: Sender<InboundEvent>,
    raw_tx: Option<Sender<BinanceMdMsg>>,
    cmd_tx: Sender<ManagerCommand>,
) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let batches_payloads = subscriptions_payloads;

    //the template function need to call this version of recreate_with_snapshots for binance, where snapshot_url and raw_tx are Some
    let (snapshot_url, raw_tx) = (snapshot_url.unwrap(), raw_tx.unwrap());

    if batches_payloads.len() > ws_id_capacity() {
        return Err(ConnectorError::TooManyWsBatchesForU8Id {
            batches: batches_payloads.len(),
            max_supported: ws_id_capacity(),
        }
        .into());
    }

    cmd_tx.send(ManagerCommand::AbortAllConnections).await?;
    inbound_tx.send(InboundEvent::ClearBookState).await?;

    for (i, batch) in batches_payloads.iter().enumerate() {
        let (writer_tx, writer_rx) = channel::<WriteCommand>(64);
        let (ws_stream, _) =
            tokio::time::timeout(WS_CONNECT_TIMEOUT_SECS, connect_async(ws_url.as_str()))
                .await
                .map_err(|_| ConnectorError::WebSocketConnectTimeout)??;

        let (write, read) = ws_stream.split();

        let reader_url = ws_url.clone();
        let writer_url = ws_url.clone();

        let reader_tx_clone = inbound_tx.clone();
        let Some(ws_id) = ws_id_from_index(i) else {
            return Err(ConnectorError::TooManyWsBatchesForU8Id {
                batches: batches_payloads.len(),
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

        writer_tx
            .send(WriteCommand::Raw(batch.message.clone()))
            .await?;

        let client = reqwest::Client::new();
        let raw_tx_snap = raw_tx.clone();
        let snapshot_url_snap = snapshot_url.clone();
        let symbols = batch.symbols.clone();

        stream::iter(symbols)
            .map(Ok::<_, Box<dyn Error + Send + Sync + 'static>>)
            .try_for_each_concurrent(10, move |symbol| {
                let client = client.clone();
                let raw_tx = raw_tx_snap.clone();
                let snapshot_url = snapshot_url_snap.clone();

                async move {
                    let params = DepthQuery {
                        symbol: &symbol,
                        limit: 100,
                    };

                    let response = client
                        .get(snapshot_url.as_str())
                        .query(&params)
                        .send()
                        .await?
                        .error_for_status()?;

                    let bytes = response.bytes().await?;

                    raw_tx
                        .send(BinanceMdMsg::Snapshot(SnapshotMsg {
                            symbol,
                            payload: RawMdMsg(bytes.to_vec()),
                        }))
                        .await?;

                    Ok(())
                }
            })
            .await?;
    }
    Ok(())
}
