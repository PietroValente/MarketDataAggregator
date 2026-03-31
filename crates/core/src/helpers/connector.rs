use std::{error::Error, time::Duration};

use bytes::BytesMut;
use futures_util::StreamExt;
use reqwest::Client;
use serde::de::DeserializeOwned;
use url::Url;

pub async fn fetch_json<T>( client: &Client, url: &Url, timeout: Option<Duration> ) -> Result<T, Box<dyn Error + Send + Sync + 'static>>
where
    T: DeserializeOwned,
{
    let mut req = client.get(url.as_str());

    if let Some(t) = timeout {
        req = req.timeout(t);
    }

    let resp = req
        .send()
        .await?
        .error_for_status()?;

    let mut stream = resp.bytes_stream();
    let mut body = BytesMut::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        body.extend_from_slice(&chunk);
    }

    let parsed = serde_json::from_slice::<T>(&body)?;
    Ok(parsed)
}