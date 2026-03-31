use std::error::Error;

use reqwest::Client;
use url::Url;

use crate::events::PingMsg;
use crate::logging::types::Component;
use crate::types::Exchange;

/// Contract for exchange websocket connectors.
///
/// Defines the required lifecycle:
/// - build subscriptions
/// - open streams
/// - run the event loop
#[allow(async_fn_in_trait)]
pub trait ExchangeConnector {
    /// Type representing the subscription plan (payloads/batches).
    type SubscriptionsInfo;

    fn exchange() -> Exchange;

    fn component() -> Component {
        Component::Connector
    }

    /// Build subscription data (may include REST calls and batching).
    async fn build_subscriptions(
        client: Client,
        rest_url: &Url,
        max_subscription_per_ws: usize,
    ) -> Result<Self::SubscriptionsInfo, Box<dyn Error + Send + Sync + 'static>>;

    /// Create websocket connections and send subscriptions.
    async fn subscribe_streams(
        &mut self,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>>;

    /// Respond to a ping message.
    async fn pong(
        &self,
        msg: PingMsg,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>>;

    /// Start the connector (setup + main loop).
    async fn start(&mut self);
}