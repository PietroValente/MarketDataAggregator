use std::error::Error;

use crate::{logging::types::Component, types::Exchange};

/// Common interface for exchange-specific adapters.
///
/// An adapter consumes exchange-native messages and emits the project's normalized events.
/// `SnapshotPayload` and `UpdatePayload` are the *parsed* types the adapter wants to validate
/// (sequence continuity, ordering) before normalization.
pub trait ExchangeAdapter {
    type SnapshotPayload;
    type UpdatePayload;

    fn exchange() -> Exchange;

    fn component() -> Component {
        Component::Adapter
    }

    /// Validate a parsed snapshot payload.
    fn validate_snapshot(
        &mut self,
        payload: &Self::SnapshotPayload,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>>;

    /// Validate a parsed update payload.
    fn validate_update(
        &mut self,
        payload: &Self::UpdatePayload,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>>;

    /// Run the adapter event loop (typically until the inbound stream is closed).
    fn run(&mut self);
}
