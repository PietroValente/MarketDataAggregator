use std::{collections::HashMap, time::Instant};

use tokio::sync::mpsc::Sender;
use tracing::{error, warn};

use crate::{
    connector::types::{INACTIVITY_TIMEOUT_SECS, POLL_INTERVAL_SECS},
    events::{ControlEvent, EngineMessage, EventEnvelope, NormalizedEvent},
    traits::adapter::ExchangeAdapter,
    types::{ExchangeStatus, Instrument},
};

pub fn clear_book_state<T>(book_states: &mut HashMap<Instrument, T>)
where
    T: Default,
{
    for book in book_states.values_mut() {
        *book = T::default();
    }
}

pub fn send_normalized_event<T>(normalized_tx: &Sender<EngineMessage>, event: NormalizedEvent)
where
    T: ExchangeAdapter,
{
    let event_envelope = EventEnvelope {
        exchange: T::exchange(),
        event,
    };

    if let Err(e) = normalized_tx.blocking_send(EngineMessage::Apply(event_envelope)) {
        error!(
            exchange = ?T::exchange(),
            component = ?T::component(),
            error = ?e,
            "error while sending normalized event"
        );
    }
}

pub fn send_status<T>(normalized_tx: &Sender<EngineMessage>, status: ExchangeStatus)
where
    T: ExchangeAdapter,
{
    send_normalized_event::<T>(normalized_tx, NormalizedEvent::ApplyStatus(status));
}

pub fn compute_status(live_books: usize, total_books: usize) -> ExchangeStatus {
    if total_books == 0 {
        ExchangeStatus::Initializing(0.0)
    } else if live_books == total_books {
        ExchangeStatus::Live
    } else {
        ExchangeStatus::Initializing(live_books as f32 / total_books as f32)
    }
}

pub fn handle_inactivity_timeout<T>(
    last_msg_at: &mut Instant,
    resync_in_progress: &mut bool,
    control_tx: &Sender<ControlEvent>,
) where
    T: ExchangeAdapter,
{
    if last_msg_at.elapsed() >= INACTIVITY_TIMEOUT_SECS {
        warn!(
            exchange = ?T::exchange(),
            component = ?T::component(),
            timeout_secs = ?INACTIVITY_TIMEOUT_SECS.as_secs(),
            "no messages received for too long, triggering resync"
        );

        if !*resync_in_progress {
            if let Err(e) = control_tx.blocking_send(ControlEvent::Resync) {
                error!(
                    exchange = ?T::exchange(),
                    component = ?T::component(),
                    error = ?e,
                    "error while sending resync after inactivity timeout"
                );
            } else {
                *resync_in_progress = true;
            }
        }

        *last_msg_at = Instant::now();
    }

    std::thread::sleep(POLL_INTERVAL_SECS);
}
