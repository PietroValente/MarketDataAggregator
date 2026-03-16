use std::{error::Error, fs};

use scylla::client::{session::Session, session_builder::SessionBuilder};
use tokio::sync::mpsc::Receiver;

use super::types::LogEvent;

pub struct DbLoggingWriter {
    receiver: Receiver<LogEvent>,
    session: Session
}

impl DbLoggingWriter {
    pub async fn new(uri: &str, init_path: &str, receiver: Receiver<LogEvent>) -> Result<Self, Box<dyn Error>> {
        let init = fs::read_to_string(init_path)?;
        let session = SessionBuilder::new()
        .known_node(uri)
        .build()
        .await?;

        for stmt in init.split(';') {
            let stmt = stmt.trim();
    
            if stmt.is_empty() {
                continue;
            }
    
            session.query_unpaged(stmt, &[]).await?;
        }

        Ok(Self {
            receiver,
            session
        })
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn Error>> {
        while let Some(log_event) = self.receiver.recv().await {
            match log_event {
                LogEvent::Instrument(instrument_log) => {
                    let component_exchange = instrument_log.component_exchange;
            
                    let level = component_exchange.generic.level.to_string();
                    let ts = component_exchange.generic.timestamp;
                    let target = component_exchange.generic.target.to_string();
            
                    let location = component_exchange
                        .generic
                        .location
                        .map(|location| format!("{}:{}", location.file, location.line))
                        .unwrap_or_default();
            
                    let message = component_exchange.generic.message.unwrap_or_default();
            
                    let component = component_exchange.component;
                    let exchange = component_exchange.exchange;
                    let instrument = instrument_log.instrument;
            
                    let instrument_row = (
                        level.clone(),
                        instrument,
                        component.clone(),
                        exchange.clone(),
                        ts,
                        message.clone(),
                        target.clone(),
                        location.clone(),
                    );
            
                    let component_exchange_row = (
                        level,
                        component,
                        exchange,
                        ts,
                        message,
                        target,
                        location,
                    );
            
                    self.session
                        .query_unpaged(
                            "INSERT INTO instrument_logs (
                                level,
                                instrument,
                                component,
                                exchange,
                                ts,
                                message,
                                target,
                                location
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            instrument_row,
                        )
                        .await?;
            
                    self.session
                        .query_unpaged(
                            "INSERT INTO component_exchange_logs (
                                level,
                                component,
                                exchange,
                                ts,
                                message,
                                target,
                                location
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            component_exchange_row,
                        )
                        .await?;
                }
            
                LogEvent::ComponentExchange(component_exchange_log) => {
                    let level = component_exchange_log.generic.level.to_string();
                    let ts = component_exchange_log.generic.timestamp;
                    let target = component_exchange_log.generic.target.to_string();
            
                    let location = component_exchange_log
                        .generic
                        .location
                        .map(|location| format!("{}:{}", location.file, location.line))
                        .unwrap_or_default();
            
                    let message = component_exchange_log.generic.message.unwrap_or_default();
            
                    let component_exchange_row = (
                        level,
                        component_exchange_log.component,
                        component_exchange_log.exchange,
                        ts,
                        message,
                        target,
                        location,
                    );
            
                    self.session
                        .query_unpaged(
                            "INSERT INTO component_exchange_logs (
                                level,
                                component,
                                exchange,
                                ts,
                                message,
                                target,
                                location
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            component_exchange_row,
                        )
                        .await?;
                }
            
                LogEvent::Generic(generic_log) => {
                    let level = generic_log.level.to_string();
                    let ts = generic_log.timestamp;
                    let target = generic_log.target.to_string();
            
                    let location = generic_log
                        .location
                        .map(|location| format!("{}:{}", location.file, location.line))
                        .unwrap_or_default();
            
                    let message = generic_log.message.unwrap_or_default();
            
                    let generic_row = (
                        level,
                        ts,
                        message,
                        target,
                        location,
                    );
            
                    self.session
                        .query_unpaged(
                            "INSERT INTO generic_logs (
                                level,
                                ts,
                                message,
                                target,
                                location
                            ) VALUES (?, ?, ?, ?, ?)",
                            generic_row,
                        )
                        .await?;
                }
            }
        }
        Ok(())
    }
}