use std::time::Duration;

use futures::StreamExt;
use hypersdk::hypercore::{self, Subscription, ws::Event};
use tokio::time::sleep;

#[async_trait::async_trait]
pub trait SubscriptionHandler {
    fn subscriptions(&self) -> Vec<Subscription>;
    async fn handle_incoming(&self, msg: hypercore::Incoming) -> bool;
}

#[derive(Default)]
pub struct IngestService {
    handlers: Vec<Box<dyn SubscriptionHandler>>,
}

impl IngestService {
    pub fn new(handlers: Vec<Box<dyn SubscriptionHandler>>) -> Self {
        Self { handlers }
    }

    pub async fn run(&mut self) {
        log::info!("[IngestService::run] starting");

        loop {
            let mut ws = hypercore::mainnet_ws();
            log::info!("[IngestService::run] SDK WebSocket created");

            while let Some(e) = ws.next().await {
                log::debug!("[IngestService::run] Received from WebSocket: {:?}", e);
                let m = match e {
                    Event::Connected => {
                        // Subscribe once connected
                        for sub in self.handlers.iter().flat_map(|h| h.subscriptions()) {
                            ws.subscribe(sub);
                        }
                        continue;
                    }
                    Event::Disconnected => {
                        log::warn!("[IngestService::run] Event::Disconnected");
                        continue;
                    }
                    Event::Message(m) => m,
                };

                match m {
                    hypercore::Incoming::SubscriptionResponse(og) => {
                        log::warn!("[IngestService::run] Handle: {og:?}");
                    }
                    m => {
                        let mut handled = false;
                        for handler in &self.handlers {
                            if handler.handle_incoming(m.clone()).await {
                                handled = true;
                            }
                        }

                        if !handled {
                            log::warn!("[IngestService::run] unhandled message");
                        }
                    }
                }
            }

            log::warn!("[IngestService::run] WebSocket disconnected, trying to connect again...");
            sleep(Duration::from_secs(2)).await;
        }
    }
}
