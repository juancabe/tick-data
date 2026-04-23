use futures::StreamExt;
use hypersdk::hypercore::{self, ws::Event};

#[derive(thiserror::Error, Debug)]
enum IngestError {}

pub struct IngestService {
    // Connected or Disconnected
    c_state: Event,
}

impl Default for IngestService {
    fn default() -> Self {
        Self {
            c_state: Event::Disconnected,
        }
    }
}

impl IngestService {
    pub async fn run(&mut self) {
        let mut ws = hypercore::mainnet_ws();
        ws.subscribe(hypercore::Subscription::Trades { coin: "BTC".into() });

        while let Some(e) = ws.next().await {
            log::debug!("Received from WebSocket: {:?}", e);
            let m = match e {
                Event::Connected => {
                    if matches!(self.c_state, Event::Disconnected) {
                        self.c_state = Event::Connected
                    } else {
                        log::warn!("Received {e:?} when c_state was {:?}", self.c_state);
                    }
                    continue;
                }
                Event::Disconnected => {
                    if matches!(self.c_state, Event::Connected) {
                        self.c_state = Event::Disconnected
                    } else {
                        log::warn!("Received {e:?} when c_state was {:?}", self.c_state);
                    }
                    continue;
                }
                Event::Message(m) => m,
            };

            match m {
                hypercore::Incoming::SubscriptionResponse(og) => {
                    log::warn!("Handle: {og:?}");
                }
                hypercore::Incoming::Trades(trades) => {
                    log::warn!("Handle: {trades:?}");
                }
                unhandled => {
                    log::warn!("Unhandled event: {unhandled:?}");
                }
            }
        }
    }
}
