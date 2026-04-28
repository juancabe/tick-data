use std::{collections::HashSet, time::Duration};

use futures::StreamExt;
use hypersdk::hypercore::{self, ws::Event};
use tokio::{sync::mpsc::Sender, time::sleep};

use crate::persistence::models::{mid::MyMid, trade::MyTrade};

pub struct IngestService {
    // Connected or Disconnected
    c_state: Event,
    coins: HashSet<String>,
    trades_sender: Option<Sender<Vec<MyTrade>>>,
    all_mids_sender: Option<Sender<Vec<MyMid>>>,
}

const DEX: Option<String> = None;

impl Default for IngestService {
    fn default() -> Self {
        Self {
            c_state: Event::Disconnected,
            coins: Default::default(),
            trades_sender: Default::default(),
            all_mids_sender: Default::default(),
        }
    }
}

impl IngestService {
    pub fn new(
        coins: HashSet<String>,
        trades_sender: Option<Sender<Vec<MyTrade>>>,
        all_mids_sender: Option<Sender<Vec<MyMid>>>,
    ) -> Self {
        Self {
            coins,
            trades_sender,
            all_mids_sender,
            ..Default::default()
        }
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
                        if matches!(self.c_state, Event::Disconnected) {
                            self.c_state = Event::Connected
                        } else {
                            log::warn!(
                                "[IngestService::run] Received {e:?} when c_state was {:?}",
                                self.c_state
                            );
                        }
                        // Subscribe once connected
                        for coin in self.coins.clone() {
                            ws.subscribe(hypercore::Subscription::Trades { coin });
                        }

                        if self.all_mids_sender.is_some() {
                            ws.subscribe(hypercore::Subscription::AllMids { dex: DEX });
                        }
                        continue;
                    }
                    Event::Disconnected => {
                        if matches!(self.c_state, Event::Connected) {
                            self.c_state = Event::Disconnected
                        } else {
                            log::warn!(
                                "[IngestService::run] Received {e:?} when c_state was {:?}",
                                self.c_state
                            );
                        }
                        continue;
                    }
                    Event::Message(m) => m,
                };

                match m {
                    hypercore::Incoming::SubscriptionResponse(og) => {
                        log::warn!("[IngestService::run] Handle: {og:?}");
                    }
                    hypercore::Incoming::Trades(trades) => {
                        log::debug!("[IngestService::run] Received trades: {trades:?}");
                        log::debug!("[IngestService::run] Received {} trades", trades.len());
                        if let Some(trades_sender) = &self.trades_sender {
                            let trades = trades.into_iter().map(MyTrade::from).collect();
                            if let Err(e) = trades_sender.send(trades).await {
                                log::error!(
                                    "[IngestService::run] Error sending trades to consumer: {e:?}"
                                )
                            };
                        }
                    }
                    hypercore::Incoming::AllMids { dex, mids } => {
                        if DEX != dex {
                            log::error!("DEX != dex, skipping");
                            continue;
                        }

                        log::debug!("[IngestService::run] Received mids: {}", mids.len());

                        if let Some(mids_sender) = &self.all_mids_sender
                            && let Err(e) = mids_sender.send(MyMid::from_hm(mids)).await
                        {
                            log::error!(
                                "[IngestService::run] Error sending mids to consumer: {e:?}"
                            )
                        }
                    }
                    unhandled => {
                        log::warn!("[IngestService::run] Unhandled event: {unhandled:?}");
                    }
                }
            }

            log::warn!("[IngestService::run] WebSocket disconnected, trying to connect again...");
            sleep(Duration::from_secs(2)).await;
        }
    }
}
