#[cfg(test)]
use std::sync::Once;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use futures::{StreamExt, stream::FuturesUnordered};
#[cfg(test)]
use log::LevelFilter;
use parquet::basic::{Compression, GzipLevel};
use tokio::{sync::mpsc::Receiver, task::JoinSet};

use crate::{
    ingest::IngestService,
    persistence::{Persistence, trade::MyTrade},
};

mod ingest;
mod persistence;

pub struct TickData {
    ingest: IngestService,
    trades_senders: HashMap<String, tokio::sync::mpsc::Sender<Vec<MyTrade>>>,
    persistences: Vec<Persistence<MyTrade>>,
    trades_receiver: Receiver<Vec<MyTrade>>,
}

pub const DEFAULT_COMPRESSION_LEVEL: u32 = 7;

impl TickData {
    pub async fn new(
        coins: HashSet<String>,
        max_total_ram: usize,
        work_dir: impl AsRef<Path>,
        compression_level: Option<Compression>,
    ) -> anyhow::Result<Self> {
        let (trades_sender, trades_receiver) = tokio::sync::mpsc::channel(10);
        let mut trades_senders = HashMap::new();
        let mut persistences = Vec::new();
        let mut trades_dir = work_dir.as_ref().to_path_buf();
        trades_dir.push("trades");

        let comp_level = compression_level.unwrap_or(Compression::GZIP(
            GzipLevel::try_new(DEFAULT_COMPRESSION_LEVEL).unwrap(),
        ));

        for coin in &coins {
            let mut dir_path = trades_dir.to_path_buf();
            dir_path.push(coin.clone());
            tokio::fs::create_dir_all(&dir_path).await?;

            let (persist_sender, persist_receiver) = tokio::sync::mpsc::channel(10);

            let p = Persistence::new(
                coin.clone(),
                persist_receiver,
                dir_path.clone(),
                dir_path.clone(),
                max_total_ram / coins.len(),
                comp_level,
            );

            trades_senders.insert(coin.clone(), persist_sender);
            persistences.push(p);
        }

        let ingest = IngestService::new(coins, Some(trades_sender));

        Ok(Self {
            ingest,
            trades_senders,
            persistences,
            trades_receiver,
        })
    }

    pub async fn run(mut self) {
        let handle_trades = async {
            let senders = self.trades_senders;

            while let Some(trades) = self.trades_receiver.recv().await {
                let trade_batches = Self::to_trade_batches(trades.into_iter());
                for (coin, trades) in trade_batches.into_iter() {
                    match senders.get(&coin) {
                        Some(sender) => {
                            if let Err(e) = sender.send(trades).await {
                                log::error!(
                                    "[TickData::run::handle_trades] sender.send resulted in error: {e:?}"
                                )
                            }
                        }
                        None => {
                            log::error!(
                                "[TickData::run::handle_trades] trades_senders doesn't contain coin: {coin}"
                            )
                        }
                    }
                }
            }
        };

        let run_persistences_futs = async {
            let mut unordered = FuturesUnordered::new();
            for fut in self.persistences {
                unordered.push(async move { fut.run_persistence().await });
            }

            if let Some(r) = unordered.next().await {
                log::error!("[TickData::run::run_persistences_futs] A future returned: {r:?}")
            }
        };

        tokio::select! {
            _ = self.ingest.run() => {}
            _ = handle_trades => {}
            _ = run_persistences_futs => {}
        }
    }

    fn to_trade_batches(trades: impl Iterator<Item = MyTrade>) -> HashMap<String, Vec<MyTrade>> {
        let mut batches: HashMap<String, Vec<MyTrade>> = HashMap::new();

        for trade in trades {
            let coin_str = trade.coin.to_string();
            batches.entry(coin_str).or_default().push(trade);
        }

        log::info!(
            "[TickData::to_trade_batches] Dissected into {:?}",
            batches.keys()
        );

        batches
    }
}

#[cfg(test)]
static INIT: Once = Once::new();
#[cfg(test)]
fn init_logger() {
    INIT.call_once(|| {
        env_logger::builder()
            // 1. Force the level to Debug (ignores environment variables)
            .filter_level(LevelFilter::Debug)
            // 2. Ensures logs are captured by the test runner correctly
            .is_test(true)
            .init();
    });
}

#[cfg(test)]
mod tests {}
