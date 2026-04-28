#[cfg(test)]
use std::sync::Once;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    pin::Pin,
    time::Duration,
};

use futures::{StreamExt, stream::FuturesUnordered};
#[cfg(test)]
use log::LevelFilter;
use parquet::basic::{Compression, ZstdLevel};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    time::Instant,
};

use crate::{
    ingest::IngestService,
    persistence::{
        Persistence,
        models::{mid::MyMid, trade::MyTrade},
    },
};

mod ingest;
mod persistence;

struct MidsThings {
    persistence: Persistence<MyMid>,
    receiver: Receiver<Vec<MyMid>>,
    persistence_sender: Sender<Vec<MyMid>>,
}

type MidPers = Persistence<MyMid>;
type MidsRec = Receiver<Vec<MyMid>>;
type MidsSen = Sender<Vec<MyMid>>;

impl MidsThings {
    fn destructure(self) -> ((MidsRec, MidsSen), MidPers) {
        let Self {
            persistence,
            receiver,
            persistence_sender,
        } = self;
        ((receiver, persistence_sender), persistence)
    }
}

pub struct TickData {
    ingest: IngestService,
    trades_persistence_senders: HashMap<String, Sender<Vec<MyTrade>>>,
    trades_receiver: Receiver<Vec<MyTrade>>,
    trade_persistences: Vec<Persistence<MyTrade>>,
    mids: Option<MidsThings>,
}

pub const DEFAULT_COMPRESSION_LEVEL: u32 = 7;

impl TickData {
    pub async fn new(
        coins: HashSet<String>,
        per_hot_file_max_size: usize,
        work_dir: impl AsRef<Path>,
        compression_level: Option<Compression>,
        mids_enabled: bool,
    ) -> anyhow::Result<Self> {
        let (trades_sender, trades_receiver) = tokio::sync::mpsc::channel(10);
        let mut trades_persistence_senders = HashMap::new();
        let mut persistences = Vec::new();
        let mut trades_dir = work_dir.as_ref().to_path_buf();
        trades_dir.push("trades");

        let comp_level =
            compression_level.unwrap_or(Compression::ZSTD(ZstdLevel::try_new(9).unwrap()));

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
                per_hot_file_max_size,
                comp_level,
            );

            trades_persistence_senders.insert(coin.clone(), persist_sender);
            persistences.push(p);
        }

        let mut mids_dir = work_dir.as_ref().to_path_buf();
        mids_dir.push("all_mids");
        tokio::fs::create_dir_all(&mids_dir).await?;

        let (mids, mids_sender) = if mids_enabled {
            let (ms, mr) = tokio::sync::mpsc::channel(10);
            let (ps, pr) = tokio::sync::mpsc::channel(10);

            let mp = Persistence::new(
                MyMid::ID_NAME.to_string(),
                pr,
                mids_dir.clone(),
                mids_dir,
                per_hot_file_max_size,
                comp_level,
            );

            (
                Some(MidsThings {
                    persistence: mp,
                    receiver: mr,
                    persistence_sender: ps,
                }),
                Some(ms),
            )
        } else {
            (None, None)
        };

        let ingest = IngestService::new(coins, Some(trades_sender), mids_sender);

        Ok(Self {
            ingest,
            trades_persistence_senders,
            trade_persistences: persistences,
            trades_receiver,
            mids,
        })
    }

    async fn send_t<T: std::fmt::Debug + Clone>(val: T, sender: &Sender<T>, sent_thing: &str) {
        let before_send = Instant::now();

        if let Err(e) = sender.send(val.clone()).await {
            log::error!(
                "[TickData::run::handle_trades] sender.send resulted in error: {e:?} for val: {val:?}"
            );
        }
        let bs = before_send.elapsed();
        if bs > Duration::from_millis(1) {
            log::warn!(
                "[TickData::run::handle_trades] time to send {sent_thing}: {:?} > 1ms",
                bs
            );
        };
    }

    pub async fn run(mut self) {
        let handle_trades = async {
            let senders = self.trades_persistence_senders;

            while let Some(trades) = self.trades_receiver.recv().await {
                let trade_batches = Self::to_trade_batches(trades.into_iter());
                for (coin, trades) in trade_batches.into_iter() {
                    match senders.get(&coin) {
                        Some(sender) => {
                            Self::send_t(trades, sender, "trade").await;
                        }
                        None => {
                            log::error!(
                                "[TickData::run::handle_trades] trades_senders doesn't contain coin: {coin}"
                            )
                        }
                    }
                }
            }

            log::error!("[TickData::handle_trades] finished");
        };

        let (handle_mids, persistence_mid) =
            if let Some((handle_mids_p, persistence_p)) = self.mids.map(|m| m.destructure()) {
                (Some(handle_mids_p), Some(persistence_p))
            } else {
                (None, None)
            };

        let handle_mids = async {
            let Some((mut receiver, sender)) = handle_mids else {
                std::future::pending::<()>().await;
                unreachable!()
            };
            while let Some(mids) = receiver.recv().await {
                Self::send_t(mids, &sender, "all_mids").await;
            }
        };

        let run_persistences_futs = async {
            type BoxedFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;

            let mut unordered = FuturesUnordered::<BoxedFuture>::new();
            for fut in self.trade_persistences {
                unordered.push(Box::pin(async move { fut.run_persistence().await }));
            }
            if let Some(p) = persistence_mid {
                unordered.push(Box::pin(async move { p.run_persistence().await }));
            }

            if let Some(r) = unordered.next().await {
                log::error!("[TickData::run::run_persistences_futs] A future returned: {r:?}")
            }
        };

        let task_name = tokio::select! {
            _ = self.ingest.run() => { "ingest" }
            _ = handle_trades => { "handle_trades" }
            _ = handle_mids => { "handle_mids" }
            _ = run_persistences_futs => { "run_persistences_futs"}
        };

        log::error!("[TickData::run] {task_name} main task finished");
    }

    fn to_trade_batches(trades: impl Iterator<Item = MyTrade>) -> HashMap<String, Vec<MyTrade>> {
        let mut batches: HashMap<String, Vec<MyTrade>> = HashMap::new();

        for trade in trades {
            let coin_str = trade.coin.to_string();
            batches.entry(coin_str).or_default().push(trade);
        }

        log::debug!(
            "[TickData::to_trade_batches] Dissected {} trades into {:?}",
            batches.values().flatten().count(),
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
