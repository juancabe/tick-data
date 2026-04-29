#[cfg(test)]
use std::sync::Once;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    pin::Pin,
    time::Duration,
};

use futures::{StreamExt, stream::FuturesUnordered};
use hypersdk::hypercore::{Incoming, Subscription};
#[cfg(test)]
use log::LevelFilter;
use parquet::basic::{Compression, ZstdLevel};
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    ingest::{IngestService, SubscriptionHandler},
    persistence::{
        Persistable, Persistence,
        models::{mid::MyMid, trade::MyTrade},
    },
};

mod ingest;
mod persistence;

#[async_trait::async_trait]
pub trait Runnable: Sync + Send {
    async fn run(&mut self) -> anyhow::Result<()>;
}

pub struct DataPipeline<T: Persistable> {
    to_persist_sender: Sender<Vec<T>>,
}

impl<T: Persistable> DataPipeline<T> {
    async fn send(&self, value: Vec<T>) -> anyhow::Result<()> {
        let start = Instant::now();
        self.to_persist_sender.send(value).await?;
        let elapsed = start.elapsed();

        let epsilon = Duration::from_millis(1);
        if elapsed > epsilon {
            log::warn!("[DataPipeline::send] send took ({elapsed:?}) > ({epsilon:?})");
        }

        Ok(())
    }
}

pub struct EnabledTrades {
    pub coins: Vec<String>,
}

pub struct WorkingTrades {
    wt: HashMap<String, DataPipeline<MyTrade>>,
}

impl WorkingTrades {
    pub async fn new(
        enabled: EnabledTrades,
        base_dir: PathBuf,
        max_hot_bytes: usize,
        compression: Compression,
    ) -> anyhow::Result<(Self, Vec<Persistence<MyTrade>>)> {
        let mut wt = HashMap::new();
        let mut persistences = Vec::new();

        for coin in enabled.coins {
            let (to_persist_sender, to_persist_receiver) = tokio::sync::mpsc::channel(10);
            let id = coin.clone();

            let mut coin_dir = base_dir.clone();
            coin_dir.push(&id);

            let mut hot_storage_dir = coin_dir.clone();
            hot_storage_dir.push("hot-storage");
            tokio::fs::create_dir_all(&hot_storage_dir).await?;

            let mut compressed_storage_dir = coin_dir.clone();
            compressed_storage_dir.push("compressed-storage");
            tokio::fs::create_dir_all(&compressed_storage_dir).await?;

            let persistence = Persistence::new(
                id,
                to_persist_receiver,
                hot_storage_dir,
                compressed_storage_dir,
                max_hot_bytes,
                compression,
            );

            persistences.push(persistence);

            let dp = DataPipeline { to_persist_sender };

            wt.insert(coin, dp);
        }

        Ok((Self { wt }, persistences))
    }
}

pub enum Trades {
    EnabledTrades(EnabledTrades),
    WorkingTrades(WorkingTrades),
}

#[async_trait::async_trait]
impl SubscriptionHandler for WorkingTrades {
    fn subscriptions(&self) -> Vec<Subscription> {
        self.wt
            .keys()
            .cloned()
            .map(|coin| Subscription::Trades { coin })
            .collect()
    }

    async fn handle_incoming(&self, msg: hypersdk::hypercore::Incoming) -> bool {
        let Incoming::Trades(v) = msg else {
            return false;
        };

        let log_name: &'static str = "[SubscriptionHandler for WorkingTrades::handle_incoming]";

        let err_msg = format!("{log_name} received a msg with diverging coins: {v:?}");
        let my_trades: Vec<MyTrade> = v.into_iter().map(MyTrade::from).collect();
        let all_same = my_trades
            .windows(2)
            .all(|window| window[0].coin == window[1].coin);

        if !all_same {
            log::error!("{err_msg}");
            return false;
        }

        let Some(rec_coin) = my_trades.first().map(|t| t.coin.clone()) else {
            log::error!("{log_name} received empty trades");
            return false;
        };

        let Some(data_pipeline) = self.wt.get(rec_coin.as_str()) else {
            log::error!(
                "{log_name} received messages for coin {rec_coin} which is not in handled coins"
            );
            return false;
        };

        if let Err(e) = data_pipeline.send(my_trades).await {
            log::error!("{log_name} Couldn't send values: {e:?}");
            return false;
        }
        true
    }
}

#[derive(Clone)]
pub enum Dex {
    Principal,
    Other(String),
}

impl Dex {
    fn to_hyper_expects(&self) -> Option<String> {
        match self {
            Dex::Principal => None,
            Dex::Other(o) => Some(o.clone()),
        }
    }
}

pub struct EnabledMids {
    pub dexes: Vec<Dex>,
}

pub struct WorkingMids {
    wm: HashMap<Option<String>, DataPipeline<MyMid>>,
}

impl WorkingMids {
    pub async fn new(
        enabled: EnabledMids,
        base_dir: PathBuf,
        max_hot_bytes: usize,
        compression: Compression,
    ) -> anyhow::Result<(Self, Vec<Persistence<MyMid>>)> {
        let mut wm = HashMap::new();
        let mut persistences = Vec::new();

        for dex in enabled.dexes {
            let (to_persist_sender, to_persist_receiver) = tokio::sync::mpsc::channel(10);
            let id = dex
                .clone()
                .to_hyper_expects()
                .unwrap_or("DEFAULT".to_string());

            let mut dex_dir = base_dir.clone();
            dex_dir.push(id.clone() + "_DEX");

            let mut hot_storage_dir = dex_dir.clone();
            hot_storage_dir.push("hot-storage");
            tokio::fs::create_dir_all(&hot_storage_dir).await?;

            let mut compressed_storage_dir = dex_dir.clone();
            compressed_storage_dir.push("compressed-storage");
            tokio::fs::create_dir_all(&compressed_storage_dir).await?;

            let persistence = Persistence::new(
                id,
                to_persist_receiver,
                hot_storage_dir,
                compressed_storage_dir,
                max_hot_bytes,
                compression,
            );

            persistences.push(persistence);

            let dp = DataPipeline { to_persist_sender };

            wm.insert(dex.to_hyper_expects(), dp);
        }

        Ok((Self { wm }, persistences))
    }
}

#[async_trait::async_trait]
impl SubscriptionHandler for WorkingMids {
    fn subscriptions(&self) -> Vec<Subscription> {
        self.wm
            .keys()
            .cloned()
            .map(|dex| Subscription::AllMids { dex })
            .collect()
    }

    async fn handle_incoming(&self, msg: hypersdk::hypercore::Incoming) -> bool {
        let Incoming::AllMids { dex, mids } = msg else {
            return false;
        };

        let log_name: &'static str = "[SubscriptionHandler for WorkingMids::handle_incoming]";

        let Some(data_pipeline) = self.wm.get(&dex) else {
            log::error!("{log_name} received mids from unknown dex: {dex:?}");
            return false;
        };

        if let Err(e) = data_pipeline
            .to_persist_sender
            .send(MyMid::from_hm(mids))
            .await
        {
            log::error!("{log_name} Error sending mids: {e:?}");
            return false;
        }

        true
    }
}

pub enum Mids {
    EnabledMids(EnabledMids),
    WorkingMids(WorkingMids),
}

pub struct TickData {
    ingest: IngestService,
    runnables: Vec<Box<dyn Runnable>>,
}

pub const DEFAULT_COMPRESSION_LEVEL: u32 = 7;

impl TickData {
    pub async fn new(
        max_hot_bytes: usize,
        work_dir: impl AsRef<Path>,
        compression_level: Option<Compression>,
        trades: EnabledTrades,
        mids: EnabledMids,
    ) -> anyhow::Result<Self> {
        let mut trades_dir = work_dir.as_ref().to_path_buf();
        trades_dir.push("trades");

        let comp_level =
            compression_level.unwrap_or(Compression::ZSTD(ZstdLevel::try_new(9).unwrap()));

        let (working_trades, t_persistences) =
            WorkingTrades::new(trades, trades_dir, max_hot_bytes, comp_level).await?;

        let mut mids_dir = work_dir.as_ref().to_path_buf();
        mids_dir.push("mids");

        let (working_mids, m_persistences) =
            WorkingMids::new(mids, mids_dir, max_hot_bytes, comp_level).await?;

        let handlers: Vec<Box<dyn SubscriptionHandler>> = vec![
            Box::new(working_mids) as Box<dyn SubscriptionHandler>,
            Box::new(working_trades),
        ];

        let ingest = IngestService::new(handlers);

        let mut runnables = Vec::new();
        runnables.extend(
            t_persistences
                .into_iter()
                .map(|p| Box::new(p) as Box<dyn Runnable>),
        );
        runnables.extend(
            m_persistences
                .into_iter()
                .map(|p| Box::new(p) as Box<dyn Runnable>),
        );

        Ok(Self { ingest, runnables })
    }

    // async fn send_t<T: std::fmt::Debug + Clone>(val: T, sender: &Sender<T>, sent_thing: &str) {
    //     let before_send = Instant::now();

    //     if let Err(e) = sender.send(val.clone()).await {
    //         log::error!(
    //             "[TickData::run::handle_trades] sender.send resulted in error: {e:?} for val: {val:?}"
    //         );
    //     }
    //     let bs = before_send.elapsed();
    //     if bs > Duration::from_millis(1) {
    //         log::warn!(
    //             "[TickData::run::handle_trades] time to send {sent_thing}: {:?} > 1ms",
    //             bs
    //         );
    //     };
    // }

    pub async fn run(mut self) {
        let run_persistences_futs = async {
            type BoxedFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;

            let mut unordered = FuturesUnordered::<BoxedFuture>::new();
            for mut fut in self.runnables {
                unordered.push(Box::pin(async move { fut.run().await }));
            }

            if let Some(r) = unordered.next().await {
                log::error!("[TickData::run::run_persistences_futs] A future returned: {r:?}")
            }
        };

        let task_name = tokio::select! {
            _ = self.ingest.run() => { "ingest" }
            _ = run_persistences_futs => { "run_persistences_futs"}
        };

        log::error!("[TickData::run] {task_name} main task finished");
    }

    // fn to_trade_batches(trades: impl Iterator<Item = MyTrade>) -> HashMap<String, Vec<MyTrade>> {
    //     let mut batches: HashMap<String, Vec<MyTrade>> = HashMap::new();

    //     for trade in trades {
    //         let coin_str = trade.coin.to_string();
    //         batches.entry(coin_str).or_default().push(trade);
    //     }

    //     log::debug!(
    //         "[TickData::to_trade_batches] Dissected {} trades into {:?}",
    //         batches.values().flatten().count(),
    //         batches.keys()
    //     );

    //     batches
    // }
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
