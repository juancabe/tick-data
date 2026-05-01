#[cfg(test)]
use std::sync::Once;

#[cfg(test)]
use log::LevelFilter;

use crate::worker::{Runnable, asset_context::WorkingAssetContexts};

use std::{path::Path, pin::Pin, time::UNIX_EPOCH};

use futures::{StreamExt, stream::FuturesUnordered};
use parquet::basic::{Compression, ZstdLevel};

use crate::{
    ingest::IngestService,
    worker::{SubscriptionHandler, mids::WorkingMids, trades::WorkingTrades},
};

mod ingest;
mod persistence;
mod worker;

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

pub struct EnabledCoins {
    pub coins: Vec<String>,
}

pub struct TickData {
    ingest: IngestService,
    runnables: Vec<Box<dyn Runnable>>,
}

pub const DEFAULT_COMPRESSION_LEVEL: i32 = 19;

impl TickData {
    pub async fn new(
        max_hot_bytes: usize,
        work_dir: impl AsRef<Path>,
        compression_level: Option<Compression>,
        trades: EnabledCoins,
        asset_contexts: EnabledCoins,
        mids: EnabledMids,
    ) -> anyhow::Result<Self> {
        let comp_level = compression_level.unwrap_or(Compression::ZSTD(
            ZstdLevel::try_new(DEFAULT_COMPRESSION_LEVEL).unwrap(),
        ));

        // Trades
        let mut trades_dir = work_dir.as_ref().to_path_buf();
        trades_dir.push("trades");

        let (working_trades, t_persistences) =
            WorkingTrades::new(trades, trades_dir, max_hot_bytes, comp_level).await?;

        // Mids
        let mut mids_dir = work_dir.as_ref().to_path_buf();
        mids_dir.push("mids");

        let (working_mids, m_persistences) =
            WorkingMids::new(mids, mids_dir, max_hot_bytes, comp_level).await?;

        // Contexts

        let mut contexts_dir = work_dir.as_ref().to_path_buf();
        contexts_dir.push("asset_contexts");

        let (working_contexts, c_persistences) =
            WorkingAssetContexts::new(asset_contexts, contexts_dir, max_hot_bytes, comp_level)
                .await?;

        let handlers: Vec<Box<dyn SubscriptionHandler>> = vec![
            Box::new(working_mids) as Box<dyn SubscriptionHandler>,
            Box::new(working_trades),
            Box::new(working_contexts),
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
        runnables.extend(
            c_persistences
                .into_iter()
                .map(|p| Box::new(p) as Box<dyn Runnable>),
        );

        Ok(Self { ingest, runnables })
    }

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
}

fn get_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Correct system clock")
        .as_millis()
        .try_into()
        .expect("Reasonable year to run this code :D")
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
