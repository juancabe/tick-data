use std::time::Duration;

use hypersdk::hypercore::{self, Subscription};
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::persistence::Persistable;

pub mod mids;
pub mod trades;

#[async_trait::async_trait]
pub trait SubscriptionHandler {
    fn subscriptions(&self) -> Vec<Subscription>;
    async fn handle_incoming(&self, msg: hypercore::Incoming) -> bool;
}

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
