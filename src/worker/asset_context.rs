use std::{collections::HashMap, path::PathBuf};

use hypersdk::hypercore::{Incoming, Subscription};
use parquet::basic::Compression;

use crate::{
    EnabledCoins,
    persistence::{Persistence, models::asset_context::MyAssetContext},
    worker::{DataPipeline, SubscriptionHandler},
};

pub struct WorkingAssetContexts {
    wm: HashMap<String, DataPipeline<MyAssetContext>>,
}

impl WorkingAssetContexts {
    pub async fn new(
        enabled: EnabledCoins,
        base_dir: PathBuf,
        max_hot_bytes: usize,
        compression: Compression,
    ) -> anyhow::Result<(Self, Vec<Persistence<MyAssetContext>>)> {
        let mut wm = HashMap::new();
        let mut persistences = Vec::new();

        for coin in enabled.coins {
            let (to_persist_sender, to_persist_receiver) = tokio::sync::mpsc::channel(10);
            let id = coin.clone();

            let mut context_dir = base_dir.clone();
            context_dir.push(id.clone());

            let mut hot_storage_dir = context_dir.clone();
            hot_storage_dir.push("hot-storage");
            tokio::fs::create_dir_all(&hot_storage_dir).await?;

            let mut compressed_storage_dir = context_dir.clone();
            compressed_storage_dir.push("compressed-storage");
            tokio::fs::create_dir_all(&compressed_storage_dir).await?;

            let persistence = Persistence::new(
                id,
                to_persist_receiver,
                hot_storage_dir,
                compressed_storage_dir,
                max_hot_bytes,
                compression,
            )
            .await?;

            persistences.push(persistence);

            let dp = DataPipeline { to_persist_sender };

            wm.insert(coin, dp);
        }

        Ok((Self { wm }, persistences))
    }
}

#[async_trait::async_trait]
impl SubscriptionHandler for WorkingAssetContexts {
    fn subscriptions(&self) -> Vec<Subscription> {
        self.wm
            .keys()
            .cloned()
            .map(|coin| Subscription::ActiveAssetCtx { coin })
            .collect()
    }

    async fn handle_incoming(&self, msg: hypersdk::hypercore::Incoming) -> bool {
        let Incoming::ActiveAssetCtx { coin, ctx } = msg else {
            return false;
        };

        let log_name: &'static str = "[SubscriptionHandler for WorkingMids::handle_incoming]";

        let Some(data_pipeline) = self.wm.get(&coin) else {
            log::error!("{log_name} received asset_ctx from unknown coin: {coin}");
            return false;
        };

        if let Err(e) = data_pipeline
            .to_persist_sender
            .send(vec![MyAssetContext::from(ctx)])
            .await
        {
            log::error!("{log_name} Error sending mids: {e:?}");
            return false;
        }

        true
    }
}
