use std::{collections::HashMap, path::PathBuf};

use hypersdk::hypercore::{Incoming, Subscription};
use parquet::basic::Compression;

use crate::{
    EnabledMids,
    persistence::{Persistence, models::mid::MyMid},
    worker::{DataPipeline, SubscriptionHandler},
};

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
            )
            .await?;

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
