use std::{collections::HashMap, path::PathBuf};

use hypersdk::hypercore::{Incoming, Subscription};
use parquet::basic::Compression;

use crate::{
    EnabledTrades,
    persistence::{Persistence, models::trade::MyTrade},
    worker::{DataPipeline, SubscriptionHandler},
};

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
            )
            .await?;

            persistences.push(persistence);

            let dp = DataPipeline { to_persist_sender };

            wt.insert(coin, dp);
        }

        Ok((Self { wt }, persistences))
    }
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
