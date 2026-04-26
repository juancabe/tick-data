use std::{collections::HashSet, path::PathBuf, str::FromStr};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let coins = HashSet::from_iter(
        ["BTC", "ETH", "LTC", "XMR"]
            .into_iter()
            .map(|s| s.to_string()),
    );
    tick_data::TickData::new(coins, 5_000_000, PathBuf::from_str("./").unwrap(), None)
        .await?
        .run()
        .await;
    Ok(())
}
