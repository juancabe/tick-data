use std::{collections::HashSet, env, path::PathBuf, str::FromStr};

const DEFAULT_HOT_BUDGET: usize = 125_000_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let hot_budget = env::var("HOT_BUDGET")
        .ok()
        .and_then(|string| string.parse::<usize>().ok())
        .inspect(|hot_budget| {
            log::info!("[main] budget parsed: {hot_budget}");
        })
        .unwrap_or(DEFAULT_HOT_BUDGET);

    let coins: Vec<String> = env::var("TRADE_COINS")
        .map(|val| val.split(',').map(|s| s.to_string()).collect())
        .inspect(|coins| {
            log::info!("[main] coins parsed: {coins:?}");
        })
        .unwrap_or_default();

    let mids_enabled: bool = env::var("MIDS_ENABLED")
        .inspect_err(|e| {
            log::warn!("Error reading MIDS_ENABLED, assuming false: {e:?}");
        })
        .ok()
        .map(|me| {
            me.parse::<bool>()
                .inspect_err(|e| {
                    panic!("Invalid MIDS_ENABLED value: {me}, error: {e:?}");
                })
                .unwrap()
        })
        .unwrap_or(false);

    let work_dir = env::var("WORK_DIR")
        .inspect(|dir| {
            let path = PathBuf::from(dir);
            assert!(path.exists(), "WORK_DIR ({dir}) must exist");
        })
        .unwrap_or("./".to_string());

    tick_data::TickData::new(
        HashSet::from_iter(coins),
        hot_budget,
        PathBuf::from_str(&work_dir).unwrap(),
        None,
        mids_enabled,
    )
    .await?
    .run()
    .await;
    Ok(())
}
