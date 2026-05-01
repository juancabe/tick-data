use std::{env, path::PathBuf, str::FromStr};

use tick_data::{Dex, EnabledCoins, EnabledMids};

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

    let trades: Vec<String> = env::var("TRADE_COINS")
        .map(|val| val.split(',').map(|s| s.to_string()).collect())
        .inspect(|coins| {
            log::info!("[main] trade coins parsed: {coins:?}");
        })
        .unwrap_or_default();

    let asset_contexts: Vec<String> = env::var("ASSET_CONTEXT_COINS")
        .map(|val| val.split(',').map(|s| s.to_string()).collect())
        .inspect(|coins| {
            log::info!("[main] asset context coins parsed: {coins:?}");
        })
        .unwrap_or_default();

    let dexes: Vec<String> = env::var("MIDS_NON_DEFAULT_DEXES")
        .map(|val| val.split(',').map(|s| s.to_string()).collect())
        .inspect(|dex| {
            log::info!("[main] dex parsed: {dex:?}");
        })
        .unwrap_or_default();

    let default_mids_enabled: bool = env::var("DEFAULT_DEX_MIDS_ENABLED")
        .inspect_err(|e| {
            log::warn!("Error reading DEFAULT_DEX_MIDS_ENABLED, assuming false: {e:?}");
        })
        .ok()
        .map(|me| {
            me.parse::<bool>()
                .inspect_err(|e| {
                    panic!("Invalid DEFAULT_DEX_MIDS_ENABLED value: {me}, error: {e:?}");
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

    let mut dexes: Vec<Dex> = dexes.into_iter().map(Dex::Other).collect();
    if default_mids_enabled {
        dexes.push(Dex::Principal);
    }

    tick_data::TickData::new(
        hot_budget,
        PathBuf::from_str(&work_dir).unwrap(),
        None,
        EnabledCoins { coins: trades },
        EnabledCoins {
            coins: asset_contexts,
        },
        EnabledMids { dexes },
    )
    .await?
    .run()
    .await;
    Ok(())
}
