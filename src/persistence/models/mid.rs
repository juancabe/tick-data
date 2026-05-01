use std::{collections::HashMap, time::UNIX_EPOCH};

use postcard::experimental::max_size::MaxSize;
use serde::{Deserialize, Serialize};

use crate::persistence::{
    compressed_storage::CompressStorable,
    hot_storage::HotStorable,
    models::{MAX_SYMBOL_LEN, MyDecimal},
};

#[derive(Clone, Serialize, Deserialize, Debug, MaxSize, PartialEq, Eq, Hash)]
pub struct MyMid {
    pub coin: heapless::String<{ MAX_SYMBOL_LEN }>,
    pub mid_px: MyDecimal,
    pub timestamp: u64,
}

impl Ord for MyMid {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp
            .cmp(&other.timestamp)
            .then(self.coin.cmp(&other.coin))
    }
}

impl PartialOrd for MyMid {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl MyMid {
    pub fn from_hm(value: HashMap<String, hypersdk::Decimal>) -> Vec<Self> {
        let timestamp: u64 = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Correct system clock")
            .as_millis()
            .try_into()
            .expect("Reasonable year to run this code :D");

        let mut mids = Vec::new();

        for (coin, mid_px) in value {
            if coin.len() > MAX_SYMBOL_LEN {
                log::warn!(
                    "MAX_SYMBOL_LEN unsufficient for coin {coin} with {} chars",
                    coin.len()
                );
            }
            mids.push(Self {
                coin: coin.as_str().into(),
                mid_px: mid_px.into(),
                timestamp,
            });
        }

        mids
    }
}

impl HotStorable for MyMid {}
impl CompressStorable for MyMid {}
