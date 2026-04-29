use hypersdk::{
    Address,
    hypercore::{Liquidation, Side, Trade},
};
use postcard::experimental::max_size::MaxSize;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::persistence::{
    compressed_storage::CompressStorable,
    hot_storage::HotStorable,
    models::{MAX_SYMBOL_LEN, MyDecimal},
};

// same as hypercore::Side
#[derive(Clone, Serialize, Deserialize, Debug, MaxSize, Copy, PartialEq, Eq, Hash)]
pub enum MySide {
    Bid,
    Ask,
}

impl From<Side> for MySide {
    fn from(value: Side) -> Self {
        match value {
            Side::Bid => Self::Bid,
            Side::Ask => Self::Ask,
        }
    }
}

pub const ADDRESS_LEN: usize = 20;
#[derive(Clone, Serialize, Deserialize, Debug, MaxSize, Copy, PartialEq, Eq, Hash)]
pub struct MyAddress([u8; ADDRESS_LEN]);

impl From<Address> for MyAddress {
    fn from(value: Address) -> Self {
        Self(value.0.0)
    }
}

// TODO: investigate real max len
pub const MAX_LIQUIDATION_METHOD_LEN: usize = 20;

#[derive(Clone, Serialize, Deserialize, Debug, MaxSize, PartialEq, Eq, Hash)]
pub struct MyLiquidation {
    /// Address of liquidated user
    pub liquidated_user: MyAddress,
    /// Mark price at liquidation
    pub mark_px: MyDecimal,
    /// Liquidation method
    pub method: heapless::String<{ MAX_LIQUIDATION_METHOD_LEN }>,
}

impl From<Liquidation> for MyLiquidation {
    fn from(
        Liquidation {
            liquidated_user,
            mark_px,
            method,
        }: Liquidation,
    ) -> Self {
        let liq_user_parsed: Address = match Address::from_str(&liquidated_user) {
            Ok(a) => a,
            Err(e) => {
                log::error!(
                    "Couldn't parse liquidated_user address ({liquidated_user}) to hypercore::Address, using Default: {e}"
                );
                Address::default()
            }
        };

        let method_parsed: heapless::String<MAX_LIQUIDATION_METHOD_LEN> = match method.parse() {
            Ok(mp) => mp,
            Err(e) => {
                log::error!(
                    "Couldn't parse liquidation method ({method}) to local heapless::String<_>, using Default: {e:?}"
                );
                heapless::String::default()
            }
        };

        Self {
            liquidated_user: liq_user_parsed.into(),
            mark_px: mark_px.into(),
            method: method_parsed,
        }
    }
}

pub const HASH_LEN: usize = 32;

#[derive(Clone, Serialize, Deserialize, Debug, MaxSize, PartialEq, Eq, Hash)]
pub struct MyTrade {
    /// Market symbol
    pub coin: heapless::String<{ MAX_SYMBOL_LEN }>,
    /// Taker's side (Bid = buy, Ask = sell)
    pub side: MySide,
    /// Execution price
    pub px: MyDecimal,
    /// Trade size
    pub sz: MyDecimal,
    /// Timestamp in milliseconds
    pub time: u64,
    /// Transaction hash
    ///  - example: "0x04a636b4c418cb2b061f0439d2687e0203cb009a5f1be9fda86ee207831ca515"
    pub hash: [u8; HASH_LEN],
    /// Trade ID
    pub tid: u64,
    /// Participant addresses: [buyer, seller]
    pub users: [MyAddress; 2],
    /// Liquidation details, if applicable
    pub liquidation: Option<MyLiquidation>,
}

impl Ord for MyTrade {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.time
            .cmp(&other.time)
            .then(self.hash.cmp(&other.hash))
            .then(self.tid.cmp(&other.tid))
    }
}

impl PartialOrd for MyTrade {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl From<Trade> for MyTrade {
    fn from(
        Trade {
            coin,
            side,
            px,
            sz,
            time,
            hash,
            tid,
            users,
            liquidation,
        }: Trade,
    ) -> Self {
        let coin_parsed: heapless::String<MAX_SYMBOL_LEN> = match coin.parse() {
            Ok(p) => p,
            Err(e) => {
                log::error!(
                    "Failed coin parse; coin.len(): {}, MAX_SYMBOL_LEN: {MAX_SYMBOL_LEN}; error: {e:?}",
                    coin.len()
                );
                coin.chars()
                    .take(MAX_SYMBOL_LEN)
                    .collect::<heapless::String<MAX_SYMBOL_LEN>>()
                    .parse()
                    .unwrap()
            }
        };

        let [u0, u1] = users;

        let hash_parsed: [u8; HASH_LEN] = match alloy_primitives::B256::from_str(&hash) {
            Ok(b256) => b256.0,
            Err(e) => {
                log::error!("Failed hash_parse; hash: {hash}; error: {e:?}");
                Default::default()
            }
        };

        Self {
            coin: coin_parsed,
            side: side.into(),
            px: px.into(),
            sz: sz.into(),
            time,
            hash: hash_parsed,
            tid,
            users: [u0.into(), u1.into()],
            liquidation: liquidation.map(|l| l.into()),
        }
    }
}

impl MyTrade {
    // AI generated
    #[cfg(test)]
    fn random() -> Self {
        // Bring both traits into scope for the whole block, including local functions
        use rand::{Rng, RngExt};

        let mut rng = rand::rng();

        // 1. Replaced closures with local helper functions.
        // `impl Rng` explicitly tells the compiler this argument supports random operations.
        fn random_decimal(rng: &mut impl Rng) -> MyDecimal {
            MyDecimal {
                flags: rng.random(),
                hi: rng.random(),
                lo: rng.random(),
                mid: rng.random(),
            }
        }

        fn random_address(rng: &mut impl Rng) -> MyAddress {
            let mut addr_bytes = [0u8; ADDRESS_LEN];
            rng.fill_bytes(&mut addr_bytes);
            MyAddress(addr_bytes)
        }

        let coins = ["BTC", "ETH", "SOL", "AVAX", "LINK", "USDC"];
        let random_coin = coins[rng.random_range(0..coins.len())];
        let coin =
            heapless::String::from_str(random_coin).expect("Coin string exceeds MAX_SYMBOL_LEN");

        let side = if rng.random_bool(0.5) {
            MySide::Bid
        } else {
            MySide::Ask
        };

        let mut hash = [0u8; HASH_LEN];
        rng.fill_bytes(&mut hash);

        let liquidation = if rng.random_bool(0.2) {
            let methods = ["Market", "Force", "MarginCall"];
            let random_method = methods[rng.random_range(0..methods.len())];

            Some(MyLiquidation {
                liquidated_user: random_address(&mut rng),
                mark_px: random_decimal(&mut rng),
                method: heapless::String::from_str(random_method)
                    .expect("Method string exceeds MAX_LIQUIDATION_METHOD_LEN"),
            })
        } else {
            None
        };

        Self {
            coin,
            side,
            px: random_decimal(&mut rng),
            sz: random_decimal(&mut rng),
            time: rng.random_range(1700000000000..1800000000000),
            hash,
            tid: rng.random(),
            users: [random_address(&mut rng), random_address(&mut rng)],
            liquidation,
        }
    }

    // AI generated to test compression
    #[cfg(test)]
    pub fn sequential_new(seq: usize) -> Self {
        // 1. Cycle through a small dictionary of coins
        let coins = ["BTC", "ETH", "SOL", "AVAX", "LINK", "USDC"];
        let coin_str = coins[seq % coins.len()];
        let coin =
            heapless::String::from_str(coin_str).expect("Coin string exceeds MAX_SYMBOL_LEN");

        // 2. Alternate Bid/Ask
        let side = if seq.is_multiple_of(2) {
            MySide::Bid
        } else {
            MySide::Ask
        };

        // 3. Simulating a slowly increasing price (Delta Encoding loves this)
        let px = MyDecimal {
            flags: 0,
            hi: 0,
            lo: (50000 + seq) as u32,
            mid: 0,
        };

        // 4. Repeating sizes (e.g., sizes cycle from 1 to 10)
        let sz = MyDecimal {
            flags: 0,
            hi: 0,
            lo: ((seq % 10) + 1) as u32,
            mid: 0,
        };

        // 5. Sequential timestamps (10ms intervals) and TIDs
        let time = 1700000000000_u64 + (seq as u64 * 10);
        let tid = seq as u64;

        // 6. Deterministic, low-entropy hash
        // We just encode the sequence number into the first 8 bytes and leave the rest 0
        let mut hash = [0u8; HASH_LEN];
        hash[0..8].copy_from_slice(&(seq as u64).to_be_bytes());

        // 7. Small pool of users (e.g., 5 market makers trading against each other)
        let user1_byte = (seq % 5) as u8;
        let user2_byte = ((seq + 1) % 5) as u8;
        let users = [
            MyAddress([user1_byte; ADDRESS_LEN]),
            MyAddress([user2_byte; ADDRESS_LEN]),
        ];

        // 8. Predictable liquidations (every 100th trade)
        let liquidation = if seq > 0 && seq.is_multiple_of(100) {
            let methods = ["Market", "Force", "MarginCall"];
            let method_str = methods[seq % methods.len()];

            Some(MyLiquidation {
                liquidated_user: users[0],
                mark_px: px,
                method: heapless::String::from_str(method_str)
                    .expect("Method string exceeds MAX_LIQUIDATION_METHOD_LEN"),
            })
        } else {
            None
        };

        Self {
            coin,
            side,
            px,
            sz,
            time,
            hash,
            tid,
            users,
            liquidation,
        }
    }
}

impl HotStorable for MyTrade {}
impl CompressStorable for MyTrade {}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use parquet::basic::{Compression, ZstdLevel};

    use crate::init_logger;

    use super::*;

    #[test]
    fn test_write_parquet() {
        let items = Vec::from_iter(std::iter::repeat_with(MyTrade::random).take(10000));
        let temp_file_path = tempfile::tempdir().unwrap();
        let mut file_path = temp_file_path.path().to_path_buf();
        file_path.push("parquet");
        assert!(!file_path.exists());
        MyTrade::write_items_to_parquet(
            &items,
            &file_path,
            Compression::ZSTD(ZstdLevel::try_new(9).unwrap()),
        )
        .unwrap();
        assert!(file_path.exists());
    }

    #[test]
    fn test_read_parquet() {
        let items = Vec::from_iter(std::iter::repeat_with(MyTrade::random).take(10000));
        let temp_file_path = tempfile::tempdir().unwrap();
        let mut file_path = temp_file_path.path().to_path_buf();
        file_path.push("parquet");
        assert!(!file_path.exists());
        MyTrade::write_items_to_parquet(
            &items,
            &file_path,
            Compression::ZSTD(ZstdLevel::try_new(9).unwrap()),
        )
        .unwrap();
        assert!(file_path.exists());

        let new_items = MyTrade::read_items_from_parquet(&file_path).unwrap();

        assert_eq!(new_items.len(), items.len());

        for (ni, i) in new_items.iter().zip(items.iter()) {
            assert_eq!(ni, i);
        }

        let file = File::open(file_path).unwrap();
        init_logger();
        log::info!(
            "[test_read_parquet] file metadata: {:?}",
            file.metadata().unwrap()
        );
        log::info!(
            "[test_read_parquet] file len: {:?}, items RAM usage: {}",
            file.metadata().unwrap().len(),
            items.len() * size_of::<MyTrade>()
        );
    }

    #[test]
    fn test_parquet_compression() {
        let items: Vec<MyTrade> = (0..10000).map(MyTrade::sequential_new).collect();
        let temp_file_path = tempfile::tempdir().unwrap();
        let mut file_path = temp_file_path.path().to_path_buf();
        file_path.push("parquet");
        assert!(!file_path.exists());
        MyTrade::write_items_to_parquet(
            &items,
            &file_path,
            Compression::ZSTD(ZstdLevel::try_new(9).unwrap()),
        )
        .unwrap();
        assert!(file_path.exists());

        let new_items = MyTrade::read_items_from_parquet(&file_path).unwrap();

        assert_eq!(new_items.len(), items.len());

        for (ni, i) in new_items.iter().zip(items.iter()) {
            assert_eq!(ni, i);
        }

        let file = File::open(file_path).unwrap();
        init_logger();
        log::info!(
            "[test_read_parquet] file metadata: {:?}",
            file.metadata().unwrap()
        );
        log::info!(
            "[test_read_parquet] file len: {:?}, items RAM usage: {}",
            file.metadata().unwrap().len(),
            items.len() * size_of::<MyTrade>()
        );

        assert!(
            (file.metadata().unwrap().len() as usize) < (items.len() * size_of::<MyTrade>() / 15)
        );
    }
}
