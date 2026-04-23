use hypersdk::{
    Address, Decimal,
    hypercore::{Liquidation, Side, Trade},
};
use postcard::experimental::max_size::MaxSize;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

// same as hypercore::Side
#[derive(Clone, Serialize, Deserialize, Debug, MaxSize, Copy)]
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

#[derive(
    Clone, Serialize, Deserialize, Debug, MaxSize, Copy, bytemuck::Pod, bytemuck::Zeroable,
)]
#[repr(C)]
pub struct MyDecimal {
    // described in hypersdk::Decimal
    flags: u32,
    hi: u32,
    lo: u32,
    mid: u32,
}

impl From<Decimal> for MyDecimal {
    /// Converts hypersdk::Decimal to MyDecimal
    ///
    /// ## Panics
    /// Only if hypersdk::Decimal size doesn't match MyDecimal's size
    fn from(value: Decimal) -> Self {
        bytemuck::cast(value)
    }
}

pub const ADDRESS_LEN: usize = 20;
#[derive(Clone, Serialize, Deserialize, Debug, MaxSize, Copy)]
pub struct MyAddress([u8; ADDRESS_LEN]);

impl From<Address> for MyAddress {
    fn from(value: Address) -> Self {
        Self(value.0.0)
    }
}

// TODO: investigate real max len
pub const MAX_LIQUIDATION_METHOD_LEN: usize = 20;

#[derive(Clone, Serialize, Deserialize, Debug, MaxSize)]
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
const MAX_SYMBOL_LEN: usize = 10;

#[derive(Clone, Serialize, Deserialize, Debug, MaxSize)]
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
