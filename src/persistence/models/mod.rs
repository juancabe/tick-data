use hypersdk::Decimal;
use postcard::experimental::max_size::MaxSize;
use serde::{Deserialize, Serialize};

pub mod asset_context;
pub mod mid;
pub mod trade;

#[derive(
    Clone,
    Serialize,
    Deserialize,
    Debug,
    MaxSize,
    Copy,
    bytemuck::Pod,
    bytemuck::Zeroable,
    PartialEq,
    Eq,
    Hash,
)]
#[repr(C)]
pub struct MyDecimal {
    // described in hypersdk::Decimal
    flags: u32,
    hi: u32,
    lo: u32,
    mid: u32,
}

const MAX_SYMBOL_LEN: usize = 15;

impl From<Decimal> for MyDecimal {
    /// Converts hypersdk::Decimal to MyDecimal
    ///
    /// ## Panics
    /// Only if hypersdk::Decimal size doesn't match MyDecimal's size
    fn from(value: Decimal) -> Self {
        bytemuck::cast(value)
    }
}
