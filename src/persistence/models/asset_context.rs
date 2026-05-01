use hypersdk::{Decimal, hypercore::AssetContext};
use postcard::experimental::max_size::MaxSize;
use serde::{Deserialize, Serialize};

use crate::{
    get_timestamp,
    persistence::{
        compressed_storage::CompressStorable, hot_storage::HotStorable, models::MyDecimal,
    },
};

#[derive(Clone, Serialize, Deserialize, Debug, MaxSize, PartialEq, Eq, Hash)]
pub struct MyAssetContext {
    pub timestamp: u64,
    pub funding: MyDecimal,
    pub open_interest: MyDecimal,
    pub mark_px: Option<MyDecimal>,
    pub oracle_px: Option<MyDecimal>,
    pub mid_px: Option<MyDecimal>,
    pub premium: MyDecimal,
    pub prev_day_px: MyDecimal,
    pub day_ntl_vlm: MyDecimal,
    pub impact_pxs: Option<[MyDecimal; 2]>,
}

impl Ord for MyAssetContext {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl PartialOrd for MyAssetContext {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl From<AssetContext> for MyAssetContext {
    fn from(value: AssetContext) -> Self {
        let AssetContext {
            funding,
            open_interest,
            mark_px,
            oracle_px,
            mid_px,
            premium,
            prev_day_px,
            day_ntl_vlm,
            impact_pxs,
        } = value
            // .clone()
        ;

        let timestamp = get_timestamp();
        let ret = Self {
            timestamp,
            funding: funding.into(),
            open_interest: open_interest.into(),
            mark_px: mark_px.map(Into::into),
            oracle_px: oracle_px.map(Into::into),
            mid_px: mid_px.map(Into::into),
            premium: premium.into(),
            prev_day_px: prev_day_px.into(),
            day_ntl_vlm: day_ntl_vlm.into(),
            impact_pxs: impact_pxs.map(|pxs| {
                let f = Decimal::from_str_exact(&pxs[0]).unwrap();
                let s = Decimal::from_str_exact(&pxs[1]).unwrap();
                [f.into(), s.into()]
            }),
        };
        // log::info!("Created: {ret:?} \nfrom: {value:?}");
        ret
    }
}

impl HotStorable for MyAssetContext {}
impl CompressStorable for MyAssetContext {}

#[cfg(test)]
mod tests {}
