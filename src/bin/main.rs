#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mut td = tick_data::TickData::default();
    td.run().await;
    Ok(())
}
