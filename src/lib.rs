use crate::ingest::IngestService;

mod ingest;
mod persistence;

#[derive(Default)]
pub struct TickData {
    ingest: IngestService,
}

impl TickData {
    pub async fn run(&mut self) {
        self.ingest.run().await;
    }
}

#[cfg(test)]
mod tests {}
