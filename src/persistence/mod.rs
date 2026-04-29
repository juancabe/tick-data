use std::path::PathBuf;

use futures::future::select;
use parquet::basic::Compression;

use crate::{
    Runnable,
    persistence::{compressed_storage::CompressedStorage, hot_storage::HotStorage},
};

pub mod compressed_storage;
pub mod hot_storage;
pub mod models;

pub trait Persistable:
    hot_storage::HotStorable + compressed_storage::CompressStorable + Send + Sync + 'static
{
}

impl<T> Persistable for T where
    T: hot_storage::HotStorable + compressed_storage::CompressStorable + Send + Sync + 'static
{
}

pub struct ToCompress<T: Persistable> {
    pub to_delete_file_path: PathBuf,
    pub data: Vec<T>,
}

#[async_trait::async_trait]
impl<T: Persistable> Runnable for Persistence<T> {
    async fn run(&mut self) -> anyhow::Result<()> {
        self.run_persistence().await
    }
}

pub struct Persistence<T: Persistable> {
    id_name: String,
    to_persist_receiver: tokio::sync::mpsc::Receiver<Vec<T>>,
    hot_storage_dir: PathBuf,
    compressed_storage_dir: PathBuf,
    max_hot_bytes: usize,
    compression_level: Compression,
}

impl<T: Persistable> Persistence<T> {
    pub fn new(
        id_name: String,
        to_persist_receiver: tokio::sync::mpsc::Receiver<Vec<T>>,
        hot_storage_dir: PathBuf,
        compressed_storage_dir: PathBuf,
        max_hot_bytes: usize,
        compression_level: Compression,
    ) -> Self {
        Self {
            id_name,
            to_persist_receiver,
            hot_storage_dir,
            compressed_storage_dir,
            max_hot_bytes,
            compression_level,
        }
    }

    pub async fn run_persistence(&mut self) -> anyhow::Result<()> {
        let Persistence {
            to_persist_receiver,
            hot_storage_dir,
            compressed_storage_dir,
            max_hot_bytes,
            compression_level,
            id_name,
        } = self;

        let (compress_sender, mut compress_receiver) =
            tokio::sync::mpsc::channel::<ToCompress<T>>(100);

        loop {
            match select(
                core::pin::pin!(Self::receive_loop(
                    id_name.clone(),
                    to_persist_receiver,
                    hot_storage_dir.clone(),
                    &compress_sender,
                    *max_hot_bytes,
                )),
                core::pin::pin!(Self::run_compression(
                    compressed_storage_dir.clone(),
                    &mut compress_receiver,
                    *compression_level
                )),
            )
            .await
            {
                // TODO: handle failure ?
                futures::future::Either::Left(l) => {
                    l.0.unwrap();
                    todo!("Handle")
                }
                futures::future::Either::Right(r) => {
                    r.0.unwrap();
                    todo!("Handle")
                }
            }
        }
    }

    async fn run_compression(
        compressed_storage_dir: PathBuf,
        compress_receiver: &mut tokio::sync::mpsc::Receiver<ToCompress<T>>,
        compression_level: Compression,
    ) -> anyhow::Result<()> {
        let mut cs =
            CompressedStorage::new(compressed_storage_dir, compress_receiver, compression_level);
        cs.run().await?;
        Ok(())
    }

    async fn receive_loop(
        id_name: String,
        to_persist_receiver: &mut tokio::sync::mpsc::Receiver<Vec<T>>,
        hot_storage_dir: PathBuf,
        compress_sender: &tokio::sync::mpsc::Sender<ToCompress<T>>,
        max_hot_bytes: usize,
    ) -> anyhow::Result<()> {
        let mut hot_storage =
            HotStorage::new(id_name, max_hot_bytes, hot_storage_dir, compress_sender).await?;
        while let Some(to_persist) = to_persist_receiver.recv().await {
            hot_storage.push(to_persist).await?;
        }
        Ok(())
    }
}
