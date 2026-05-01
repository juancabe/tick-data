use std::path::{Path, PathBuf};

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
    fn are_associated_hf_cf(hf_path: &Path, cf_path: &Path) -> bool {
        let Some(hf_name) = hf_path.file_name() else {
            log::warn!("[associated_hf_cf] Unexpected no file_name of hf_path");
            return false;
        };

        let Some(cf_name) = cf_path.file_name() else {
            log::warn!("[associated_hf_cf] Unexpected no file_name of cf_path");
            return false;
        };

        hf_name
            .to_str()
            .and_then(|hf| {
                cf_name
                    .to_str()
                    .and_then(|cf| cf.split(".").next())
                    .map(|cf| cf == hf)
            })
            .unwrap_or(false)
    }

    /// Cleans start state from leftover objects on File System
    ///
    /// ## Objects to expect
    /// ### Compressed storage dir (`compressed_storage_dir`)
    /// - `.pq`: finished compressed files
    ///    - *CASE A*: a `HotStorage` _h_ file is associated with a `.pq` (complete), remove it as it is a leftover from `CompressedStorage::compress_and_delete` *B-C* state
    /// - `.pq.tmp`
    ///    - *CASE B*: remove it, assert an associated _h_ file exists on `hot_storage_dir`
    ///
    /// ### Hot storage dir (`hot_storage_dir`)
    /// - One or more _h_ files
    ///
    async fn startup_cleanup(
        id_name: &str,
        hot_storage_dir: &Path,
        compressed_storage_dir: &Path,
    ) -> anyhow::Result<()> {
        let hs_file_names =
            HotStorage::<T>::find_old_files_pathbufs(id_name, hot_storage_dir).await?;

        let cs_file_names =
            CompressedStorage::<T>::find_old_files_pathbufs(id_name, compressed_storage_dir)
                .await?;

        // CASE A
        let hfs_to_delete = hs_file_names.iter().filter(|hf| {
            cs_file_names
                .iter()
                .filter_map(|(path, cf)| cf.complete().map(|_| path))
                .any(|cf| Self::are_associated_hf_cf(hf, cf))
        });

        // CASE B
        let cfs_to_delete = cs_file_names
            .iter()
            .filter_map(|(path, cf)| cf.temp().map(|_| path))
            .filter(|cf| {
                if !hs_file_names
                    .iter()
                    .any(|hf| Self::are_associated_hf_cf(hf, cf))
                {
                    log::error!("Temp compressed file (.pq.tmp) without corresponding hf: {cf:?}");
                    false
                } else {
                    true
                }
            });

        for to_delete in hfs_to_delete.chain(cfs_to_delete) {
            // try delete
            if let Err(e) = tokio::fs::remove_file(&to_delete).await {
                log::error!("Error removing file: {e:?}");
            } else {
                log::info!("Deleted file: {to_delete:?}");
            }
        }

        Ok(())
    }

    pub async fn new(
        id_name: String,
        to_persist_receiver: tokio::sync::mpsc::Receiver<Vec<T>>,
        hot_storage_dir: PathBuf,
        compressed_storage_dir: PathBuf,
        max_hot_bytes: usize,
        compression_level: Compression,
    ) -> anyhow::Result<Self> {
        Self::startup_cleanup(&id_name, &hot_storage_dir, &compressed_storage_dir)
            .await
            .map(|()| Self {
                id_name,
                to_persist_receiver,
                hot_storage_dir,
                compressed_storage_dir,
                max_hot_bytes,
                compression_level,
            })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{hot_storage::HotStorable, compressed_storage::CompressStorable};
    use serde::{Serialize, Deserialize};

    #[derive(
        Serialize, Deserialize, Default, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord,
    )]
    struct MockPersistable;
    impl HotStorable for MockPersistable {}
    impl CompressStorable for MockPersistable {}

    const MOCK_ID: &str = "MOCKID";

    async fn setup_dirs() -> (tempfile::TempDir, tempfile::TempDir) {
        crate::init_logger();
        (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap())
    }

    #[tokio::test]
    async fn test_startup_cleanup_aborted_parquet_write() {
        let (hot_dir, comp_dir) = setup_dirs().await;
        
        // Setup: h.1 and h.1.pq.tmp
        let h1_name = MockPersistable::unique_file_name_wo_ext(MOCK_ID);
        let h1_path = hot_dir.path().join(&h1_name);
        tokio::fs::write(&h1_path, "hot data").await.unwrap();

        let tmp_name = format!("{}.pq.tmp", h1_name);
        let tmp_path = comp_dir.path().join(&tmp_name);
        tokio::fs::write(&tmp_path, "tmp data").await.unwrap();

        // Action
        Persistence::<MockPersistable>::startup_cleanup(MOCK_ID, hot_dir.path(), comp_dir.path())
            .await
            .unwrap();

        // Assertion: h.1.pq.tmp deleted, h.1 exists
        assert!(!tmp_path.exists(), "tmp file should be deleted");
        assert!(h1_path.exists(), "hot file should be preserved");
    }

    #[tokio::test]
    async fn test_startup_cleanup_ghost_of_success() {
        let (hot_dir, comp_dir) = setup_dirs().await;
        
        // Setup: h.1 and h.1.pq
        let h1_name = MockPersistable::unique_file_name_wo_ext(MOCK_ID);
        let h1_path = hot_dir.path().join(&h1_name);
        tokio::fs::write(&h1_path, "hot data").await.unwrap();

        let pq_name = format!("{}.pq", h1_name);
        let pq_path = comp_dir.path().join(&pq_name);
        tokio::fs::write(&pq_path, "pq data").await.unwrap();

        // Action
        Persistence::<MockPersistable>::startup_cleanup(MOCK_ID, hot_dir.path(), comp_dir.path())
            .await
            .unwrap();

        // Assertion: h.1 deleted, h.1.pq exists
        assert!(!h1_path.exists(), "hot file should be deleted");
        assert!(pq_path.exists(), "pq file should be preserved");
    }

    #[tokio::test]
    async fn test_startup_cleanup_uncompressed_safeguard() {
        let (hot_dir, comp_dir) = setup_dirs().await;
        
        // Setup: h.1 only
        let h1_name = MockPersistable::unique_file_name_wo_ext(MOCK_ID);
        let h1_path = hot_dir.path().join(&h1_name);
        tokio::fs::write(&h1_path, "hot data").await.unwrap();

        // Action
        Persistence::<MockPersistable>::startup_cleanup(MOCK_ID, hot_dir.path(), comp_dir.path())
            .await
            .unwrap();

        // Assertion: h.1 exists
        assert!(h1_path.exists(), "hot file should be preserved");
    }

    #[tokio::test]
    async fn test_startup_cleanup_orphaned_pq_tmp_error_log() {
        let (hot_dir, comp_dir) = setup_dirs().await;
        
        // Setup: ONLY h.1.pq.tmp (orphaned)
        let h1_name = MockPersistable::unique_file_name_wo_ext(MOCK_ID);
        let tmp_name = format!("{}.pq.tmp", h1_name);
        let tmp_path = comp_dir.path().join(&tmp_name);
        tokio::fs::write(&tmp_path, "tmp data").await.unwrap();

        // Action
        Persistence::<MockPersistable>::startup_cleanup(MOCK_ID, hot_dir.path(), comp_dir.path())
            .await
            .unwrap();

        // Assertion: Based on user instruction, orphaned .pq.tmp is NOT deleted
        assert!(tmp_path.exists(), "orphaned tmp file should NOT be deleted");
    }
}
