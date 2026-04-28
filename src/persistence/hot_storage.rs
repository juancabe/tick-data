use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use postcard::{from_bytes_cobs, to_allocvec_cobs};
use serde::{Serialize, de::DeserializeOwned};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
};

use crate::persistence::Persistable;

pub trait HotStorable:
    DeserializeOwned + Serialize + Eq + std::fmt::Debug + std::hash::Hash
{
    fn unique_file_name(id_name: &str) -> String {
        let mut ret = String::new();
        ret.push_str(&uuid::Uuid::now_v7().to_string());
        ret.push('_');
        ret.push_str(id_name);
        ret
    }

    fn get_latest_file_name_match<'a>(file_names: impl Iterator<Item = &'a str>) -> Option<String> {
        file_names
            .flat_map(|f| f.split("_").next().map(|p_uuid| (f, p_uuid)))
            .flat_map(|(f, p_uuid)| uuid::Uuid::parse_str(p_uuid).map(|p_uuid| (f, p_uuid)))
            .filter(|(_, uuid)| matches!(uuid.get_version(), Some(uuid::Version::SortRand)))
            .max_by_key(|(_, uuid)| *uuid)
            .map(|(f, _)| f.to_string())
    }
}

pub struct HotStorage<T: Persistable> {
    /// max desired RAM usage of the hot storage's `data`, can be surpassed by `(n - 1) * sizeof(n)` when `Self::push` is called with `n` items
    id_name: String,
    max_hot_bytes: usize,
    data: HashSet<T>,
    file: File,
    file_path: PathBuf,
    dir_path: PathBuf,
    push_calls: u8,
    to_compress_sender: tokio::sync::mpsc::Sender<super::ToCompress<T>>,
}

impl<T: Persistable> HotStorage<T> {
    async fn find_old_files_pathbufs(
        id_name: &str,
        dir_path: impl AsRef<Path>,
    ) -> anyhow::Result<Vec<PathBuf>> {
        let mut entries = fs::read_dir(dir_path).await?;
        let mut file_names = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            log::debug!("Entry found: {entry:?}");
            let Ok(metadata) = entry.metadata().await else {
                // Symlinks may return err if broken
                log::debug!("Entry metadata error");
                continue;
            };
            if !metadata.is_file() {
                log::debug!("Entry is not file");
                continue;
            }

            if !entry.file_name().to_string_lossy().contains(id_name)
                || entry.file_name().to_string_lossy().contains(".pq")
            {
                log::debug!(
                    "Entry file_name ({}) doesn't contain: {}",
                    entry.file_name().to_string_lossy(),
                    id_name
                );
                continue;
            }

            file_names.push(entry.path());
        }

        Ok(file_names)
    }

    async fn recover_old_file(
        id_name: &str,
        dir_path: impl AsRef<Path>,
    ) -> anyhow::Result<Option<String>> {
        let old_files = Self::find_old_files_pathbufs(id_name, dir_path).await?;
        let file_names = old_files
            .iter()
            .filter_map(|pb| pb.file_name())
            .filter_map(|osstr| osstr.to_str());
        let last_file = T::get_latest_file_name_match(file_names);
        log::info!("last_file: {:?}", last_file);
        Ok(last_file)
    }

    pub async fn new(
        id_name: String,
        max_hot_bytes: usize,
        dir: PathBuf,
        to_compress_sender: tokio::sync::mpsc::Sender<super::ToCompress<T>>,
    ) -> anyhow::Result<Self> {
        let file_name = Self::recover_old_file(&id_name, &dir)
            .await?
            .unwrap_or_else(|| T::unique_file_name(&id_name));
        let mut file_full_path = dir.clone();
        file_full_path.push(file_name);

        let mut file = Self::open_file(&file_full_path).await?;

        let data = match Self::read_file(&mut file).await? {
            Some(d) => HashSet::from_iter(d),
            None => {
                file.set_len(0).await?;
                log::info!("File {file_full_path:?} size was truncated to 0");
                HashSet::new()
            }
        };

        Ok(Self {
            max_hot_bytes,
            data,
            file,
            file_path: file_full_path,
            to_compress_sender,
            dir_path: dir,
            id_name,
            push_calls: 1,
        })
    }

    async fn open_file(file_full_path: impl AsRef<Path>) -> anyhow::Result<File> {
        log::debug!("[open_file] Opening {:?}", file_full_path.as_ref());
        let ret = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(file_full_path.as_ref())
            .await
            .map_err(Into::into);
        log::debug!("[open_file] Opened {:?}", file_full_path.as_ref());
        ret
    }

    pub fn data_ram_usage(&self) -> usize {
        self.data.len() * std::mem::size_of::<T>()
    }

    /// Push `iter` values to RAM collection (`data`) and `File`
    /// `File` contents are synced to disk every 10 calls to `push`
    pub async fn push(&mut self, data: Vec<T>) -> anyhow::Result<()> {
        Self::append_to_file(&mut self.file, data.iter()).await?;
        self.data.extend(data);

        // Sync `File` contents with disk
        if self.data_ram_usage() >= self.max_hot_bytes {
            // File will be dropped, sync
            self.file.sync_data().await?;

            let new_file_name = T::unique_file_name(&self.id_name);
            log::warn!(
                "[HotStorage::push] data usage ({data_usage}) is higher than max ({max}), creating new hot file ({new_file_name})",
                data_usage = self.data_ram_usage(),
                max = self.max_hot_bytes
            );

            let mut new_vec = HashSet::new();
            // Clears self.data and gives us data to send
            std::mem::swap(&mut new_vec, &mut self.data);

            let mut new_file_path = self.dir_path.clone();
            new_file_path.push(new_file_name);

            self.file = Self::open_file(&new_file_path).await?;

            self.to_compress_sender
                .send(super::ToCompress {
                    to_delete_file_path: self.file_path.clone(),
                    data: new_vec.into_iter().collect(),
                })
                .await
                .map_err(|_| anyhow::anyhow!("Send failed, receiver dropped"))?;

            self.file_path = new_file_path.clone();
        } else if self.push_calls >= 10 {
            self.file.sync_data().await?;
            self.push_calls = 0.try_into().unwrap();
        }

        self.push_calls += 1;

        log::debug!(
            "[HotStorage::push] syncing file contents to disk | data_ram_usage: {}",
            self.data_ram_usage()
        );

        Ok(())
    }

    /// Writes COBS serialized `val` to `File`
    /// Can fail on serializing and writing the file
    /// File contents not synced to disk
    async fn append_to_file(file: &mut File, iter: impl Iterator<Item = &T>) -> anyhow::Result<()> {
        let mut bytes: Vec<Vec<u8>> = Vec::new();
        for t in iter {
            bytes.push(to_allocvec_cobs(t)?);
        }
        file.write_all(&bytes.concat()).await?;
        Ok(())
    }

    /// Reads `file` contents and tries to parse them as `Vec<T>`
    ///
    /// ## Behaviour
    /// Reads the file as given (assumes cursor is at beginning)
    /// When finished cursor'll be placed at the end of the `File` unless an error causes early return
    ///
    /// ### Parse error case
    /// Logs parsing error and returns Ok(None)
    /// ### `fs` error
    /// Returns `Err(_)`
    /// ### Valid (even if empty) `Vec<T>`
    /// Returns `Ok(vec)`
    async fn read_file(file: &mut File) -> anyhow::Result<Option<Vec<T>>> {
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();
        let mut deserialize_buf = Vec::new();

        loop {
            let size = reader.read_until(0, &mut deserialize_buf).await?;
            if size == 0 {
                break;
            }

            let record: T = match from_bytes_cobs(&mut deserialize_buf) {
                Ok(r) => r,
                Err(pe) => {
                    log::warn!("Parse error reading file: {pe:?}");
                    return Ok(None);
                }
            };
            records.push(record);
            deserialize_buf.clear();
        }

        Ok(Some(records))
    }
}

#[cfg(test)]
mod tests {
    use std::os::fd::AsRawFd;

    use crate::{init_logger, persistence::compressed_storage::CompressStorable};

    use super::super::*;
    use super::*;
    use serde::Deserialize;
    use tokio::sync::mpsc;

    #[derive(Serialize, Deserialize, Default, Clone, Copy, Debug, PartialEq, Eq, Hash)]
    struct HotStorableMock(usize);
    const MOCK_ID_NAME: &str = "HOT_STORABLE_MOCK";

    impl HotStorable for HotStorableMock {}
    impl CompressStorable for HotStorableMock {}

    const MAX_HOT_BYTES: usize = 100_000_000;

    fn get_channel() -> (
        mpsc::Sender<ToCompress<HotStorableMock>>,
        mpsc::Receiver<ToCompress<HotStorableMock>>,
    ) {
        let c = tokio::sync::mpsc::channel(100);
        (c.0, c.1)
    }

    #[tokio::test]
    async fn test_hot_file_create() -> anyhow::Result<()> {
        init_logger();
        let dir = tempfile::tempdir()?;
        let (s, _) = get_channel();

        let _ = HotStorage::<HotStorableMock>::new(
            MOCK_ID_NAME.to_string(),
            MAX_HOT_BYTES,
            dir.path().to_path_buf(),
            s,
        )
        .await?;
        let old_paths =
            HotStorage::<HotStorableMock>::find_old_files_pathbufs(MOCK_ID_NAME, dir.path())
                .await?;
        assert_eq!(old_paths.len(), 1);

        HotStorage::<HotStorableMock>::recover_old_file(MOCK_ID_NAME, dir.path())
            .await?
            .expect("Should exist");

        Ok(())
    }

    #[tokio::test]
    async fn test_hot_file_reuse_empty() -> anyhow::Result<()> {
        init_logger();
        let dir = tempfile::tempdir()?;
        let (s, _) = get_channel();

        let hot_storage1 = HotStorage::<HotStorableMock>::new(
            MOCK_ID_NAME.to_string(),
            MAX_HOT_BYTES,
            dir.path().to_path_buf(),
            s.clone(),
        )
        .await?;
        let hot_storage2 = HotStorage::<HotStorableMock>::new(
            MOCK_ID_NAME.to_string(),
            MAX_HOT_BYTES,
            dir.path().to_path_buf(),
            s.clone(),
        )
        .await?;

        assert_eq!(hot_storage1.file_path, hot_storage2.file_path);
        Ok(())
    }

    #[tokio::test]
    async fn test_hot_file_reuse() -> anyhow::Result<()> {
        init_logger();
        let dir = tempfile::tempdir()?;

        const N_MOCKS: usize = 4;
        let mocks = (0..N_MOCKS).map(HotStorableMock);

        let (s, _) = get_channel();
        let mut hot_storage1 = HotStorage::<HotStorableMock>::new(
            MOCK_ID_NAME.to_string(),
            MAX_HOT_BYTES,
            dir.path().to_path_buf(),
            s.clone(),
        )
        .await?;
        hot_storage1.push(mocks.collect()).await?;

        let hot_storage2 = HotStorage::<HotStorableMock>::new(
            MOCK_ID_NAME.to_string(),
            MAX_HOT_BYTES,
            dir.path().to_path_buf(),
            s.clone(),
        )
        .await?;

        assert_eq!(hot_storage1.file_path, hot_storage2.file_path);
        assert_eq!(hot_storage1.data.len(), N_MOCKS);
        assert_eq!(hot_storage1.data.len(), hot_storage2.data.len());

        Ok(())
    }

    #[tokio::test]
    async fn test_hot_file_triggers_compress() -> anyhow::Result<()> {
        init_logger();
        let dir = tempfile::tempdir()?;

        const N_MOCKS: usize = 400;
        const MAX_HOT_BYTES: usize = size_of::<HotStorableMock>() * (N_MOCKS / 4 * 3);

        let mut mocks = (0..N_MOCKS).map(HotStorableMock);
        let (s, mut r) = get_channel();
        let mut hot_storage1 = HotStorage::<HotStorableMock>::new(
            MOCK_ID_NAME.to_string(),
            MAX_HOT_BYTES,
            dir.path().to_path_buf(),
            s.clone(),
        )
        .await?;

        const BATCH_N: usize = 4;
        // This for loop pushes 3200 bytes = (u64 * 400) to ram
        for _ in 0..(N_MOCKS / BATCH_N) {
            log::info!("[tests::test_hot_file_triggers_compress] pushing {BATCH_N} values",);
            hot_storage1
                .push(mocks.by_ref().take(BATCH_N).collect())
                .await
                .unwrap();
        }

        log::info!(
            "[tests::test_hot_file_triggers_compress] finished pushing {} total values",
            size_of::<HotStorableMock>() * N_MOCKS
        );
        assert!(r.try_recv().is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_hot_file_triggered_compress_created_new_file() -> anyhow::Result<()> {
        init_logger();
        let dir = tempfile::tempdir()?;

        const N_MOCKS: usize = 400;
        const MAX_HOT_BYTES: usize = size_of::<HotStorableMock>() * (N_MOCKS / 4 * 3);

        let mut mocks = (0..N_MOCKS).map(HotStorableMock);
        let (s, mut r) = get_channel();
        let mut hot_storage1 = HotStorage::<HotStorableMock>::new(
            MOCK_ID_NAME.to_string(),
            MAX_HOT_BYTES,
            dir.path().to_path_buf(),
            s.clone(),
        )
        .await?;

        let start_file_path = hot_storage1.file_path.clone();
        let start_file_d = hot_storage1.file.as_raw_fd();
        const BATCH_N: usize = 4;
        // This for loop pushes 3200 bytes = (u64 * 400) to ram
        for _ in 0..(N_MOCKS / BATCH_N) {
            log::info!("[tests::test_hot_file_triggers_compress] pushing {BATCH_N} values",);
            hot_storage1
                .push(mocks.by_ref().take(BATCH_N).collect())
                .await
                .unwrap();
        }

        let end_file_path = hot_storage1.file_path.clone();
        let end_file_d = hot_storage1.file.as_raw_fd();
        log::info!(
            "[tests::test_hot_file_triggers_compress] finished pushing {} total values",
            size_of::<HotStorableMock>() * N_MOCKS
        );

        assert!(r.try_recv().is_ok());
        assert_ne!(start_file_path, end_file_path);
        assert_ne!(start_file_d, end_file_d);
        assert!(start_file_path.try_exists().unwrap());

        Ok(())
    }
}
