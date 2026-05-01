use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use postcard::{from_bytes_cobs, to_allocvec_cobs};
use serde::{Serialize, de::DeserializeOwned};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
};

use crate::persistence::Persistable;

const UFN_UUID_VERSION: uuid::Version = uuid::Version::SortRand;

pub trait HotStorable:
    DeserializeOwned + Serialize + Eq + std::fmt::Debug + std::hash::Hash + Clone + Ord
{
    fn unique_file_name_wo_ext(id_name: &str) -> String {
        assert!(!id_name.contains("_"));
        let mut ret = String::new();
        let uuid = uuid::Uuid::now_v7();
        assert_eq!(
            uuid.get_version()
                .expect("Should contain valid UUID version"),
            UFN_UUID_VERSION
        );

        ret.push_str(&uuid.to_string());
        ret.push('_');
        ret.push_str(id_name);

        assert!(Self::valid_unique_file_name_wo_ext(&ret, id_name));

        ret
    }

    fn valid_unique_file_name_wo_ext(file_name: &str, id_name: &str) -> bool {
        let mut split = file_name.split("_");

        // destructuring
        let Some(uuid_part) = split.next() else {
            return false;
        };
        let Some(id_name_part) = split.next() else {
            return false;
        };
        let None = split.next() else { return false };

        // parsing
        // uuid
        let Ok(uuid) = uuid::Uuid::parse_str(uuid_part) else {
            return false;
        };
        let Some(version) = uuid.get_version() else {
            return false;
        };
        if !version.eq(&UFN_UUID_VERSION) {
            return false;
        };

        // id_name
        if !id_name_part.eq(id_name) {
            return false;
        }

        true
    }

    // Doesn't check if the complete file name is valid, just the first part "[uuid]_"
    fn get_latest_file_name_match<'a>(file_names: impl Iterator<Item = &'a str>) -> Option<String> {
        file_names
            .flat_map(|f| f.split("_").next().map(|p_uuid| (f, p_uuid)))
            .flat_map(|(f, p_uuid)| uuid::Uuid::parse_str(p_uuid).map(|p_uuid| (f, p_uuid)))
            .filter(|(_, uuid)| matches!(uuid.get_version(), Some(uuid::Version::SortRand)))
            .max_by_key(|(_, uuid)| *uuid)
            .map(|(f, _)| f.to_string())
    }
}

pub struct HotStorage<'a, T: Persistable> {
    /// max desired RAM usage of the hot storage's `data`, can be surpassed by `(n - 1) * sizeof(n)` when `Self::push` is called with `n` items
    id_name: String,
    max_hot_bytes: usize,
    data: HashSet<T>,
    file: File,
    file_path: PathBuf,
    dir_path: PathBuf,
    push_calls: u8,
    to_compress_sender: &'a tokio::sync::mpsc::Sender<super::ToCompress<T>>,
}

impl<'a, T: Persistable> HotStorage<'a, T> {
    #[cfg(test)]
    fn get_ordered_data_vec(&self) -> Vec<T> {
        let mut vec: Vec<T> = self.data.iter().cloned().collect();
        vec.sort();
        vec
    }

    pub async fn find_old_files_pathbufs(
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

            let efn = entry.file_name();
            let Some(file_name) = efn.to_str() else {
                continue;
            };

            if !T::valid_unique_file_name_wo_ext(file_name, id_name) {
                continue;
            }

            file_names.push(entry.path());
        }

        Ok(file_names)
    }

    async fn recover_old_data(
        id_name: &str,
        dir_path: impl AsRef<Path>,
    ) -> anyhow::Result<(Vec<T>, Vec<PathBuf>)> {
        let old_files = Self::find_old_files_pathbufs(id_name, dir_path).await?;
        let mut data = Vec::new();

        for file in &old_files {
            let mut file = Self::open_file(file).await?;
            let n_data = Self::read_file(&mut file).await?;
            data.extend(n_data);
        }

        Ok((data, old_files))
    }

    /// Create a new `HotStorage`
    ///
    /// # Flow
    /// 1. Read all current hot files (chfs) and extract data (`Vec<T>`) from them
    /// 2. Write all that data to a new hot file
    /// 3. Delete all chfs
    pub async fn new(
        id_name: String,
        max_hot_bytes: usize,
        dir: PathBuf,
        to_compress_sender: &'a tokio::sync::mpsc::Sender<super::ToCompress<T>>,
    ) -> anyhow::Result<Self> {
        // 1.
        let (data, to_delete_files) = Self::recover_old_data(&id_name, &dir).await?;
        let data = HashSet::from_iter(data);

        // 2.
        let new_file_name = T::unique_file_name_wo_ext(&id_name);
        let mut file_full_path = dir.clone();
        file_full_path.push(new_file_name);
        let mut new_file = Self::open_file(&file_full_path).await?;

        Self::append_to_file(&mut new_file, data.iter()).await?;
        new_file.sync_all().await?;

        // 3.
        for to_delete_file in to_delete_files {
            fs::remove_file(to_delete_file).await?;
        }

        log::info!(
            "[HotStorage::new] loaded for ({id_name}) with sizeof(data) = {sizeof_data} ({percent}% usage)",
            sizeof_data = size_of::<T>() * data.len(),
            percent = size_of::<T>() * data.len() * 100 / max_hot_bytes,
        );

        Ok(Self {
            max_hot_bytes,
            data,
            file: new_file,
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

        for datum in data {
            if self.data.contains(&datum) {
                log::warn!(
                    "[HotStorage::push] id_name: {id} value already exists on data: {datum:?}",
                    id = self.id_name
                );
                continue;
            }

            self.data.insert(datum);
        }

        // Sync `File` contents with disk
        if self.data_ram_usage() >= self.max_hot_bytes {
            // File will be dropped, sync
            self.file.sync_data().await?;

            let new_file_name = T::unique_file_name_wo_ext(&self.id_name);
            log::warn!(
                "[HotStorage::push] data usage ({data_usage}) is higher than max ({max}), creating new hot file ({new_file_name})",
                data_usage = self.data_ram_usage(),
                max = self.max_hot_bytes
            );

            let mut new_hs = HashSet::new();

            // Clears self.data and gives us data to send
            std::mem::swap(&mut new_hs, &mut self.data);

            let mut full_data: Vec<T> = new_hs.into_iter().collect();
            full_data.sort();

            let mut new_file_path = self.dir_path.clone();
            new_file_path.push(new_file_name);

            self.file = Self::open_file(&new_file_path).await?;

            self.to_compress_sender
                .send(super::ToCompress {
                    to_delete_file_path: self.file_path.clone(),
                    data: full_data,
                })
                .await
                .map_err(|_| anyhow::anyhow!("Send failed, receiver dropped"))?;

            self.file_path = new_file_path.clone();
        } else if self.push_calls >= 10 {
            self.file.sync_data().await?;
            self.push_calls = 0.try_into().unwrap();
            log::debug!(
                "\ndata size: {}\nfile size: {:?}\nmax hot b: {}",
                self.data.len() * size_of::<T>(),
                self.file.stream_position().await,
                self.max_hot_bytes
            )
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
    /// Logs parsing error and returns `Ok(vec)` with records parsed so far
    /// ### `fs` error
    /// Returns `Err(_)`
    /// ### Valid (even if empty) `Vec<T>`
    /// Returns `Ok(vec)`
    async fn read_file(file: &mut File) -> anyhow::Result<Vec<T>> {
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
                    return Ok(records);
                }
            };
            records.push(record);
            deserialize_buf.clear();
        }

        Ok(records)
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

    #[derive(
        Serialize, Deserialize, Default, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord,
    )]
    struct HotStorableMock(usize);
    const MOCK_ID_NAME: &str = "HOTSTORABLEMOCK";

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
            &s,
        )
        .await?;
        let old_paths =
            HotStorage::<HotStorableMock>::find_old_files_pathbufs(MOCK_ID_NAME, dir.path())
                .await?;
        assert_eq!(old_paths.len(), 1);

        let (data, files) =
            HotStorage::<HotStorableMock>::recover_old_data(MOCK_ID_NAME, dir.path()).await?;

        assert_eq!(data.len(), 0);
        assert_eq!(files.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_hot_file_delete_empty() -> anyhow::Result<()> {
        init_logger();
        let dir = tempfile::tempdir()?;
        let (s, _) = get_channel();

        let hot_storage1 = HotStorage::<HotStorableMock>::new(
            MOCK_ID_NAME.to_string(),
            MAX_HOT_BYTES,
            dir.path().to_path_buf(),
            &s,
        )
        .await?;
        let hot_storage2 = HotStorage::<HotStorableMock>::new(
            MOCK_ID_NAME.to_string(),
            MAX_HOT_BYTES,
            dir.path().to_path_buf(),
            &s,
        )
        .await?;

        assert_ne!(hot_storage1.file_path, hot_storage2.file_path);
        assert!(!hot_storage1.file_path.exists());
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
            &s,
        )
        .await?;
        hot_storage1.push(mocks.clone().collect()).await?;

        let hot_storage2 = HotStorage::<HotStorableMock>::new(
            MOCK_ID_NAME.to_string(),
            MAX_HOT_BYTES,
            dir.path().to_path_buf(),
            &s,
        )
        .await?;

        assert_ne!(hot_storage1.file_path, hot_storage2.file_path);
        assert_eq!(hot_storage1.data.len(), N_MOCKS);
        assert_eq!(hot_storage1.data.len(), hot_storage2.data.len());
        assert!(
            hot_storage1
                .get_ordered_data_vec()
                .iter()
                .zip(hot_storage2.get_ordered_data_vec().iter().zip(mocks))
                .all(|(h1, (h2, m))| h1 == h2 && *h2 == m)
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_hot_file_triggers_compress() -> anyhow::Result<()> {
        init_logger();
        let dir = tempfile::tempdir()?;

        const N_MOCKS: usize = 400;
        const MAX_HOT_BYTES: usize = size_of::<HotStorableMock>() * (N_MOCKS / 4 * 3); // = 8 * (300) = 2400

        let mut mocks = (0..N_MOCKS).map(HotStorableMock);
        let (s, mut r) = get_channel();
        let mut hot_storage1 = HotStorage::<HotStorableMock>::new(
            MOCK_ID_NAME.to_string(),
            MAX_HOT_BYTES,
            dir.path().to_path_buf(),
            &s,
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

        assert!(r.try_recv().unwrap().data.is_sorted());
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
            &s,
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

        assert!(r.try_recv().unwrap().data.is_sorted());
        assert_ne!(start_file_path, end_file_path);
        assert_ne!(start_file_d, end_file_d);
        assert!(start_file_path.try_exists().unwrap());

        Ok(())
    }

    #[tokio::test]
    async fn test_recover_old_data_multiple_files() -> anyhow::Result<()> {
        init_logger();
        let dir = tempfile::tempdir()?;

        let h1_name = HotStorableMock::unique_file_name_wo_ext(MOCK_ID_NAME);
        let h1_path = dir.path().join(&h1_name);
        let mut file1 = tokio::fs::File::create(&h1_path).await?;
        HotStorage::<HotStorableMock>::append_to_file(
            &mut file1,
            [HotStorableMock(1), HotStorableMock(2)].iter(),
        )
        .await?;
        file1.sync_all().await?;

        let h2_name = HotStorableMock::unique_file_name_wo_ext(MOCK_ID_NAME);
        let h2_path = dir.path().join(&h2_name);
        let mut file2 = tokio::fs::File::create(&h2_path).await?;
        HotStorage::<HotStorableMock>::append_to_file(
            &mut file2,
            [HotStorableMock(3), HotStorableMock(4)].iter(),
        )
        .await?;
        file2.sync_all().await?;

        let (data, files) =
            HotStorage::<HotStorableMock>::recover_old_data(MOCK_ID_NAME, dir.path()).await?;

        assert_eq!(data.len(), 4);
        assert!(data.contains(&HotStorableMock(1)));
        assert!(data.contains(&HotStorableMock(2)));
        assert!(data.contains(&HotStorableMock(3)));
        assert!(data.contains(&HotStorableMock(4)));
        assert_eq!(files.len(), 2);
        assert!(files.contains(&h1_path));
        assert!(files.contains(&h2_path));

        Ok(())
    }

    #[tokio::test]
    async fn test_recover_old_data_corrupted_tail_returns_partial() -> anyhow::Result<()> {
        init_logger();
        let dir = tempfile::tempdir()?;

        let h1_name = HotStorableMock::unique_file_name_wo_ext(MOCK_ID_NAME);
        let h1_path = dir.path().join(&h1_name);
        let mut file1 = tokio::fs::File::create(&h1_path).await?;
        HotStorage::<HotStorableMock>::append_to_file(
            &mut file1,
            [HotStorableMock(1), HotStorableMock(2)].iter(),
        )
        .await?;
        file1.sync_all().await?;

        // Corrupt the tail
        use tokio::io::AsyncWriteExt;
        let mut file1_append = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&h1_path)
            .await?;
        file1_append.write_all(&[0x01, 0x02, 0x03, 0x04]).await?; // Random bytes that don't form a valid COBS struct
        file1_append.sync_all().await?;

        let (data, files) =
            HotStorage::<HotStorableMock>::recover_old_data(MOCK_ID_NAME, dir.path()).await?;

        assert_eq!(data.len(), 2);
        assert!(data.contains(&HotStorableMock(1)));
        assert!(data.contains(&HotStorableMock(2)));
        assert_eq!(files.len(), 1);
        assert!(files.contains(&h1_path));

        Ok(())
    }

    #[tokio::test]
    async fn test_recover_old_data_empty_directory() -> anyhow::Result<()> {
        init_logger();
        let dir = tempfile::tempdir()?;

        let (data, files) =
            HotStorage::<HotStorableMock>::recover_old_data(MOCK_ID_NAME, dir.path()).await?;

        assert!(data.is_empty());
        assert!(files.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_hot_storage_new_consolidates_recovered_files() -> anyhow::Result<()> {
        init_logger();
        let dir = tempfile::tempdir()?;
        let (s, _) = get_channel();

        let h1_name = HotStorableMock::unique_file_name_wo_ext(MOCK_ID_NAME);
        let h1_path = dir.path().join(&h1_name);
        let mut file1 = tokio::fs::File::create(&h1_path).await?;
        HotStorage::<HotStorableMock>::append_to_file(&mut file1, [HotStorableMock(1)].iter())
            .await?;
        file1.sync_all().await?;

        let h2_name = HotStorableMock::unique_file_name_wo_ext(MOCK_ID_NAME);
        let h2_path = dir.path().join(&h2_name);
        let mut file2 = tokio::fs::File::create(&h2_path).await?;
        HotStorage::<HotStorableMock>::append_to_file(&mut file2, [HotStorableMock(2)].iter())
            .await?;
        file2.sync_all().await?;

        let hot_storage = HotStorage::<HotStorableMock>::new(
            MOCK_ID_NAME.to_string(),
            MAX_HOT_BYTES,
            dir.path().to_path_buf(),
            &s,
        )
        .await?;

        assert_eq!(hot_storage.data.len(), 2);
        assert!(hot_storage.data.contains(&HotStorableMock(1)));
        assert!(hot_storage.data.contains(&HotStorableMock(2)));

        assert!(!h1_path.exists());
        assert!(!h2_path.exists());
        assert!(hot_storage.file_path.exists());

        assert_ne!(hot_storage.file_path, h1_path);
        assert_ne!(hot_storage.file_path, h2_path);

        Ok(())
    }

    #[tokio::test]
    async fn test_hot_storage_new_deduplicates_overlapping_data() -> anyhow::Result<()> {
        init_logger();
        let dir = tempfile::tempdir()?;
        let (s, _) = get_channel();

        let h1_name = HotStorableMock::unique_file_name_wo_ext(MOCK_ID_NAME);
        let h1_path = dir.path().join(&h1_name);
        let mut file1 = tokio::fs::File::create(&h1_path).await?;
        HotStorage::<HotStorableMock>::append_to_file(
            &mut file1,
            [HotStorableMock(1), HotStorableMock(2)].iter(),
        )
        .await?;
        file1.sync_all().await?;

        let h2_name = HotStorableMock::unique_file_name_wo_ext(MOCK_ID_NAME);
        let h2_path = dir.path().join(&h2_name);
        let mut file2 = tokio::fs::File::create(&h2_path).await?;
        HotStorage::<HotStorableMock>::append_to_file(
            &mut file2,
            [HotStorableMock(2), HotStorableMock(3)].iter(),
        )
        .await?;
        file2.sync_all().await?;

        let hot_storage = HotStorage::<HotStorableMock>::new(
            MOCK_ID_NAME.to_string(),
            MAX_HOT_BYTES,
            dir.path().to_path_buf(),
            &s,
        )
        .await?;

        assert_eq!(hot_storage.data.len(), 3);
        assert!(hot_storage.data.contains(&HotStorableMock(1)));
        assert!(hot_storage.data.contains(&HotStorableMock(2)));
        assert!(hot_storage.data.contains(&HotStorableMock(3)));

        assert!(!h1_path.exists());
        assert!(!h2_path.exists());

        Ok(())
    }
}
