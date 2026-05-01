use std::{
    fs::File,
    path::{Path, PathBuf},
};

use arrow::datatypes::FieldRef;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use serde::{Serialize, de::DeserializeOwned};
use serde_arrow::schema::{SchemaLike, TracingOptions};

use crate::persistence::Persistable;

pub trait CompressStorable: Serialize + DeserializeOwned {
    /// Writes a `Vec<CompressStorable>` to `File` at `file_path`
    ///
    /// If `SIGKILL` is received:
    /// - before `writer.write`, no changes to FS
    /// - while `writer.write` or `writer.close`, `file_path` content will be corrupted
    /// - after `writer.write` OS has file contents written to cache, operation done
    fn write_items_to_parquet(
        items: &Vec<Self>,
        file_path: impl AsRef<Path>,
        compression: Compression,
    ) -> anyhow::Result<()> {
        let start = tokio::time::Instant::now();

        if items.is_empty() {
            return Ok(()); // Nothing to write
        }

        let fields = Vec::<FieldRef>::from_type::<Self>(
            TracingOptions::default().enums_without_data_as_strings(true),
        )?;
        let batch = serde_arrow::to_record_batch(&fields, items)?;

        let mut file = File::create_new(file_path)?;

        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();

        let mut writer = ArrowWriter::try_new(&mut file, batch.schema(), Some(props))?;

        writer.write(&batch)?;
        // This call writes headers
        writer.close()?;
        file.sync_all()?;

        log::info!(
            "[CompressStorable::write_items_to_parquet] took ({duration:?}) to complete",
            duration = start.elapsed()
        );

        Ok(())
    }

    #[cfg(test)]
    fn read_items_from_parquet(file_path: impl AsRef<Path>) -> anyhow::Result<Vec<Self>> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let file = File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        let mut all_items = Vec::new();
        for maybe_batch in reader {
            let batch = maybe_batch?;
            let chunk_items: Vec<Self> = serde_arrow::from_record_batch(&batch)?;
            all_items.extend(chunk_items);
        }

        Ok(all_items)
    }
}

pub enum CSFileName {
    Complete,
    Temp,
}

impl CSFileName {
    pub fn complete(&self) -> Option<&Self> {
        match &self {
            CSFileName::Complete => Some(self),
            CSFileName::Temp => None,
        }
    }

    pub fn temp(&self) -> Option<&Self> {
        match &self {
            CSFileName::Complete => None,
            CSFileName::Temp => Some(self),
        }
    }

    pub fn try_from_string_and_id<T: Persistable>(id_name: &str, value: String) -> Option<Self> {
        let mut split = value.split(".");

        // file_name part
        let file_name_wo_ext = split.next()?;

        if !T::valid_unique_file_name_wo_ext(file_name_wo_ext, id_name) {
            return None;
        }

        // extension part
        let pq_ext = split.next()?;
        if !pq_ext.eq("pq") {
            return None;
        };

        match split.next() {
            Some(tmp_ext) => {
                if !tmp_ext.eq("tmp") {
                    return None;
                }
                Some(Self::Temp)
            }
            None => Some(Self::Complete),
        }
    }
}

pub struct CompressedStorage<'a, T: Persistable> {
    dir_path: PathBuf,
    to_compress_receiver: &'a mut tokio::sync::mpsc::Receiver<super::ToCompress<T>>,
    compression_level: Compression,
}

impl<'a, T: Persistable> CompressedStorage<'a, T> {
    pub fn new(
        dir_path: PathBuf,
        to_compress_receiver: &'a mut tokio::sync::mpsc::Receiver<super::ToCompress<T>>,
        compression_level: Compression,
    ) -> Self {
        CompressedStorage {
            dir_path,
            to_compress_receiver,
            compression_level,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        while let Some(tc) = self.to_compress_receiver.recv().await {
            self.compress_and_delete(tc).await?;
        }
        log::warn!("Receiver's Channel closed");
        Ok(())
    }

    pub async fn find_old_files_pathbufs(
        id_name: &str,
        dir_path: impl AsRef<Path>,
    ) -> anyhow::Result<Vec<(PathBuf, CSFileName)>> {
        let mut entries = tokio::fs::read_dir(dir_path).await?;
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

            let Ok(file_name) = entry.file_name().into_string() else {
                continue;
            };

            let Some(csfn) = CSFileName::try_from_string_and_id::<T>(id_name, file_name) else {
                continue;
            };

            file_names.push((entry.path(), csfn));
        }

        Ok(file_names)
    }

    /// Writes compressed ".pq" file and deletes `to_delete_file_path`
    ///
    /// ## Left state
    /// Based on where (-A., A., A-B, B-C, C-D) it fails
    /// -  *-A*: The `to_delete_file_path` will remain uncompressed, no FS changes  
    /// -  *A.*: Refer to `write_items_to_parquet`
    /// - *A-B*: `.pq.tmp` temporal file written, contents not marked as commited yet
    /// - *B-C*: `.pq` file contents commited, need to remove `to_delete_file_path`
    /// - *C-*: Operation done! `.pq` file written and `to_delete_file_path` deleted
    async fn compress_and_delete(&self, tc: super::ToCompress<T>) -> anyhow::Result<PathBuf> {
        let mut file_name = tc
            .to_delete_file_path
            .file_name()
            .ok_or(anyhow::anyhow!("File name should be there"))?
            .to_string_lossy()
            .to_string();
        file_name.push_str(".pq");

        let file_path = self.dir_path.join(file_name);
        let compression_level = self.compression_level;
        let file_path_tmp = file_path.with_extension(".tmp");
        let file_path_closure = file_path_tmp.clone();

        // A.
        // Writer wants to use blocking `Writer`
        tokio::task::spawn_blocking(move || {
            let data = tc.data;
            T::write_items_to_parquet(&data, file_path_closure, compression_level)
        })
        .await??;

        // B.
        tokio::fs::rename(file_path_tmp, &file_path).await?;
        // C.
        tokio::fs::remove_file(tc.to_delete_file_path).await?;

        Ok(file_path)
    }
}

#[cfg(test)]
mod tests {
    use parquet::basic::ZstdLevel;
    use serde::Deserialize;
    use tokio::sync::mpsc;

    use crate::{
        init_logger,
        persistence::{ToCompress, hot_storage::HotStorable},
    };

    use super::*;

    #[derive(
        Serialize, Deserialize, Default, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord,
    )]
    struct CompressStorableMock {
        val: u8,
    }

    impl HotStorable for CompressStorableMock {}
    impl CompressStorable for CompressStorableMock {}

    fn get_channel() -> (
        mpsc::Sender<ToCompress<CompressStorableMock>>,
        mpsc::Receiver<ToCompress<CompressStorableMock>>,
    ) {
        let c = tokio::sync::mpsc::channel(100);
        (c.0, c.1)
    }

    #[tokio::test]
    async fn test_new() {
        init_logger();
        let (_, mut r) = get_channel();
        let dir = tempfile::tempdir().unwrap();
        let _ = CompressedStorage::new(
            dir.path().to_path_buf(),
            &mut r,
            Compression::ZSTD(ZstdLevel::try_new(9).unwrap()),
        );
    }

    #[tokio::test]
    async fn test_compress_and_delete() {
        init_logger();
        let (_, mut r) = get_channel();
        let dir = tempfile::tempdir().unwrap();
        let cs = CompressedStorage::new(
            dir.path().to_path_buf(),
            &mut r,
            Compression::ZSTD(ZstdLevel::try_new(9).unwrap()),
        );

        let datum = CompressStorableMock::default();
        let data = Vec::from_iter(std::iter::repeat_n(datum, 10000));

        let mut hot_file_path = dir.path().to_path_buf();
        hot_file_path.push("hot_file");
        let _file = tokio::fs::File::create(&hot_file_path).await.unwrap();

        let tc = ToCompress {
            to_delete_file_path: hot_file_path.clone(),
            data,
        };

        assert!(hot_file_path.exists());
        let file_path = cs.compress_and_delete(tc).await.unwrap();
        assert!(!hot_file_path.exists());
        let file = File::open(file_path).unwrap();
        log::info!(
            "[test_compress_and_delete] file metadata: {:?}",
            file.metadata().unwrap()
        );
    }

    #[tokio::test]
    async fn test_read_parquet() {
        init_logger();
        let (_, mut r) = get_channel();
        let dir = tempfile::tempdir().unwrap();
        let cs = CompressedStorage::new(
            dir.path().to_path_buf(),
            &mut r,
            Compression::ZSTD(ZstdLevel::try_new(9).unwrap()),
        );

        const DATA_N: usize = 10000;

        let datum = CompressStorableMock::default();
        let data = Vec::from_iter(std::iter::repeat_n(datum, DATA_N));

        let mut file_path = dir.path().to_path_buf();
        file_path.push("hot_file");
        let _file = tokio::fs::File::create(&file_path).await.unwrap();

        let tc = ToCompress {
            to_delete_file_path: file_path,
            data,
        };

        let file_path = cs.compress_and_delete(tc).await.unwrap();
        let data = CompressStorableMock::read_items_from_parquet(file_path).unwrap();
        assert_eq!(data.len(), DATA_N);
    }
}
