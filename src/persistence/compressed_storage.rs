use std::{
    fs::File,
    path::{Path, PathBuf},
};

use arrow::datatypes::FieldRef;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_arrow::schema::{SchemaLike, TracingOptions};

use crate::persistence::Persistable;

pub trait CompressStorable: Serialize + DeserializeOwned {
    fn write_items_to_parquet(
        items: &Vec<Self>,
        file_path: impl AsRef<Path>,
        compression: Compression,
    ) -> anyhow::Result<()> {
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

        Ok(())
    }
    fn read_items_from_parquet(file_path: impl AsRef<Path>) -> anyhow::Result<Vec<Self>> {
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

pub struct CompressedStorage<T: Persistable> {
    dir_path: PathBuf,
    to_compress_receiver: tokio::sync::mpsc::Receiver<super::ToCompress<T>>,
    compression_level: Compression,
}

impl<T: Persistable> CompressedStorage<T> {
    pub fn new(
        dir_path: PathBuf,
        to_compress_receiver: tokio::sync::mpsc::Receiver<super::ToCompress<T>>,
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
        let file_path_c = file_path.clone();

        // Writer wants to use blocking `Writer`
        tokio::task::spawn_blocking(move || {
            let data = tc.data;
            T::write_items_to_parquet(&data, file_path, compression_level)
        })
        .await??;

        tokio::fs::remove_file(tc.to_delete_file_path).await?;
        Ok(file_path_c)
    }
}

#[cfg(test)]
mod tests {
    use parquet::basic::GzipLevel;
    use serde::Deserialize;
    use tokio::sync::mpsc;

    use crate::{
        init_logger,
        persistence::{ToCompress, hot_storage::HotStorable},
    };

    use super::*;

    #[derive(Serialize, Deserialize, Default, Clone, Copy, Debug, PartialEq, Eq)]
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
        let (_, r) = get_channel();
        let dir = tempfile::tempdir().unwrap();
        let _ = CompressedStorage::new(
            dir.path().to_path_buf(),
            r,
            Compression::GZIP(GzipLevel::try_new(7).unwrap()),
        );
    }

    #[tokio::test]
    async fn test_compress_and_delete() {
        init_logger();
        let (_, r) = get_channel();
        let dir = tempfile::tempdir().unwrap();
        let cs = CompressedStorage::new(
            dir.path().to_path_buf(),
            r,
            Compression::GZIP(GzipLevel::try_new(7).unwrap()),
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
        let (_, r) = get_channel();
        let dir = tempfile::tempdir().unwrap();
        let cs = CompressedStorage::new(
            dir.path().to_path_buf(),
            r,
            Compression::GZIP(GzipLevel::try_new(7).unwrap()),
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
