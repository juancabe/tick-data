use std::path::{Path, PathBuf};

use bytemuck::checked::from_bytes;
use postcard::{experimental::max_size::MaxSize, from_bytes_cobs, to_allocvec, to_allocvec_cobs, to_vec};
use serde::{Serialize, de::DeserializeOwned};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
};

mod trade;

pub trait HotStorable: MaxSize + DeserializeOwned + Serialize {
    const IDENTIFY_NAME: &'static str;

    fn unique_file_name() -> PathBuf {
        let mut ret = String::new();
        ret.push_str(&uuid::Uuid::now_v7().to_string());
        ret.push_str(Self::IDENTIFY_NAME);
        PathBuf::from(ret)
    }
}

pub struct HotStorage<T: HotStorable> {
    max_hot_bytes: usize,
    data: Vec<T>,
    file: File,
}

impl<T: HotStorable> HotStorage<T> {
    pub async fn new(max_hot_bytes: usize, mut dir: PathBuf) -> anyhow::Result<Self> {
        dir.push(T::unique_file_name());
        let file_full_path = dir;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(&file_full_path)
            .await?;

        let data = match Self::read_file(&mut file).await? {
            Some(d) => d,
            None => {
                file.set_len(0).await?;
                log::info!("File {file_full_path:?} size was truncated to 0");
                Vec::new()
            }
        };

        Ok(Self {
            max_hot_bytes,
            data,
            file,
        })
    }

    pub async fn push(&mut self, val: T) {
        self.data.push(val);
    }

    /// Writes COBS serialized `val` to `File`
    /// Can fail on serializing and writing the file
    async fn append_to_file(file: &mut File, val: &T) -> anyhow::Result<()> {
        let bytes = to_allocvec_cobs(val)?;
        file.write_all(&bytes).await?;
        Ok(())
    }

    /// Reads `file` contents and tries to parse them as `Vec<T>`
    ///
    /// ## Behaviour
    /// ### Parse error case
    /// Logs parsing error and returns Ok(None)
    /// ### `fs` error
    /// Returns `Err(_)`
    /// ### Valid (even if empty) `Vec<T>`
    /// Returns `Ok(vec)`
    async fn read_file(file: &mut File) -> anyhow::Result<Option<Vec<T>>> {
        let start_pos = file.stream_position().await?;

        let mut reader = BufReader::new(file);
        let mut records = Vec::new();
        let mut deserialize_buf = Vec::new()

        loop {
            let size = reader.read_until(0, &mut deserialize_buf).await?;
            if size == 0 {
                break;
            }

            let record = match from_bytes_cobs(&mut deserialize_buf) {
                Ok(r) => r,
                Err(pe) => {
                    log::warn!("Parse error reading file: {pe:?}");
                    return Ok(None)
                },
            };
            
        };

        todo!()
    }
}

pub trait CompressedStorage {
    async fn compress(self);
}
