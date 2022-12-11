use std::{collections::HashMap, io::ErrorKind, path::PathBuf, sync::Arc, sync::Mutex as StdMutex};

use bytes::Bytes;
use futures_util::Future;
use tokio::{
    fs::File,
    io::{self, AsyncReadExt},
    task,
};

use super::{
    entry::ByteReadBuffer,
    types::{CacheContext, CacheEntry, CacheKey, StreamError},
};

pub enum ReadFromDiskResult<Value> {
    NotFound,
    Reading(Arc<StdMutex<ByteReadBuffer>>, Value),
    Err(StreamError),
}

pub async fn read_from_disk<Key, Value, Context, FutureImpl>(
    key: &Key,
    ctx: &Arc<Context>,
    read_finished_future: FutureImpl,
) -> ReadFromDiskResult<Value>
where
    FutureImpl: Future<Output = ()> + Sync + Send + 'static,
    Key: CacheKey,
    Value: CacheEntry,
    Context: CacheContext<Key, Value>,
{
    let destination_path = ctx.get_root_dir().join(key.get_relative_path());
    let file_open_result = File::open(&destination_path).await;
    let file = match file_open_result {
        Err(err) => {
            return match err.kind() {
                ErrorKind::NotFound => ReadFromDiskResult::NotFound,
                kind => ReadFromDiskResult::Err(StreamError::UnknownIOError(kind)),
            }
        }
        Ok(file) => file,
    };
    let value = match read_value_disk(&destination_path).await {
        Err(err) => return ReadFromDiskResult::Err(StreamError::UnknownIOError(err.kind())),
        Ok(value) => value,
    };
    read_stream_file_async(file, read_finished_future, value).await
}

pub async fn read_value_disk<Value>(path: &PathBuf) -> Result<Value, io::Error>
where
    Value: CacheEntry,
{
    let file_extensions = Value::entries();
    let mut results = HashMap::new();
    for file_ext in file_extensions {
        let path_with_ext = path.with_file_name(format!(
            "{}.{}",
            path.file_name().unwrap().to_string_lossy(),
            file_ext
        ));
        let mut file = File::open(path_with_ext).await?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        results.insert(file_ext, Bytes::from(bytes.into_boxed_slice()));
    }
    Ok(Value::from_entries(results).unwrap())
}

async fn read_stream_file_async<Value, FutureImpl>(
    mut file: File,
    read_finished_future: FutureImpl,
    value: Value,
) -> ReadFromDiskResult<Value>
where
    Value: CacheEntry,
    FutureImpl: Future<Output = ()> + Send + Sync + 'static,
{
    let reader = ByteReadBuffer::new();
    let reader_clone = reader.clone();
    task::spawn(async move {
        let mut slice: Box<[u8]> = Box::new([0; 16384]);
        loop {
            let read_result = file.read(&mut slice).await;
            if let Err(err) = read_result {
                ByteReadBuffer::register_error(&reader, StreamError::UnknownIOError(err.kind()));
                read_finished_future.await;
                return;
            }
            let read_size = read_result.unwrap();
            if read_size > 0 {
                ByteReadBuffer::push(&reader, Bytes::copy_from_slice(&slice[0..read_size]));
            } else {
                break;
            }
        }
        ByteReadBuffer::flush(&reader);
        read_finished_future.await;
    });
    ReadFromDiskResult::Reading(reader_clone, value)
}
