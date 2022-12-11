use std::{
    sync::{Arc, MutexGuard as StdMutexGuard},
    task::{Poll, Waker},
};

use super::{
    cache::Cache,
    disk_read::{read_from_disk, ReadFromDiskResult},
    types::{CacheContext, CacheEntry, CacheKey, ReadFromUpstreamResult, StreamError},
    write_disk::ensure_file_on_disk,
};
use bytes::Bytes;
use futures_util::Stream;
use std::sync::Mutex as StdMutex;
use tokio::sync::RwLock;

pub trait SerializableCacheEntry {
    fn into_serialized(&self) -> Bytes;
    fn size(&self) -> Bytes;
}

pub enum State<Value> {
    Unknown,
    InTransitViaNetwork(Arc<StdMutex<ByteReadBuffer>>, Value),
    InTransitFromDisk(Arc<StdMutex<ByteReadBuffer>>, Value),
    InMemory(Bytes, Value),
    InMemoryAndPopulatedToDisk(Bytes, Value),
}

pub struct CachedResource<Value> {
    pub stream: ContentStream,
    pub value: Value,
}

pub struct Entry<Value: CacheEntry> {
    pub state: State<Value>,
}

impl<Value> Entry<Value>
where
    Value: CacheEntry,
{
    pub async fn drop_from_memory(self: &mut Self) {
        match &self.state {
            State::InMemory(_bytes, _value) => self.state = State::Unknown,
            State::InMemoryAndPopulatedToDisk(_bytes, _value) => {
                self.state = State::Unknown
            }
            _ => {}
        }
    }

    pub async fn maybe_get_stream(
        self: &Self,
    ) -> Option<Result<CachedResource<Value>, StreamError>> {
        match &self.state {
            State::Unknown => None,
            State::InTransitViaNetwork(read_stream, value) => Some(Ok(CachedResource {
                stream: Self::handle_in_transit(read_stream),
                value: value.clone(),
            })),
            State::InTransitFromDisk(read_stream, value) => Some(Ok(CachedResource {
                stream: Self::handle_in_transit(read_stream),
                value: value.clone(),
            })),
            State::InMemory(bytes, value) => Some(Ok(CachedResource {
                stream: Self::handle_in_memory(bytes),
                value: value.clone(),
            })),
            State::InMemoryAndPopulatedToDisk(bytes, value) => Some(Ok(CachedResource {
                stream: Self::handle_in_memory(bytes),
                value: value.clone(),
            })),
        }
    }

    pub async fn get_stream<Key: CacheKey, Context: CacheContext<Key, Value>>(
        self: &mut Self,
        cache: &Arc<Cache<Key, Value>>,
        entry: &Arc<RwLock<Self>>,
        key: &Key,
        ctx: &Arc<Context>,
    ) -> Result<CachedResource<Value>, StreamError> {
        match &self.state {
            State::Unknown => self.handle_unknown(cache, entry, key, ctx).await,
            State::InTransitViaNetwork(read_stream, value) => Ok(CachedResource {
                stream: Self::handle_in_transit(read_stream),
                value: value.clone(),
            }),
            State::InTransitFromDisk(read_stream, value) => Ok(CachedResource {
                stream: Self::handle_in_transit(read_stream),
                value: value.clone(),
            }),
            State::InMemory(bytes, value) => Ok(CachedResource {
                stream: Self::handle_in_memory(bytes),
                value: value.clone(),
            }),
            State::InMemoryAndPopulatedToDisk(bytes, value) => Ok(CachedResource {
                stream: Self::handle_in_memory(bytes),
                value: value.clone(),
            }),
        }
    }

    async fn handle_unknown<Key: CacheKey, Context: CacheContext<Key, Value>>(
        self: &mut Self,
        cache: &Arc<Cache<Key, Value>>,
        entry: &Arc<RwLock<Self>>,
        key: &Key,
        ctx: &Arc<Context>,
    ) -> Result<CachedResource<Value>, StreamError> {
        match self.handle_read_from_disk(cache, entry, key, ctx).await {
            Err(err) => match err {
                StreamError::FileDoesNotExists => (),
                _ => {
                    return Err(err);
                }
            },
            Ok(value) => {
                return Ok(value);
            }
        };
        self.handle_read_from_upstream(cache, entry, key, ctx).await
    }

    async fn on_entry_in_memory<Key: CacheKey>(
        cache: Arc<Cache<Key, Value>>,
        entry: Arc<RwLock<Self>>,
        key: Key,
    ) {
        let mut entry_guard = entry.write().await;
        let (stream, value, did_read_from_disk) =
            if let State::InTransitViaNetwork(stream, value) = &entry_guard.state {
                (stream.clone(), value.clone(), false)
            } else if let State::InTransitFromDisk(stream, value) = &entry_guard.state {
                (stream.clone(), value.clone(), true)
            } else {
                return;
            };

        match ByteReadBuffer::get_result(&stream) {
            Err(_err) => todo!(),
            Ok(bytes) => {
                let size = bytes.len().try_into().unwrap_or(u64::MAX);
                if did_read_from_disk {
                    entry_guard.state =
                        State::InMemoryAndPopulatedToDisk(bytes.clone(), value.clone());
                } else {
                    entry_guard.state = State::InMemory(bytes.clone(), value.clone());
                }
                drop(entry_guard);
                cache.on_new_entry_added(&key, size).await;
            }
        };
    }

    async fn on_entry_ready_to_write<Key: CacheKey, Context: CacheContext<Key, Value>>(
        entry: Arc<RwLock<Self>>,
        key: Key,
        ctx: Arc<Context>,
    ) {
        let (data, value) = if let State::InMemory(data, value) = &entry.write().await.state {
            (data.clone(), value.clone())
        } else {
            return;
        };

        let new_state = State::InMemoryAndPopulatedToDisk(data.clone(), value.clone());
        Entry::write_in_memory_state_to_disk(&key, &ctx, data, value.clone()).await;
        let mut entry_guard = entry.write().await;
        entry_guard.state = new_state;
    }

    async fn write_in_memory_state_to_disk<Key: CacheKey, Context: CacheContext<Key, Value>>(
        key: &Key,
        ctx: &Arc<Context>,
        data: Bytes,
        value: Value,
    ) {
        if let Err(_err) =
            ensure_file_on_disk(&ctx.get_root_dir().join(key.get_relative_path()), &data).await
        {
            todo!()
        }
        let path = ctx.get_root_dir().join(key.get_relative_path());
        for (file_ext, content) in value.into_hash_map().iter() {
            let path_with_ext = path.with_file_name(format!(
                "{}.{}",
                path.file_name().unwrap().to_string_lossy(),
                file_ext
            ));
            if let Err(_err) = ensure_file_on_disk(path_with_ext.as_ref(), content).await {
                todo!()
            }
        }
    }

    async fn handle_read_from_upstream<Key: CacheKey, Context: CacheContext<Key, Value>>(
        self: &mut Self,
        cache: &Arc<Cache<Key, Value>>,
        entry: &Arc<RwLock<Self>>,
        key: &Key,
        ctx: &Arc<Context>,
    ) -> Result<CachedResource<Value>, StreamError> {
        let entry_clone = entry.clone();
        let key_clone = key.clone();
        let cache_clone = cache.clone();
        let ctx_clone = ctx.clone();
        let result: ReadFromUpstreamResult<Value> = ctx
            .from_upstream(key, async move {
                Self::on_entry_in_memory(cache_clone, entry_clone.clone(), key_clone.clone()).await;
                Self::on_entry_ready_to_write(entry_clone, key_clone, ctx_clone).await;
            })
            .await;
        match result {
            ReadFromUpstreamResult::Reading(stream, value) => {
                self.state = State::InTransitViaNetwork(stream.clone(), value.clone());
                Ok(CachedResource {
                    stream: ContentStream::from_byte_read_buffer(&stream),
                    value,
                })
            }
            ReadFromUpstreamResult::Err(err) => Err(err),
        }
    }

    async fn handle_read_from_disk<Key: CacheKey, Context: CacheContext<Key, Value>>(
        self: &mut Self,
        cache: &Arc<Cache<Key, Value>>,
        entry: &Arc<RwLock<Self>>,
        key: &Key,
        ctx: &Arc<Context>,
    ) -> Result<CachedResource<Value>, StreamError> {
        let entry_clone = entry.clone();
        let key_clone = key.clone();
        let cache_clone = cache.clone();
        let result: ReadFromDiskResult<Value> = read_from_disk(key, ctx, async move {
            Self::on_entry_in_memory(cache_clone, entry_clone, key_clone).await
        })
        .await;
        match result {
            ReadFromDiskResult::Reading(stream, value) => {
                self.state = State::InTransitFromDisk(stream.clone(), value.clone());
                Ok(CachedResource {
                    stream: ContentStream::from_byte_read_buffer(&stream),
                    value,
                })
            }
            ReadFromDiskResult::NotFound => Err(StreamError::FileDoesNotExists),
            ReadFromDiskResult::Err(err) => Err(err),
        }
    }

    fn handle_in_transit(buffer: &Arc<StdMutex<ByteReadBuffer>>) -> ContentStream {
        ContentStream::from_byte_read_buffer(buffer)
    }

    fn handle_in_memory(bytes: &Bytes) -> ContentStream {
        ContentStream::from_bytes(bytes.clone())
    }
}

pub struct ByteReadBuffer {
    data: Vec<Bytes>,
    listeners: Vec<Waker>,
    finished: bool,
    error: Option<StreamError>,
}

impl ByteReadBuffer {
    pub fn new() -> Arc<StdMutex<ByteReadBuffer>> {
        Arc::new(StdMutex::new(ByteReadBuffer {
            data: Vec::new(),
            listeners: Vec::new(),
            finished: false,
            error: None,
        }))
    }

    pub fn from_bytes(bytes: Bytes) -> Arc<StdMutex<ByteReadBuffer>> {
        Arc::new(StdMutex::new(ByteReadBuffer {
            data: vec![bytes],
            listeners: Vec::new(),
            finished: true,
            error: None,
        }))
    }

    fn maybe_get_data(
        buffer: &Arc<StdMutex<ByteReadBuffer>>,
        index: &mut usize,
        waker: &Waker,
    ) -> Poll<Option<Result<Bytes, StreamError>>> {
        let mut write_guard = buffer.lock().unwrap();
        if *index < write_guard.data.len() {
            *index += 1;
            Poll::Ready(Some(Ok(write_guard.data[*index - 1].clone())))
        } else if write_guard.finished {
            Poll::Ready(None)
        } else {
            write_guard.listeners.push(waker.clone());
            Poll::Pending
        }
    }

    pub fn push(buffer: &Arc<StdMutex<ByteReadBuffer>>, bytes: Bytes) {
        let mut write_guard = buffer.lock().unwrap();
        write_guard.data.push(bytes);
        ByteReadBuffer::internal_flush(&mut write_guard);
    }

    pub fn register_error(buffer: &Arc<StdMutex<ByteReadBuffer>>, error: StreamError) {
        let mut write_guard = buffer.lock().unwrap();
        write_guard.error = Some(error);
        write_guard.finished = true;
        ByteReadBuffer::internal_flush(&mut write_guard);
    }

    /// Should be called after any writes are finished
    pub fn flush(buffer: &Arc<StdMutex<ByteReadBuffer>>) {
        let mut write_guard = buffer.lock().unwrap();
        write_guard.finished = true;
        Self::internal_flush(&mut write_guard);
    }

    pub fn get_result(buffer: &Arc<StdMutex<ByteReadBuffer>>) -> Result<Bytes, StreamError> {
        let write_guard = buffer.lock().unwrap();
        if let Some(err) = &write_guard.error {
            return Err(err.clone());
        }
        Ok(Bytes::from(write_guard.data.concat()))
    }

    fn internal_flush(buffer_write_guard: &mut StdMutexGuard<ByteReadBuffer>) {
        let mut moved = Vec::new();
        std::mem::swap(&mut moved, &mut buffer_write_guard.listeners);
        for waker in moved {
            waker.wake()
        }
    }
}

pub struct ContentStream {
    buffer: Arc<StdMutex<ByteReadBuffer>>,
    read_index: usize,
}

impl ContentStream {
    fn from_bytes(bytes: Bytes) -> ContentStream {
        ContentStream {
            buffer: ByteReadBuffer::from_bytes(bytes),
            read_index: 0,
        }
    }
    fn from_byte_read_buffer(buffer: &Arc<StdMutex<ByteReadBuffer>>) -> ContentStream {
        ContentStream {
            buffer: buffer.clone(),
            read_index: 0,
        }
    }
}

impl Stream for ContentStream {
    type Item = Result<Bytes, StreamError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        ByteReadBuffer::maybe_get_data(&self.buffer.clone(), &mut self.read_index, cx.waker())
    }
}
