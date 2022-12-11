use core::fmt;
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt::Debug,
    hash::Hash,
    path::PathBuf,
    sync::{Arc, Mutex as StdMutex},
};

use bytes::Bytes;
use futures_util::Future;
use tokio::io;

use super::entry::ByteReadBuffer;
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub enum StreamError {
    PackageDoesNotExists,
    FileDoesNotExists,
    UnknownIOError(io::ErrorKind),
    UnknownNetworError(Arc<reqwest::Error>),
}

impl fmt::Display for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            StreamError::PackageDoesNotExists => write!(f, "Package not found"),
            StreamError::FileDoesNotExists => write!(f, "File does not exists"),
            StreamError::UnknownIOError(err) => std::fmt::Display::fmt(&err, f),
            StreamError::UnknownNetworError(err) => std::fmt::Display::fmt(&err, f),
        }
    }
}

impl Error for StreamError {}

pub enum ReadFromUpstreamResult<Value> {
    Reading(Arc<StdMutex<ByteReadBuffer>>, Value),
    Err(StreamError),
}

#[async_trait]
pub trait CacheContext<Key: CacheKey, Value: CacheEntry>: Send + Sync + 'static {
    fn get_root_dir(&self) -> PathBuf;
    async fn from_upstream<FutureImpl>(
        self: &Arc<Self>,
        key: &Key,
        read_finished_future: FutureImpl,
    ) -> ReadFromUpstreamResult<Value>
    where
        FutureImpl: Future<Output = ()> + Sync + Send + 'static;
}

pub trait CacheKey: Hash + Eq + Send + Sync + Clone + 'static {
    fn get_relative_path(&self) -> PathBuf;
    fn get_url(&self) -> String;
}

pub trait CacheEntry: Send + Sync + 'static + Clone {
    fn from_entries(map: HashMap<&'static str, Bytes>) -> Option<Self>;
    fn into_hash_map(&self) -> HashMap<String, Bytes>;
    fn entries() -> HashSet<&'static str>;
}
