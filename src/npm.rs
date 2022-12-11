use bytes::Bytes;
use futures_util::{Future, StreamExt};
use http::{StatusCode};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use crate::cache::{
    cache::Cache,
    entry::{ByteReadBuffer, CachedResource},
    types::{CacheContext, CacheEntry, CacheKey, ReadFromUpstreamResult, StreamError},
};
use async_trait::async_trait;
use tokio::task;

#[derive(Hash, PartialEq, Eq, Clone, Debug)]
pub struct MetadataKey {
    pub package_name: String,
    pub package_scope: Option<String>,
}

impl CacheKey for MetadataKey {
    fn get_relative_path(&self) -> PathBuf {
        let mut path = PathBuf::new();
        if let Some(scope) = &self.package_scope {
            path.push(format!("@{}", scope));
        }
        path.push(&self.package_name);
        path
    }

    fn get_url(&self) -> String {
        match &self.package_scope {
            Some(scope) => format!(
                "https://registry.npmjs.com/@{}/{}",
                scope, &self.package_name
            ),
            None => format!("https://registry.npmjs.com/{}", &self.package_name),
        }
    }
}

#[derive(Hash, PartialEq, Eq, Clone, Debug)]
pub struct PackageKey {
    pub package_name: String,
    pub package_scope: Option<String>,
    pub tgz_filename: String,
}

impl CacheKey for PackageKey {
    fn get_relative_path(&self) -> PathBuf {
        let mut path = PathBuf::new();
        if let Some(scope) = &self.package_scope {
            path.push(format!("@{}", scope));
        }
        path.push(&self.package_name);
        path.push(&self.tgz_filename);
        path
    }

    fn get_url(&self) -> String {
        match &self.package_scope {
            Some(scope) => format!(
                "https://registry.npmjs.com/@{}/{}/-/{}",
                scope, &self.package_name, &self.tgz_filename
            ),
            None => format!(
                "https://registry.npmjs.com/{}/-/{}",
                &self.package_name, &self.tgz_filename
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MetadataInfo {
}

impl From<reqwest::header::HeaderMap> for MetadataInfo {
    fn from(_headers: reqwest::header::HeaderMap) -> Self {
        MetadataInfo {
        }
    }
}

impl CacheEntry for MetadataInfo {
    // Headers do not need to be cached for npm to work, I'm just caching them
    // to test proof of concept for additional values other than response body.
    fn from_entries(_map: HashMap<&'static str, bytes::Bytes>) -> Option<Self> {
        Some(MetadataInfo {
        })
    }

    fn entries() -> HashSet<&'static str> {
        [].into()
    }

    fn into_hash_map(&self) -> HashMap<String, Bytes> {
        HashMap::new()
    }
}

#[derive(Clone, Debug)]
pub struct PackageInfo {
}

impl From<reqwest::header::HeaderMap> for PackageInfo {
    fn from(_headers: reqwest::header::HeaderMap) -> Self {
        PackageInfo {
        }
    }
}

impl CacheEntry for PackageInfo {
    fn from_entries(_map: HashMap<&'static str, Bytes>) -> Option<Self> {
        Some(PackageInfo {
        })
    }

    fn entries() -> HashSet<&'static str> {
        [].into()
    }

    fn into_hash_map(&self) -> HashMap<String, Bytes> {
        HashMap::new()
    }
}

pub struct NpmCache {
    root_dir: PathBuf,
    api: Arc<reqwest::Client>,
    metadata: Arc<Cache<MetadataKey, MetadataInfo>>,
    package: Arc<Cache<PackageKey, PackageInfo>>,
}

fn create_http_client() -> Arc<reqwest::Client> {
    let client_builder = reqwest::Client::builder();
    let client_builder = client_builder.timeout(Duration::from_secs(10));
    Arc::new(client_builder.build().expect("Failed to create client"))
}

impl NpmCache {
    pub fn new(root_dir: PathBuf) -> Arc<Self> {
        let http_client = create_http_client();
        Arc::new(NpmCache {
            root_dir,
            api: http_client,
            metadata: Cache::new(),
            package: Cache::new(),
        })
    }

    pub async fn get_package(
        self: &Arc<Self>,
        key: &PackageKey,
    ) -> Result<CachedResource<PackageInfo>, StreamError> {
        self.package.get_resource(key, self).await
    }

    pub async fn get_metadata(
        self: &Arc<Self>,
        key: &MetadataKey,
    ) -> Result<CachedResource<MetadataInfo>, StreamError> {
        self.metadata.get_resource(key, self).await
    }

    async fn from_upstream_impl<Key, Value, FutureImpl>(
        self: &Arc<Self>,
        key: &Key,
        read_finished_future: FutureImpl,
    ) -> ReadFromUpstreamResult<Value>
    where
        Key: CacheKey,
        Value: CacheEntry + From<reqwest::header::HeaderMap>,
        FutureImpl: Future<Output = ()> + Sync + Send + 'static,
    {
        let response_result = self.api.get(key.get_url()).send().await;
        let response = match response_result {
            Ok(request) => request,
            Err(err) => {
                return match err.status() {
                    Some(StatusCode::NOT_FOUND) => {
                        ReadFromUpstreamResult::Err(StreamError::PackageDoesNotExists)
                    }
                    _ => {
                        ReadFromUpstreamResult::Err(StreamError::UnknownNetworError(Arc::new(err)))
                    }
                }
            }
        };
        let value = response.headers().clone().into();
        let mut stream = response.bytes_stream();
        let reader = ByteReadBuffer::new();
        let reader_clone = reader.clone();
        task::spawn(async move {
            while let Some(value) = stream.next().await {
                match value {
                    Err(err) => {
                        ByteReadBuffer::register_error(
                            &reader,
                            StreamError::UnknownNetworError(Arc::new(err)),
                        );
                        read_finished_future.await;
                        return;
                    }
                    Ok(value) => {
                        ByteReadBuffer::push(&reader, value);
                    }
                };
            }
            ByteReadBuffer::flush(&reader);
            read_finished_future.await
        });
        ReadFromUpstreamResult::Reading(reader_clone, value)
    }
}

#[async_trait]
impl CacheContext<PackageKey, PackageInfo> for NpmCache {
    fn get_root_dir(&self) -> PathBuf {
        self.root_dir.join("workingdir/packages")
    }

    async fn from_upstream<FutureImpl>(
        self: &Arc<Self>,
        key: &PackageKey,
        read_finished_future: FutureImpl,
    ) -> ReadFromUpstreamResult<PackageInfo>
    where
        FutureImpl: Future<Output = ()> + Sync + Send + 'static,
    {
        Self::from_upstream_impl::<PackageKey, PackageInfo, _>(self, key, read_finished_future)
            .await
    }
}

#[async_trait]
impl CacheContext<MetadataKey, MetadataInfo> for NpmCache {
    fn get_root_dir(&self) -> PathBuf {
        self.root_dir.join("workingdir/metadata")
    }
    async fn from_upstream<FutureImpl>(
        self: &Arc<Self>,
        key: &MetadataKey,
        read_finished_future: FutureImpl,
    ) -> ReadFromUpstreamResult<MetadataInfo>
    where
        FutureImpl: Future<Output = ()> + Sync + Send + 'static,
    {
        Self::from_upstream_impl::<_, MetadataInfo, _>(self, key, read_finished_future).await
    }
}
