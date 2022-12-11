use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use super::{
    entry::{CachedResource, Entry, State},
    size_counter::CacheSizeCounter,
    types::{CacheContext, CacheEntry, CacheKey, StreamError},
};

pub struct Cache<Key, Value>
where
    Key: CacheKey,
    Value: CacheEntry,
{
    entries: RwLock<HashMap<Key, Arc<RwLock<Entry<Value>>>>>,
    size_counter: CacheSizeCounter<Key>,
}

impl<Key, Value> Cache<Key, Value>
where
    Key: CacheKey,
    Value: CacheEntry,
{
    pub fn new() -> Arc<Cache<Key, Value>> {
        Arc::new(Cache {
            entries: RwLock::new(HashMap::new()),
            size_counter: CacheSizeCounter::new(100 * 1024 * 1024),
        })
    }

    pub async fn get_resource<Context: CacheContext<Key, Value>>(
        self: &Arc<Self>,
        key: &Key,
        context: &Arc<Context>,
    ) -> Result<CachedResource<Value>, StreamError> {
        let entry = self.get_entry(key).await;
        let maybe_stream = async {
            let read_guard = entry.read().await;
            read_guard.maybe_get_stream().await
        }
        .await;
        match maybe_stream {
            Some(value) => value,
            None => {
                let mut write_guard = entry.write().await;
                write_guard.get_stream(self, &entry, key, context).await
            }
        }
    }

    pub async fn on_new_entry_added(self: &Arc<Self>, key: &Key, size: u64) {
        let eviciton_queue = self.size_counter.on_new_entry_added(key, size).await;
        for key in eviciton_queue {
            let entry = self.get_entry(&key).await;
            let mut write_guard = entry.write().await;
            write_guard.drop_from_memory().await;
        }
    }

    async fn get_entry(self: &Arc<Self>, key: &Key) -> Arc<RwLock<Entry<Value>>> {
        let maybe_entry = self.get_entry_with_readlock(key).await;
        match maybe_entry {
            Some(value) => value,
            None => self.get_entry_with_writelock(key).await,
        }
    }

    async fn get_entry_with_writelock(self: &Arc<Self>, key: &Key) -> Arc<RwLock<Entry<Value>>> {
        let mut write_guard = self.entries.write().await;
        let entry = write_guard.get(key);
        match entry {
            Some(value) => value.clone(),
            None => {
                let new_entry = Arc::new(RwLock::new(Entry {
                    state: State::Unknown,
                }));
                write_guard.insert(key.clone(), new_entry.clone());
                new_entry
            }
        }
    }

    async fn get_entry_with_readlock(
        self: &Arc<Self>,
        key: &Key,
    ) -> Option<Arc<RwLock<Entry<Value>>>> {
        let read_guard = self.entries.read().await;
        let entry = read_guard.get(&key);
        match entry {
            None => None,
            Some(value) => Some(value.clone()),
        }
    }
}
