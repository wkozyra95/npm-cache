use moka::future::Cache as MokaCache;
use std::{hash::Hash, sync::Arc};

#[derive(Clone)]
struct EvictionQueue<Key>
where
    Key: Hash + Eq + Send + Sync + Clone,
{
    queue: Arc<std::sync::Mutex<Vec<Key>>>,
}

impl<Key> EvictionQueue<Key>
where
    Key: Hash + Eq + Send + Sync + Clone,
{
    fn flush(&self) -> Vec<Key> {
        let mut guard = self.queue.lock().unwrap();
        let copy = guard.clone();
        guard.clear();
        copy
    }

    fn add(&self, key: Key) {
        let mut guard = self.queue.lock().unwrap();
        guard.push(key);
    }
}

pub struct CacheSizeCounter<Key>
where
    Key: Hash + Eq + Send + Sync + Clone + 'static,
{
    cache: MokaCache<Key, u64>,
    eviction_queue: EvictionQueue<Key>,
}

impl<Key> CacheSizeCounter<Key>
where
    Key: Hash + Eq + Send + Sync + Clone + 'static,
{
    pub fn new(max_size: u64) -> CacheSizeCounter<Key> {
        let eviction_queue: EvictionQueue<Key> = EvictionQueue {
            queue: Arc::new(std::sync::Mutex::new(Vec::new())),
        };
        let eviction_queue_clone = eviction_queue.clone();
        let cache = MokaCache::builder()
            .weigher(|_key: &Key, value: &u64| value.clone().try_into().unwrap_or(u32::MAX))
            .eviction_listener_with_queued_delivery_mode(move |key, _value, _cause| {
                let key_clone = (*key).clone();
                eviction_queue_clone.add(key_clone)
            })
            .max_capacity(max_size)
            .build();
        CacheSizeCounter {
            cache,
            eviction_queue,
        }
    }
    pub async fn on_new_entry_added(&self, key: &Key, size: u64) -> Vec<Key> {
        self.cache.insert(key.clone(), size).await;
        return self.eviction_queue.flush();
    }
}
