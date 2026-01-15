use std::fs;
use std::io::{Write};
use std::path::{PathBuf};
use std::sync::Arc;

use anyhow::{Result};
use bytes::Bytes;
use memmap2::Mmap;
use moka::future::Cache;
use uuid::Uuid;
use moka::policy::EvictionPolicy;
use moka::Expiry;
use std::time::{Instant, Duration};

/// Enum representing the storage backend type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CacheType {
    Memory,
    MemoryMap,
    Disk,
}

// --- Backend Specific Structures ---

#[derive(Debug)]
pub struct MemoryEntry {
    data: Bytes,
    size: u64,
    ttl: Duration,
}

#[derive(Debug)]
pub struct MmapEntry {
    path: PathBuf,
    map: Mmap,
    size: u64,
    ttl: Duration,
}

// Use unsafe to allow Sync/Send for Mmap if you are sure about access patterns.
// memmap2::Mmap is generally Send but Sync depends on usage. 
// Since we only read after creation, this is safe.
unsafe impl Sync for MmapEntry {}
unsafe impl Send for MmapEntry {}

#[derive(Debug)]
pub struct DiskEntry {
    path: PathBuf,
    size: u64,
    ttl: Duration,
}

// --- Entry Definitions ---

/// A unified Entry enum that wraps the different storage backends.
/// Moka requires values to be `Clone`, `Send`, and `Sync`.
/// We use `Arc` for the heavier file-backed entries to allow cheap cloning.
#[derive(Clone, Debug)]
pub enum Entry {
    Memory(Arc<MemoryEntry>),
    // We wrap Mmap and Disk paths in Arc so the Cache can clone the entry cheapy.
    // The clean-up logic happens in the `eviction_listener`.
    MemoryMap(Arc<MmapEntry>),
    Disk(Arc<DiskEntry>),
}

impl Entry {
    /// Returns the size in bytes (cost) of the entry.
    pub fn size(&self) -> u64 {
        match self {
            Entry::Memory(e) => e.size,
            Entry::MemoryMap(e) => e.size,
            Entry::Disk(e) => e.size,
        }
    }

    pub fn cache_type(&self) -> CacheType {
        match self {
            Entry::Memory(_) => CacheType::Memory,
            Entry::MemoryMap(_) => CacheType::MemoryMap,
            Entry::Disk(_) => CacheType::Disk,
        }
    }

    /// Helper to get the data as a byte slice (if in memory/mmap) or read from disk.
    /// For Disk, this is a simplified sync read for demonstration.
    pub async fn read_bytes(&self) -> Result<Bytes> {
        match self {
            Entry::Memory(e) => Ok(e.data.clone()),
            Entry::MemoryMap(m) => {
                // Mmap is essentially a byte slice
                Ok(Bytes::copy_from_slice(&m.map[..]))
            }
            Entry::Disk(d) => {
                // In a real async app, use tokio::fs::read
                let data = tokio::fs::read(&d.path).await?;
                Ok(Bytes::from(data))
            }
        }
    }
}

#[derive(Clone)]
pub struct EntryExpiry;

impl Expiry<String, Entry> for EntryExpiry
{
    #[allow(unused_variables)]
    fn expire_after_create(&self, key: &String, value: &Entry, created_at: Instant) -> Option<Duration> {
        match value {
            Entry::Memory(e) => Some(e.ttl),
            Entry::MemoryMap(e) => Some(e.ttl),
            Entry::Disk(e) => Some(e.ttl),
        }
    }
}

pub struct TieredCacheConfig {
    pub base_dir: PathBuf,
    pub max_cost_mem: Option<u64>,
    pub max_cost_mmap: Option<u64>,
    pub max_cost_disk: Option<u64>,
    pub memory_map_threshold: Option<u64>,
    pub disk_threshold: Option<u64>,
}

// --- constants mirroring your Go config ---
const DEFAULT_MEMORY_THRESHOLD: u64 = 10 * 1024 * 1024; // 10 MB
const DEFAULT_DISK_THRESHOLD: u64 = 100 * 1024 * 1024; // 100 MB
const DEFAULT_MAX_COST_MEM: u64 = 1 << 30; // 1 GB
const DEFAULT_MAX_COST_MMAP: u64 = 2 << 30; // 2 GB
const DEFAULT_MAX_COST_DISK: u64 = 100 << 30; // 100 GB

impl TieredCacheConfig {
    // use default values if not provided
    pub fn with_defaults(self) -> Self {
        Self {
            base_dir: self.base_dir,
            max_cost_mem: Some(self.max_cost_mem.unwrap_or(DEFAULT_MAX_COST_MEM)),
            max_cost_mmap: Some(self.max_cost_mmap.unwrap_or(DEFAULT_MAX_COST_MMAP)),
            max_cost_disk: Some(self.max_cost_disk.unwrap_or(DEFAULT_MAX_COST_DISK)),
            memory_map_threshold: Some(self
                .memory_map_threshold
                .unwrap_or(DEFAULT_MEMORY_THRESHOLD)),
            disk_threshold: Some(self
                .disk_threshold
                .unwrap_or(DEFAULT_DISK_THRESHOLD)),
        }
    }
}

// --- The Tiered Cache ---
pub struct TieredCache {
    // We separate caches to enforce strict "tiering" by size, similar to your Go logic.
    memory_store: Cache<String, Entry>,
    mmap_store: Cache<String, Entry>,
    disk_store: Cache<String, Entry>,
    cfg: TieredCacheConfig,
}

impl TieredCache {
    pub fn new(cfg: TieredCacheConfig) -> Self {
        if !cfg.base_dir.exists() {
            fs::create_dir_all(&cfg.base_dir).expect("Failed to create cache dir");
        }

        let cfg = cfg.with_defaults();
        let entry_expiry = EntryExpiry;

        // 1. Memory Cache
        let memory_store = Cache::builder()
            .max_capacity(cfg.max_cost_mem.unwrap_or(DEFAULT_MAX_COST_MEM)) // Interpreted as "weight" (bytes)
            .weigher(|_k, v: &Entry| v.size().try_into().unwrap_or(u32::MAX))
            .eviction_policy(EvictionPolicy::lru())
            .expire_after(entry_expiry.clone())
            .build();

        // 2. Mmap Cache (with eviction listener for cleanup)
        let mmap_store = Cache::builder()
            .max_capacity(cfg.max_cost_mmap.unwrap_or(DEFAULT_MAX_COST_MMAP)) // Shared budget or separate?
            .weigher(|_k, v: &Entry| v.size().try_into().unwrap_or(u32::MAX))
            .eviction_listener(|_k, v: Entry, _cause| {
                if let Entry::MemoryMap(m) = v {
                    let _ = fs::remove_file(&m.path); // Sync delete in background thread
                }
            })
            .eviction_policy(EvictionPolicy::lru())
            .expire_after(entry_expiry.clone())
            .build();

        // 3. Disk Cache (with eviction listener for cleanup)
        let disk_store = Cache::builder()
            .max_capacity(cfg.max_cost_disk.unwrap_or(DEFAULT_MAX_COST_DISK)) // Larger disk budget
            .weigher(|_k, v: &Entry| v.size().try_into().unwrap_or(u32::MAX))
            .eviction_listener(|_k, v: Entry, _cause| {
                // CLEANUP LOGIC
                if let Entry::Disk(d) = v {
                    let _ = fs::remove_file(&d.path);
                }
            })
            .eviction_policy(EvictionPolicy::lru())
            .expire_after(entry_expiry.clone())
            .build();

        Self {
            memory_store,
            mmap_store,
            disk_store,
            cfg,
        }
    }

    /// The main SET method. Routes data to tiers based on size.
    pub async fn set(&self, key: String, data: Bytes, ttl: Duration) -> Result<()> {
        let size = data.len() as u64;

        if size <= self.cfg.memory_map_threshold.unwrap() {
            let entry = Entry::Memory(Arc::new(MemoryEntry {
                data,
                size,
                ttl,
            }));
            self.memory_store.insert(key, entry).await;
        } else if size <= self.cfg.disk_threshold.unwrap() {
            let mmap_path = self.cfg.base_dir.join(format!("mmap_{}.cache", Uuid::new_v4()));
            let mmap_entry = self.create_mmap_entry(mmap_path, &data, ttl)?;
            let entry = Entry::MemoryMap(Arc::new(mmap_entry));
            self.mmap_store.insert(key, entry).await;
        } else {
            let disk_path = self.cfg.base_dir.join(format!("disk_{}.cache", Uuid::new_v4()));
            let disk_entry = self.create_disk_entry(disk_path, &data, ttl).await?;
            let entry = Entry::Disk(Arc::new(disk_entry));
            self.disk_store.insert(key, entry).await;
        }

        Ok(())
    }

    /// The main GET method. Checks tiers in order of speed.
    pub async fn get(&self, key: &str) -> Option<Entry> {
        // 1. Check Memory
        if let Some(v) = self.memory_store.get(key).await {
            return Some(v);
        }

        // 2. Check Mmap
        if let Some(v) = self.mmap_store.get(key).await {
            return Some(v);
        }

        // 3. Check Disk
        if let Some(v) = self.disk_store.get(key).await {
            return Some(v);
        }

        None
    }

    // --- Helpers to create backend entries ---

    fn create_mmap_entry(&self, path: PathBuf, data: &[u8], ttl: Duration) -> Result<MmapEntry> {
        // 1. Write data to file synchronously (mmap creation is usually sync)
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        file.write_all(data)?;

        // 2. Create Mmap
        let mmap = unsafe { Mmap::map(&file)? };

        Ok(MmapEntry {
            path,
            map: mmap,
            size: data.len() as u64,
            ttl,
        })
    }

    async fn create_disk_entry(&self, path: PathBuf, data: &[u8], ttl: Duration) -> Result<DiskEntry> {
        // 1. Write data using async tokio
        tokio::fs::write(&path, data).await?;

        Ok(DiskEntry {
            path,
            size: data.len() as u64,
            ttl,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;
    use std::time::Duration;

    #[test]
    fn test_memory_map_file_can_be_removed_after_eviction() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let cfg = TieredCacheConfig {
                base_dir: PathBuf::from("./test_cache"),
                max_cost_mem: Some(1 * 1024 * 1024), // 1 MB
                max_cost_mmap: Some(5 * 1024 * 1024), // 5 MB
                max_cost_disk: Some(20 * 1024 * 1024), // 20 MB
                memory_map_threshold: Some(512 * 1024), // 512 KB
                disk_threshold: Some(2 * 1024 * 1024), // 2 MB
            };

            let cache = TieredCache::new(cfg);

            let data = Bytes::from(vec![0u8; 1 * 1024 * 1024]); // 1 MB

            cache.set("mmap_test".to_string(), data.clone(), Duration::from_secs(1)).await.unwrap();

            let entry = cache.get("mmap_test").await.unwrap();
            let mmap_path = match entry {
                Entry::MemoryMap(m) => m.path.clone(),
                _ => panic!("Expected MemoryMap entry"),
            };

            // Wait for expiration
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Force eviction by inserting more data
            let large_data = Bytes::from(vec![0u8; 6 * 1024 * 1024]); // 6 MB
            cache.set("large_test".to_string(), large_data, Duration::from_secs(60)).await.unwrap();
            let disk_entry = cache.get("large_test").await.unwrap();
            let disk_path = match disk_entry {
                Entry::Disk(d) => d.path.clone(),
                _ => panic!("Expected Disk entry"),
            };

            // Wait for cache maintenance to run eviction listeners
            // In tests, moka processes evictions in the background asynchronously.
            // Without run_pending_tasks(), the test may finish before the eviction
            // listener actually executes and deletes the file.
            // In production code, you would NOT call this. The cache handles it automatically.
            cache.mmap_store.run_pending_tasks().await;
            
            // Give a small delay to ensure file deletion completes
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Check if the mmap file has been removed
            assert!(!mmap_path.exists(), "Mmap file should be removed after eviction");

            // manual cleanup
            cache.disk_store.invalidate("large_test").await;
            assert!(!disk_path.exists(), "Disk file should be removed after eviction");
        });
    }

    #[test]
    fn test_tiered_cache_set_get() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let cfg = TieredCacheConfig {
                base_dir: PathBuf::from("./test_cache"),
                max_cost_mem: Some(10 * 1024 * 1024), // 10 MB
                max_cost_mmap: Some(50 * 1024 * 1024), // 50 MB
                max_cost_disk: Some(200 * 1024 * 1024), // 200 MB
                memory_map_threshold: Some(5 * 1024 * 1024), // 5 MB
                disk_threshold: Some(20 * 1024 * 1024), // 20 MB
            };

            let cache = TieredCache::new(cfg);

            let small_data = Bytes::from(vec![0u8; 1 * 1024 * 1024]); // 1 MB
            let medium_data = Bytes::from(vec![0u8; 10 * 1024 * 1024]); // 10 MB
            let large_data = Bytes::from(vec![0u8; 30 * 1024 * 1024]); // 30 MB

            cache.set("small".to_string(), small_data.clone(), Duration::from_secs(60)).await.unwrap();
            cache.set("medium".to_string(), medium_data.clone(), Duration::from_secs(60)).await.unwrap();
            cache.set("large".to_string(), large_data.clone(), Duration::from_secs(60)).await.unwrap();

            let retrieved_small = cache.get("small").await.unwrap();
            let retrieved_medium = cache.get("medium").await.unwrap();
            let retrieved_large = cache.get("large").await.unwrap();

            assert_eq!(retrieved_small.read_bytes().await.unwrap(), small_data);
            assert_eq!(retrieved_medium.read_bytes().await.unwrap(), medium_data);
            assert_eq!(retrieved_large.read_bytes().await.unwrap(), large_data);

            // manually evict all to clean up files
            cache.memory_store.invalidate("small").await;
            cache.mmap_store.invalidate("medium").await;
            cache.disk_store.invalidate("large").await;
        });
    }
}
