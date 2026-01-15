# Tiered TTL Cache

A flexible, multi-tiered caching library with Time-To-Live (TTL) support for efficient data management across different tiers (powered by [moka](https://github.com/moka-rs/moka/)).

## Features

- **Multi-tier caching**: Support for multiple cache levels (memory, memory-map, disk)
- **TTL support**: Automatic expiration of cached items
- **Thread-safe**: Concurrent access support
- **Automatic Eviction**: Cost-based eviction and LRU policies.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
tiered-ttl-cache = "0.1.0"
```

## Usage

```rust
use tiered_ttl_cache::{TieredCache, TieredCacheConfig};
use tokio::runtime::Runtime;
use std::time::Duration;

fn main() {
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
```

## License

MIT
