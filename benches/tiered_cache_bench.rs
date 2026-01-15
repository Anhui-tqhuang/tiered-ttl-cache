use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use std::hint::black_box;
use tiered_ttl_cache::{TieredCache, TieredCacheConfig};
use bytes::Bytes;
use std:: path::PathBuf;
use std::time::Duration;
use tokio::runtime::Runtime;

// Helper function to create a runtime for async operations
fn create_runtime() -> Runtime {
    Runtime::new().unwrap()
}

// Helper function to create cache with default config
fn create_test_cache() -> TieredCache {
    let cfg = TieredCacheConfig {
        base_dir: PathBuf::from("./bench_cache"),
        max_cost_mem: Some(100 * 1024 * 1024), // 100 MB
        max_cost_mmap: Some(500 * 1024 * 1024), // 500 MB
        max_cost_disk: Some(2 * 1024 * 1024 * 1024), // 2 GB
        memory_map_threshold: Some(5 * 1024 * 1024), // 5 MB
        disk_threshold: Some(20 * 1024 * 1024), // 20 MB
    };
    TieredCache::new(cfg)
}

// Benchmark:  Set operations for different data sizes
fn bench_set_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_operations");
    
    // Test different data sizes
    let sizes = vec![
        ("1KB", 1 * 1024),
        ("10KB", 10 * 1024),
        ("100KB", 100 * 1024),
        ("1MB", 1 * 1024 * 1024),
        ("5MB", 5 * 1024 * 1024),
        ("10MB", 10 * 1024 * 1024),
        ("50MB", 50 * 1024 * 1024),
    ];

    for (name, size) in sizes {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), &size, |b, &size| {
            let rt = create_runtime();
            let cache = create_test_cache();
            let data = Bytes::from(vec![0u8; size]);
            
            b.iter(|| {
                rt.block_on(async {
                    cache.set(
                        format!("key_{}", size),
                        black_box(data.clone()),
                        Duration::from_secs(300)
                    ).await.unwrap();
                });
            });
        });
    }
    
    group.finish();
}

// Benchmark: Get operations for different data sizes
fn bench_get_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_operations");
    
    let sizes = vec![
        ("1KB", 1 * 1024),
        ("10KB", 10 * 1024),
        ("100KB", 100 * 1024),
        ("1MB", 1 * 1024 * 1024),
        ("5MB", 5 * 1024 * 1024),
        ("10MB", 10 * 1024 * 1024),
        ("50MB", 50 * 1024 * 1024),
    ];

    for (name, size) in sizes {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), &size, |b, &size| {
            let rt = create_runtime();
            let cache = create_test_cache();
            let data = Bytes::from(vec![0u8; size]);
            let key = format!("key_{}", size);
            
            // Pre-populate cache
            rt.block_on(async {
                cache.set(key.clone(), data, Duration::from_secs(300)).await.unwrap();
            });
            
            b.iter(|| {
                rt.block_on(async {
                    let result = cache.get(black_box(&key)).await;
                    black_box(result);
                });
            });
        });
    }
    
    group.finish();
}

// Benchmark: Memory tier operations
fn bench_memory_tier(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_tier");
    
    let rt = create_runtime();
    let cache = create_test_cache();
    
    // Small data that fits in memory tier (< 5MB)
    let data = Bytes::from(vec![0u8; 1 * 1024 * 1024]); // 1 MB
    
    group.throughput(Throughput::Bytes(data.len() as u64));
    
    group.bench_function("set", |b| {
        b.iter(|| {
            rt.block_on(async {
                cache.set(
                    "memory_key".to_string(),
                    black_box(data.clone()),
                    Duration::from_secs(300)
                ).await.unwrap();
            });
        });
    });
    
    // Pre-populate for get benchmark
    rt.block_on(async {
        cache.set("memory_key".to_string(), data.clone(), Duration::from_secs(300)).await.unwrap();
    });
    
    group.bench_function("get", |b| {
        b.iter(|| {
            rt.block_on(async {
                let result = cache.get(black_box("memory_key")).await;
                black_box(result);
            });
        });
    });
    
    group.finish();
}

// Benchmark: Memory-map tier operations
fn bench_mmap_tier(c: &mut Criterion) {
    let mut group = c.benchmark_group("mmap_tier");
    
    let rt = create_runtime();
    let cache = create_test_cache();
    
    // Medium data that goes to mmap tier (5-20MB)
    let data = Bytes::from(vec![0u8; 10 * 1024 * 1024]); // 10 MB
    
    group.throughput(Throughput::Bytes(data.len() as u64));
    
    group.bench_function("set", |b| {
        b.iter(|| {
            rt.block_on(async {
                cache.set(
                    format!("mmap_key_{}", rand::random::<u32>()),
                    black_box(data.clone()),
                    Duration::from_secs(300)
                ).await.unwrap();
            });
        });
    });
    
    // Pre-populate for get benchmark
    rt.block_on(async {
        cache.set("mmap_key". to_string(), data.clone(), Duration::from_secs(300)).await.unwrap();
    });
    
    group.bench_function("get", |b| {
        b.iter(|| {
            rt.block_on(async {
                let result = cache.get(black_box("mmap_key")).await;
                black_box(result);
            });
        });
    });
    
    group.finish();
}

// Benchmark:  Disk tier operations
fn bench_disk_tier(c: &mut Criterion) {
    let mut group = c.benchmark_group("disk_tier");
    group.sample_size(20); // Reduce sample size for slow disk operations
    
    let rt = create_runtime();
    let cache = create_test_cache();
    
    // Large data that goes to disk tier (> 20MB)
    let data = Bytes::from(vec![0u8; 50 * 1024 * 1024]); // 50 MB
    
    group.throughput(Throughput::Bytes(data.len() as u64));
    
    group.bench_function("set", |b| {
        b.iter(|| {
            rt.block_on(async {
                cache.set(
                    format!("disk_key_{}", rand::random::<u32>()),
                    black_box(data.clone()),
                    Duration:: from_secs(300)
                ).await.unwrap();
            });
        });
    });
    
    // Pre-populate for get benchmark
    rt.block_on(async {
        cache.set("disk_key".to_string(), data.clone(), Duration::from_secs(300)).await.unwrap();
    });
    
    group.bench_function("get", |b| {
        b.iter(|| {
            rt.block_on(async {
                let result = cache.get(black_box("disk_key")).await;
                black_box(result);
            });
        });
    });
    
    group.finish();
}

// Benchmark: Concurrent operations
fn bench_concurrent_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_operations");
    
    let rt = create_runtime();
    let cache = std::sync::Arc::new(create_test_cache());
    
    group.bench_function("concurrent_sets_10", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = vec![];
                for i in 0..10 {
                    let cache = cache.clone();
                    let data = Bytes::from(vec![0u8; 1024 * 1024]); // 1 MB
                    let handle = tokio::spawn(async move {
                        cache.set(
                            format!("concurrent_key_{}", i),
                            data,
                            Duration::from_secs(300)
                        ).await.unwrap();
                    });
                    handles.push(handle);
                }
                
                for handle in handles {
                    handle.await.unwrap();
                }
            });
        });
    });
    
    // Pre-populate for concurrent gets
    rt.block_on(async {
        for i in 0..10 {
            let data = Bytes::from(vec![0u8; 1024 * 1024]);
            cache.set(
                format!("concurrent_key_{}", i),
                data,
                Duration::from_secs(300)
            ).await.unwrap();
        }
    });
    
    group.bench_function("concurrent_gets_10", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = vec![];
                for i in 0..10 {
                    let cache = cache. clone();
                    let handle = tokio::spawn(async move {
                        let result = cache.get(&format!("concurrent_key_{}", i)).await;
                        black_box(result);
                    });
                    handles.push(handle);
                }
                
                for handle in handles {
                    handle.await.unwrap();
                }
            });
        });
    });
    
    group.finish();
}

// Benchmark: Read bytes performance
fn bench_read_bytes(c: &mut Criterion) {
    let mut group = c. benchmark_group("read_bytes");
    
    let rt = create_runtime();
    let cache = create_test_cache();
    
    let sizes = vec![
        ("memory_1MB", 1 * 1024 * 1024),
        ("mmap_10MB", 10 * 1024 * 1024),
        ("disk_50MB", 50 * 1024 * 1024),
    ];
    
    for (name, size) in sizes {
        let data = Bytes::from(vec![0u8; size]);
        let key = format!("read_bytes_{}", name);
        
        rt.block_on(async {
            cache.set(key.clone(), data, Duration::from_secs(300)).await.unwrap();
        });
        
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), &key, |b, key| {
            b.iter(|| {
                rt.block_on(async {
                    let entry = cache.get(black_box(key)).await.unwrap();
                    let bytes = entry.read_bytes().await.unwrap();
                    black_box(bytes);
                });
            });
        });
    }
    
    group.finish();
}

// Benchmark: Cache miss performance
fn bench_cache_miss(c: &mut Criterion) {
    let mut group = c. benchmark_group("cache_miss");
    
    let rt = create_runtime();
    let cache = create_test_cache();
    
    group.bench_function("get_nonexistent", |b| {
        b.iter(|| {
            rt.block_on(async {
                let result = cache. get(black_box("nonexistent_key")).await;
                black_box(result);
            });
        });
    });
    
    group.finish();
}

// Benchmark: Mixed workload (read/write ratio)
fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    
    let rt = create_runtime();
    let cache = create_test_cache();
    
    // Pre-populate with some data
    rt.block_on(async {
        for i in 0..100 {
            let data = Bytes::from(vec![0u8; 1024 * 1024]); // 1 MB
            cache.set(format!("mixed_key_{}", i), data, Duration::from_secs(300)).await.unwrap();
        }
    });
    
    group.bench_function("read_heavy_90_10", |b| {
        let mut counter = 0;
        b. iter(|| {
            rt. block_on(async {
                // 90% reads, 10% writes
                if counter % 10 < 9 {
                    let key = format!("mixed_key_{}", counter % 100);
                    let result = cache.get(black_box(&key)).await;
                    black_box(result);
                } else {
                    let data = Bytes::from(vec![0u8; 1024 * 1024]);
                    cache.set(
                        format!("mixed_key_{}", counter % 100),
                        data,
                        Duration::from_secs(300)
                    ).await.unwrap();
                }
                counter += 1;
            });
        });
    });
    
    group.bench_function("write_heavy_30_70", |b| {
        let mut counter = 0;
        b.iter(|| {
            rt.block_on(async {
                // 30% reads, 70% writes
                if counter % 10 < 3 {
                    let key = format!("mixed_key_{}", counter % 100);
                    let result = cache.get(black_box(&key)).await;
                    black_box(result);
                } else {
                    let data = Bytes::from(vec![0u8; 1024 * 1024]);
                    cache.set(
                        format!("mixed_key_{}", counter % 100),
                        data,
                        Duration::from_secs(300)
                    ).await.unwrap();
                }
                counter += 1;
            });
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_set_operations,
    bench_get_operations,
    bench_memory_tier,
    bench_mmap_tier,
    bench_disk_tier,
    bench_concurrent_operations,
    bench_read_bytes,
    bench_cache_miss,
    bench_mixed_workload
);
criterion_main!(benches);
