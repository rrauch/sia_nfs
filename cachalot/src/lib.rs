mod disk_cache;

use crate::disk_cache::{DiskCache, DiskCacheBuilder};
use anyhow::{anyhow, bail};
use blake3::Hash;
use bytes::Bytes;
use moka::future::{Cache, CacheBuilder};
use sqlx::{Pool, Sqlite};
use std::future::Future;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const VALID_CHUNK_SIZES: [u32; 5] = [4096, 8192, 16384, 32768, 65536];
const CONTENT_HASH_SEED: [u8; 32] = [
    0xf9, 0xa2, 0x9a, 0xe8, 0xe1, 0xe3, 0x26, 0x91, 0x57, 0xab, 0x79, 0x15, 0x92, 0xc9, 0x6f, 0x2e,
    0x92, 0xef, 0xfd, 0x66, 0x59, 0x85, 0xc0, 0xd3, 0x32, 0xc7, 0x13, 0x35, 0xb4, 0x71, 0x29, 0x14,
];

pub struct Cachalot {
    mem_cache: Cache<(String, String, u64, u64), Bytes>,
    disk_cache: Option<DiskCache>,
    chunk_size: usize,
}

pub struct ChunkSize(u32);
impl TryFrom<u32> for ChunkSize {
    type Error = u32;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if !VALID_CHUNK_SIZES.contains(&value) {
            return Err(value);
        }
        Ok(ChunkSize(value))
    }
}

pub struct CachalotBuilder {
    buckets: Vec<String>,
    chunk_size: u32,
    max_mem_cache: u64,
    ttl: Duration,
    tti: Duration,
    disk_cache: Option<(PathBuf, u64, Duration, u8)>,
}

impl CachalotBuilder {
    pub fn with_disk_cache<P: AsRef<Path>>(self, path: P) -> DiskCacheBuilder {
        DiskCacheBuilder::new(
            path,
            1024 * 1024 * 1024, // 1 GiB
            10,
            self,
        )
    }

    pub fn chunk_size<P: TryInto<ChunkSize>>(mut self, chunk_size: P) -> anyhow::Result<Self> {
        self.chunk_size = chunk_size
            .try_into()
            .map_err(|_| anyhow!("chunk_size invalid"))?
            .0;
        Ok(self)
    }

    pub fn time_to_live(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    pub fn time_to_idle(mut self, tti: Duration) -> Self {
        self.tti = tti;
        self
    }

    pub fn max_mem_cache<M: TryInto<NonZeroU64>>(
        mut self,
        max_mem_cache: M,
    ) -> anyhow::Result<Self> {
        self.max_mem_cache = max_mem_cache
            .try_into()
            .map_err(|_| anyhow!("max_mem_cache cannot be zero"))?
            .get();
        Ok(self)
    }

    pub async fn build(mut self) -> anyhow::Result<Cachalot> {
        if self.max_mem_cache < self.chunk_size as u64 {
            bail!("max_mem_cache cannot be smaller than chunk_size");
        }

        let mem_cache = CacheBuilder::new(self.max_mem_cache)
            .weigher(|_, v: &Bytes| v.len() as u32)
            .time_to_live(self.ttl)
            .time_to_idle(self.tti)
            .build();

        let disk_cache = if let Some((path, max_size, ttl, max_connections)) = self.disk_cache {
            if max_size < self.chunk_size as u64 {
                bail!("max_disk_cache cannot be smaller than chunk_size");
            }

            let page_size = self.chunk_size;
            // reduce chunk_size to fit row into single database page
            self.chunk_size -= 128;

            Some(
                DiskCache::new(
                    path.as_path(),
                    page_size,
                    self.chunk_size,
                    max_size,
                    max_connections,
                    ttl,
                    self.buckets,
                )
                .await?,
            )
        } else {
            None
        };

        Ok(Cachalot {
            mem_cache,
            disk_cache,
            chunk_size: self.chunk_size as usize,
        })
    }
}

impl Cachalot {
    pub fn builder<I, T>(buckets: I) -> CachalotBuilder
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        CachalotBuilder {
            buckets: buckets
                .into_iter()
                .map(|s| s.as_ref().to_string())
                .collect(),
            chunk_size: ChunkSize::try_from(32768).unwrap().0,
            max_mem_cache: 1024 * 1024 * 100,
            ttl: Duration::from_secs(86400 * 7),
            tti: Duration::from_secs(86400 * 7),
            disk_cache: None,
        }
    }

    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    pub async fn try_get_with<F>(
        &self,
        bucket: &str,
        path: &str,
        version: u64,
        chunk: u64,
        init: F,
    ) -> anyhow::Result<Bytes>
    where
        F: Future<Output = anyhow::Result<Bytes>>,
    {
        Ok(self
            .mem_cache
            .try_get_with::<_, anyhow::Error>(
                (bucket.to_string(), path.to_string(), version, chunk),
                async {
                    if let Some(disk_cache) = self.disk_cache.as_ref() {
                        if let Some(content) = disk_cache.get(bucket, path, version, chunk).await? {
                            return Ok(content);
                        }
                    }

                    let content = init.await?;

                    if let Some(disk_cache) = self.disk_cache.as_ref() {
                        disk_cache
                            .put(bucket, path, version, chunk, content.clone())
                            .await?;
                    }

                    Ok(content)
                },
            )
            .await
            .map_err(|e: Arc<anyhow::Error>| anyhow::anyhow!(e))?)
    }
}

pub(crate) fn content_hash(content: &[u8]) -> Hash {
    let mut hasher = blake3::Hasher::new_keyed(&CONTENT_HASH_SEED);
    hasher.update("cachalot content hash v1 start\n".as_bytes());
    hasher.update("length:".as_bytes());
    hasher.update(content.len().to_le_bytes().as_slice());
    hasher.update("\ncontent:".as_bytes());
    hasher.update(content);
    hasher.update("\ncachalot content hash v1 end".as_bytes());
    hasher.finalize()
}

#[derive(Debug, Clone)]
pub(crate) struct SqlitePool {
    writer: Pool<Sqlite>,
    reader: Pool<Sqlite>,
}

impl SqlitePool {
    pub fn read(&self) -> &Pool<Sqlite> {
        &self.reader
    }

    pub fn write(&self) -> &Pool<Sqlite> {
        &self.writer
    }
}
