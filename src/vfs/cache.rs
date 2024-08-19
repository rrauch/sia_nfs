use crate::vfs::inode::Inode;
use anyhow::Result;
use itertools::Itertools;
use moka::future::{Cache, CacheBuilder};
use moka::Expiry;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(super) struct CacheManager {
    inode_cache: Cache<u64, Option<Inode>>,
    path_inode_id_cache: Cache<(String, String), Option<u64>>,
    inode_id_path_cache: Cache<u64, Option<(String, String)>>,
    dir_cache: Cache<u64, Vec<u64>>,
}

impl CacheManager {
    pub fn new(
        max_inodes: u64,
        max_dir_entries: u64,
        ttl: Duration,
        tti: Duration,
        negative_ttl: Duration,
    ) -> Self {
        Self {
            inode_cache: CacheBuilder::new(max_inodes)
                .time_to_live(ttl)
                .time_to_idle(tti)
                .expire_after(NoneExpirationPolicy::new(negative_ttl))
                .support_invalidation_closures()
                .build(),
            path_inode_id_cache: CacheBuilder::new(max_inodes)
                .time_to_live(ttl)
                .time_to_idle(tti)
                .expire_after(NoneExpirationPolicy::new(negative_ttl))
                .support_invalidation_closures()
                .build(),
            inode_id_path_cache: CacheBuilder::new(max_inodes)
                .time_to_live(ttl)
                .time_to_idle(tti)
                .expire_after(NoneExpirationPolicy::new(negative_ttl))
                .support_invalidation_closures()
                .build(),
            dir_cache: CacheBuilder::new(max_dir_entries)
                .time_to_live(ttl)
                .time_to_idle(tti)
                .support_invalidation_closures()
                .weigher(|_, v: &Vec<_>| v.len() as u32 + 1)
                .build(),
        }
    }

    pub async fn inode_id_by_bucket_path(
        &self,
        bucket_path: &(String, String),
        parent_path: &str,
        init: impl Future<Output = Result<Option<Inode>>>,
    ) -> Result<Option<u64>> {
        self.path_inode_id_cache
            .try_get_with_by_ref(&bucket_path, async {
                Ok(match init.await? {
                    Some(inode) => {
                        let id = inode.id();
                        let path = inode.to_path(parent_path);
                        let path = path.trim_end_matches('/');
                        self.inode_id_path_cache
                            .insert(id, Some((bucket_path.0.clone(), path.to_string())))
                            .await;
                        self.inode_cache.insert(id, Some(inode)).await;
                        Some(id)
                    }
                    None => None,
                })
            })
            .await
            .map_err(|e: Arc<anyhow::Error>| anyhow::anyhow!(e))
    }

    pub async fn inode_by_id(
        &self,
        id: u64,
        init: impl Future<Output = Result<Option<Inode>>>,
    ) -> Result<Option<Inode>> {
        self.inode_cache
            .try_get_with(id, init)
            .await
            .map_err(|e: Arc<anyhow::Error>| anyhow::anyhow!(e))
    }

    pub async fn inode_to_bucket_path(
        &self,
        id: u64,
        init: impl Future<Output = Result<Option<(String, String)>>>,
    ) -> Result<Option<(String, String)>> {
        self.inode_id_path_cache
            .try_get_with(id, async {
                Ok(match init.await? {
                    Some(bucket_path) => {
                        self.path_inode_id_cache
                            .insert(bucket_path.clone(), Some(id))
                            .await;
                        Some(bucket_path)
                    }
                    None => None,
                })
            })
            .await
            .map_err(|e: Arc<anyhow::Error>| anyhow::anyhow!(e))
    }

    pub async fn read_dir(
        &self,
        dir_id: u64,
        bucket: String,
        path: String,
        init: impl Future<Output = Result<(Vec<Inode>, Vec<u64>)>>,
    ) -> Result<Vec<u64>> {
        self.dir_cache
            .try_get_with(dir_id, async {
                let (inodes, obsolete_ids) = init.await?;
                self.invalidate_caches(obsolete_ids);
                let mut ids = Vec::with_capacity(inodes.len());
                for inode in inodes {
                    let path = inode.to_path(&path);
                    let id = inode.id();
                    self.inode_cache.insert(id, Some(inode)).await;
                    self.inode_id_path_cache
                        .insert(id, Some((bucket.clone(), path.clone())))
                        .await;
                    self.path_inode_id_cache
                        .insert((bucket.clone(), path), Some(id))
                        .await;
                    ids.push(id);
                }
                Ok(ids)
            })
            .await
            .map_err(|e: Arc<anyhow::Error>| anyhow::anyhow!(e))
    }

    pub async fn new_dir(&self, bucket: String, path: String, parent_id: u64) {
        self.dir_cache.invalidate(&parent_id).await;
        self.path_inode_id_cache.invalidate(&(bucket, path)).await;
    }

    pub async fn new_file(&self, id: u64, parent_id: u64) {
        self.inode_cache.invalidate(&id).await;
        self.inode_id_path_cache.invalidate(&id).await;
        self.dir_cache.invalidate(&parent_id).await;
    }

    pub async fn invalidate_bucket_path(&self, bucket: String, path: String) {
        let bucket_path = &(bucket, path);
        self.path_inode_id_cache.invalidate(bucket_path).await;
    }

    pub fn invalidate_caches<I: IntoIterator<Item = u64>>(&self, inode_ids: I) {
        let mut inode_ids = inode_ids.into_iter().collect_vec();
        inode_ids.sort_unstable();

        {
            let inode_ids = inode_ids.clone();
            self.dir_cache
                .invalidate_entries_if(move |k, v| {
                    inode_ids.contains(k) || v.iter().any(|i| inode_ids.binary_search(i).is_ok())
                })
                .expect("unable to invalidate caches using invalidate_entries_if");
        }

        {
            let inode_ids = inode_ids.clone();
            self.inode_cache
                .invalidate_entries_if(move |k, _| inode_ids.contains(k))
                .expect("unable to invalidate caches using invalidate_entries_if");
        }

        {
            let inode_ids = inode_ids.clone();
            self.path_inode_id_cache
                .invalidate_entries_if(move |_, v| match v {
                    Some(id) => inode_ids.contains(id),
                    None => true, // make sure to invalidate all `None`s too!
                })
                .expect("unable to invalidate caches using invalidate_entries_if");
        }

        self.inode_id_path_cache
            .invalidate_entries_if(move |k, _| inode_ids.contains(k))
            .expect("unable to invalidate caches using invalidate_entries_if");
    }
}

struct NoneExpirationPolicy {
    expiration_if_none: Duration,
}

impl NoneExpirationPolicy {
    fn new(expiration_if_none: Duration) -> Self {
        Self { expiration_if_none }
    }
}

impl<K, V> Expiry<K, Option<V>> for NoneExpirationPolicy {
    fn expire_after_create(
        &self,
        _key: &K,
        value: &Option<V>,
        created_at: Instant,
    ) -> Option<Duration> {
        match value {
            Some(_) => None, // keep the normal expiration policy
            None => {
                // override if `None`
                let expires = created_at + self.expiration_if_none;
                let now = Instant::now();
                Some(if expires > now {
                    expires.duration_since(now)
                } else {
                    Duration::ZERO
                })
            }
        }
    }
}
