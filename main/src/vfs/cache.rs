use crate::vfs::inode::{Inode, InodeId};
use anyhow::Result;
use itertools::Itertools;
use moka::future::{Cache, CacheBuilder};
use moka::Expiry;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(super) struct CacheManager {
    inode_cache: Cache<InodeId, Option<Inode>>,
    dir_cache: Cache<InodeId, Vec<Inode>>,
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
            dir_cache: CacheBuilder::new(max_dir_entries)
                .time_to_live(ttl)
                .time_to_idle(tti)
                .support_invalidation_closures()
                .weigher(|_, v: &Vec<_>| v.len() as u32 + 1)
                .build(),
        }
    }

    pub async fn inode_by_id(
        &self,
        id: InodeId,
        init: impl Future<Output = Result<Option<Inode>>>,
    ) -> Result<Option<Inode>> {
        self.inode_cache
            .try_get_with(id, init)
            .await
            .map_err(|e: Arc<anyhow::Error>| anyhow::anyhow!(e))
    }

    pub async fn read_dir(
        &self,
        dir_id: InodeId,
        init: impl Future<Output = Result<(Vec<Inode>, Vec<InodeId>)>>,
    ) -> Result<Vec<Inode>> {
        self.dir_cache
            .try_get_with(dir_id, async {
                let (inodes, affected_ids) = init.await?;
                self.invalidate_caches(&affected_ids).await;
                for inode in &inodes {
                    self.inode_cache
                        .insert(inode.id(), Some(inode.clone()))
                        .await;
                }
                Ok(inodes)
            })
            .await
            .map_err(|e: Arc<anyhow::Error>| anyhow::anyhow!(e))
    }

    pub async fn invalidate_caches(&self, inode_ids: &Vec<InodeId>) {
        if inode_ids.is_empty() {
            return;
        }
        let mut inode_ids = inode_ids
            .into_iter()
            .unique()
            .map(|i| i.clone())
            .collect_vec();

        inode_ids.sort_unstable();

        {
            let inode_ids = inode_ids.clone();
            self.inode_cache
                .invalidate_entries_if(move |k, _| inode_ids.contains(k))
                .expect("unable to invalidate caches using invalidate_entries_if");
        }

        {
            let inode_ids = inode_ids.clone();
            self.dir_cache
                .invalidate_entries_if(move |k, _| inode_ids.contains(k))
                .expect("unable to invalidate caches using invalidate_entries_if");
        }
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
