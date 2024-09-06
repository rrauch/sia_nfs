use crate::disk_cache::{page_count, try_free_pages, Config};
use anyhow::anyhow;
use chrono::Utc;
use sqlx::{Sqlite, Transaction};
use std::cmp::min;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime};

pub(super) struct Housekeeper {
    config: Config,
    last_vacuum: SystemTime,
}

impl Housekeeper {
    pub async fn new(config: Config, buckets: &Vec<String>) -> anyhow::Result<Self> {
        let mut tx = config.db.write().begin().await?;

        let db_buckets = sqlx::query!("SELECT DISTINCT bucket FROM files")
            .fetch_all(tx.as_mut())
            .await?
            .into_iter()
            .map(|r| r.bucket)
            .collect::<Vec<_>>();

        let obsolete_buckets = db_buckets
            .into_iter()
            .filter(|b| !buckets.contains(b))
            .collect::<Vec<_>>();

        if !obsolete_buckets.is_empty() {
            tracing::info!("removing data for obsolete buckets");
            for bucket in obsolete_buckets {
                sqlx::query!("DELETE FROM files WHERE bucket = ?", bucket)
                    .execute(tx.as_mut())
                    .await?;
            }
        }

        remove_expired(&mut tx, &config).await?;

        free_pages(&mut tx, &config).await?;

        let usable_pages = usable_pages(&mut tx, &config).await?;

        tx.commit().await?;

        config
            .usable_pages_est
            .store(usable_pages, Ordering::SeqCst);

        sqlx::query!("VACUUM").execute(config.db.write()).await?;

        Ok(Self {
            config,
            last_vacuum: SystemTime::now(),
        })
    }

    pub async fn run(&mut self) {
        let mut next_run_in = Duration::from_secs(300);
        let mut next_expired_removal = SystemTime::now() + Duration::from_secs(900);

        loop {
            let mut next_vacuum = self.last_vacuum + Duration::from_secs(86400);

            tokio::time::sleep(next_run_in).await;
            next_run_in = Duration::from_secs(60);
            if next_expired_removal <= SystemTime::now() {
                match self.remove_expired().await {
                    Ok(removed) => {
                        next_expired_removal = SystemTime::now() + Duration::from_secs(900);
                        if removed {
                            next_vacuum = SystemTime::now();
                        }
                    }
                    Err(err) => {
                        tracing::error!(error = %err, "error removing expired entries from disk cache");
                        next_expired_removal = SystemTime::now() + Duration::from_secs(3600);
                    }
                }
            }
            match self.free_pages().await {
                Ok(pages_freed) => {
                    if pages_freed > 0 {
                        tracing::info!(pages_freed = pages_freed, "freed pages");
                        next_vacuum = SystemTime::now();
                    }
                }
                Err(err) => {
                    tracing::error!(error = %err, "error trying to free space in disk cache");
                    next_run_in = Duration::from_secs(900);
                }
            }

            if next_vacuum <= SystemTime::now() {
                tracing::debug!("running VACUUM");
                let _ = sqlx::query!("VACUUM").execute(self.config.db.write()).await;
                self.last_vacuum = SystemTime::now();
            }
        }
    }

    async fn remove_expired(&self) -> anyhow::Result<bool> {
        let mut tx = self.config.db.write().begin().await?;
        let mut up = None;
        let ret = remove_expired(&mut tx, &self.config).await?;
        if ret {
            up = Some(usable_pages(&mut tx, &self.config).await?);
        }
        tx.commit().await?;
        if let Some(up) = up {
            self.config.usable_pages_est.store(up, Ordering::SeqCst);
        }
        Ok(ret)
    }

    async fn free_pages(&self) -> anyhow::Result<u32> {
        let mut tx = self.config.db.writer.begin().await?;
        let pages_freed = free_pages(&mut tx, &self.config).await?;
        let up = usable_pages(&mut tx, &self.config).await?;
        tx.commit().await?;
        self.config.usable_pages_est.store(up, Ordering::SeqCst);
        Ok(pages_freed)
    }
}

async fn free_pages(tx: &mut Transaction<'_, Sqlite>, config: &Config) -> anyhow::Result<u32> {
    let extra_pages_needed = {
        let usable_pages = usable_pages(tx, config).await?;
        if usable_pages >= config.min_free_pages {
            0
        } else {
            config.min_free_pages - usable_pages
        }
    };

    let mut pages_freed = 0;
    if extra_pages_needed > 0 {
        tracing::info!(
            extra_pages_needed = extra_pages_needed,
            "freeing pages from disk cache"
        );
        pages_freed = try_free_pages(extra_pages_needed, tx, config).await?;
        tracing::info!(pages_freed = pages_freed, "pages freed");
    }

    Ok(pages_freed)
}

async fn remove_expired(tx: &mut Transaction<'_, Sqlite>, config: &Config) -> anyhow::Result<bool> {
    let deadline = Utc::now() - config.ttl;

    let did_work = sqlx::query!("DELETE FROM content WHERE created <= ?", deadline)
        .execute(tx.as_mut())
        .await?
        .rows_affected()
        > 0;

    Ok(did_work)
}

async fn usable_pages(tx: &mut Transaction<'_, Sqlite>, config: &Config) -> anyhow::Result<u32> {
    let max_page_count = max_page_count(tx).await?;
    let page_count = page_count(tx).await?;
    let freelist_count = freelist_count(tx).await?;

    let available_pages = max_page_count.saturating_sub(page_count);
    if available_pages == 0 {
        return Ok(freelist_count);
    }

    let path = config.path.clone();
    let available_disk_space =
        tokio::task::spawn_blocking(move || fs4::available_space(&path)).await??;

    let potential_pages =
        u32::try_from(available_disk_space / config.page_size as u64).unwrap_or(u32::MAX);

    let usable = min(potential_pages + freelist_count, available_pages);
    Ok(usable)
}

async fn freelist_count(tx: &mut Transaction<'_, Sqlite>) -> anyhow::Result<u32> {
    sqlx::query!("PRAGMA freelist_count")
        .fetch_one(tx.as_mut())
        .await?
        .freelist_count
        .map(|c| c as u32)
        .ok_or(anyhow!("unable to get freelist_count from database"))
}

async fn max_page_count(tx: &mut Transaction<'_, Sqlite>) -> anyhow::Result<u32> {
    sqlx::query!("PRAGMA max_page_count")
        .fetch_one(tx.as_mut())
        .await?
        .max_page_count
        .map(|c| c as u32)
        .ok_or(anyhow!("unable to get max_page_count from database"))
}
