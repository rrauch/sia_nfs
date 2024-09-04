use crate::disk_cache::{free_pages, remove_expired, usable_pages, Config};
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime};

pub(super) struct Housekeeper {
    config: Config,
}

impl Housekeeper {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub async fn run(&mut self) {
        let mut next_run_in = Duration::from_secs(300);
        let mut next_vacuum = SystemTime::now() + Duration::from_secs(3600);
        let mut next_expired_removal = SystemTime::now() + Duration::from_secs(900);

        loop {
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

            if let Err(err) = self.free_pages().await {
                tracing::error!(error = %err, "error trying to free space in disk cache");
                next_run_in = Duration::from_secs(900);
            } else {
                next_vacuum = SystemTime::now();
            }
            if next_vacuum <= SystemTime::now() {
                tracing::debug!("running VACUUM");
                let _ = sqlx::query!("VACUUM").execute(self.config.db.write()).await;
                next_vacuum = SystemTime::now() + Duration::from_secs(3600);
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

    async fn free_pages(&self) -> anyhow::Result<()> {
        let mut tx = self.config.db.writer.begin().await?;
        free_pages(&mut tx, &self.config).await?;
        let up = usable_pages(&mut tx, &self.config).await?;
        tx.commit().await?;
        self.config.usable_pages_est.store(up, Ordering::SeqCst);
        Ok(())
    }
}
