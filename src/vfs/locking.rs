use anyhow::bail;
use itertools::Itertools;
use std::cmp::PartialEq;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};
use tokio::task::JoinHandle;
use tokio::time::timeout;

pub(super) struct LockManager {
    acquisition_timeout: Duration,
    path_locks: Arc<Mutex<HashMap<String, Arc<RwLock<String>>>>>,
    gc: JoinHandle<()>,
}

impl LockManager {
    pub fn new(acquisition_timeout: Duration) -> Self {
        let path_locks = Arc::new(Mutex::new(HashMap::new()));
        let gc = {
            let path_locks = path_locks.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    {
                        let mut locks = path_locks.lock().expect("unable to get file locks");
                        // remove all locks currently not held outside this map
                        locks.retain(|_, lock| Arc::strong_count(lock) > 1)
                    }
                }
            })
        };
        Self {
            acquisition_timeout,
            path_locks,
            gc,
        }
    }

    pub async fn lock<'a, T: IntoIterator<Item = LockRequest<'a>>>(
        &self,
        reqs: T,
    ) -> anyhow::Result<Vec<LockHolder>> {
        let mut requests: HashMap<String, LockRequest> = HashMap::new();
        reqs.into_iter().for_each(|r| {
            let path = format!("/{}{}", &r.bucket, &r.path);
            match requests.entry(path) {
                Entry::Occupied(mut entry) => {
                    // replace read locks with write locks if both are present
                    if r.lock_type == LockType::Write && entry.get().lock_type == LockType::Read {
                        entry.insert(r);
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(r);
                }
            }
        });

        let locks = {
            let mut path_locks = self.path_locks.lock().expect("unable to lock path_locks");
            // it's important to sort the paths to avoid deadlocks, hopefully
            requests
                .iter()
                .map(|(path, _)| path)
                .sorted_unstable()
                .map(|path| {
                    (
                        path.clone(),
                        path_locks
                            .entry(path.clone())
                            .or_insert_with(|| Arc::new(RwLock::new(path.clone())))
                            .clone(),
                    )
                })
                .collect::<BTreeMap<_, _>>()
        };

        let expected = locks.len();
        if expected == 0 {
            bail!("no locks requested");
        }

        let res = timeout(self.acquisition_timeout, async move {
            let mut resp = Vec::with_capacity(requests.len());
            for (path, lock) in locks.into_iter() {
                let req = requests.remove(&path).expect("request for lock is missing");
                resp.push(match req.lock_type {
                    LockType::Read => {
                        tracing::trace!("acquiring read lock for {}", path);
                        LockHolder::Read(ReadLock {
                            inner: lock.read_owned().await,
                        })
                    }

                    LockType::Write => {
                        tracing::trace!("acquiring write lock for {}", path);
                        LockHolder::Write(WriteLock {
                            inner: lock.write_owned().await,
                        })
                    }
                });
            }
            resp
        })
        .await?;

        assert_eq!(res.len(), expected);

        Ok(res)
    }
}

impl Drop for LockManager {
    fn drop(&mut self) {
        self.gc.abort();
    }
}

#[derive(PartialEq, Eq)]
pub(super) enum LockType {
    Read,
    Write,
}

pub(super) struct LockRequest<'a> {
    bucket: &'a str,
    path: &'a str,
    lock_type: LockType,
}

impl<'a> LockRequest<'a> {
    pub(super) fn read(bucket: &'a str, path: &'a str) -> Self {
        LockRequest {
            bucket,
            path,
            lock_type: LockType::Read,
        }
    }

    pub(super) fn write(bucket: &'a str, path: &'a str) -> Self {
        LockRequest {
            bucket,
            path,
            lock_type: LockType::Write,
        }
    }
}

pub(super) struct ReadLock {
    inner: OwnedRwLockReadGuard<String>,
}

impl ReadLock {
    pub fn path(&self) -> &str {
        self.inner.as_str()
    }
}

impl Drop for ReadLock {
    fn drop(&mut self) {
        tracing::trace!("dropping read lock for {}", self.inner.as_str())
    }
}

pub(super) struct WriteLock {
    inner: OwnedRwLockWriteGuard<String>,
}

impl WriteLock {
    pub fn path(&self) -> &str {
        self.inner.as_str()
    }
}

impl Drop for WriteLock {
    fn drop(&mut self) {
        tracing::trace!("dropping write lock for {}", self.inner.as_str())
    }
}

pub(super) enum LockHolder {
    Read(ReadLock),
    Write(WriteLock),
}
