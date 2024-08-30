use crate::io_scheduler::{Action, QueueState};
use std::time::{Duration, SystemTime};

pub(crate) struct DownloadStrategy {
    max_active: usize,
    max_wait_for_match: Duration,
    min_wait_for_match: Duration,
}

impl DownloadStrategy {
    pub fn new(
        max_active: usize,
        max_wait_for_match: Duration,
        min_wait_for_match: Duration,
    ) -> Self {
        Self {
            max_active,
            max_wait_for_match,
            min_wait_for_match,
        }
    }

    pub fn advise<'a>(
        &self,
        state: &'a QueueState,
    ) -> anyhow::Result<(Duration, Option<Action<'a>>)> {
        assert!(!state.waiting.is_empty());
        // first, check if we need to free idle resources
        if state.active.len() + state.preparing.len() + state.idle.len() >= self.max_active {
            // find the resource that has been idle the longest
            if let Some(idle) = state.idle.iter().min_by_key(|i| i.since) {
                return Ok((Duration::from_millis(0), Some(Action::Free(idle))));
            } else {
                // nothing we can do now
                return Ok((Duration::from_millis(250), None));
            }
        }

        // get the wait resource with the lowest offset
        let waiting = state.waiting.iter().min_by_key(|w| w.offset).unwrap();
        if waiting.offset == 0 {
            // no need to wait here
            return Ok((
                Duration::from_millis(250),
                Some(Action::NewResource(waiting)),
            ));
        }
        let wait_duration = SystemTime::now()
            .duration_since(waiting.since)
            .unwrap_or_default();
        if wait_duration < self.min_wait_for_match {
            let next_try_in = (self.min_wait_for_match - wait_duration) + Duration::from_millis(1);
            // check later
            return Ok((next_try_in, None));
        }

        if state.active.get_before_offset(waiting.offset).count() == 0 {
            // start new download now
            return Ok((
                Duration::from_millis(250),
                Some(Action::NewResource(waiting)),
            ));
        }

        let wait_duration = SystemTime::now()
            .duration_since(waiting.since)
            .unwrap_or_default();
        if wait_duration < self.max_wait_for_match {
            let next_try_in = (self.max_wait_for_match - wait_duration) + Duration::from_millis(1);
            // check later
            return Ok((next_try_in, None));
        }

        Ok((
            Duration::from_millis(250),
            Some(Action::NewResource(waiting)),
        ))
    }
}
