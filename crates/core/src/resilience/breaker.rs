//! Consecutive-failure circuit breaker. Pure (no clock): counts consecutive
//! page-level write failures and reports when the threshold is crossed.

/// Tracks consecutive failures; opens when `threshold` is reached.
#[derive(Debug)]
pub struct CircuitBreaker {
    threshold: u32,
    consecutive: u32,
}

impl CircuitBreaker {
    /// New breaker that opens after `threshold` consecutive failures.
    pub fn new(threshold: u32) -> Self {
        Self {
            threshold: threshold.max(1),
            consecutive: 0,
        }
    }

    /// Record a success; resets the consecutive counter.
    pub fn record_success(&mut self) {
        self.consecutive = 0;
    }

    /// Record a failure; returns `true` if the circuit is now open.
    pub fn record_failure(&mut self) -> bool {
        self.consecutive = self.consecutive.saturating_add(1);
        self.consecutive >= self.threshold
    }

    /// Current consecutive-failure count.
    pub fn consecutive(&self) -> u32 {
        self.consecutive
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opens_after_threshold_consecutive_failures() {
        let mut b = CircuitBreaker::new(3);
        assert!(!b.record_failure());
        assert!(!b.record_failure());
        assert!(b.record_failure(), "third consecutive failure should open");
        assert_eq!(b.consecutive(), 3);
    }

    #[test]
    fn success_resets_the_counter() {
        let mut b = CircuitBreaker::new(3);
        b.record_failure();
        b.record_failure();
        b.record_success();
        assert_eq!(b.consecutive(), 0);
        assert!(!b.record_failure());
    }

    #[test]
    fn threshold_one_opens_immediately() {
        let mut b = CircuitBreaker::new(1);
        assert!(b.record_failure());
    }
}
