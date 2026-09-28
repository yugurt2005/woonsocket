use std::time::{SystemTime, UNIX_EPOCH};

#[inline]
pub fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}
