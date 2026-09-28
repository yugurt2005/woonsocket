use std::time::Duration;
use woonsocket_work::Work;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenLoopKind {
    Constant,
    Poisson,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    ClosedLoop { num_threads: u64 },
    OpenLoop { interval: Duration, kind: OpenLoopKind },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunSettings {
    pub mode: Mode,
    pub work: Work,
    pub runtime: Duration,
    pub warmup: Duration,
}
