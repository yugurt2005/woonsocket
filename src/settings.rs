use serde::Serialize;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use woonsocket_work::Work;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum OpenLoopKind {
    Constant,
    Poisson,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Mode {
    ClosedLoop,
    OpenLoop { interval: Duration, kind: OpenLoopKind },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunSettings {
    pub addr: SocketAddr,
    pub mode: Mode,
    pub num_threads: u64,
    pub work: Work,
    pub duration: Duration,
    pub outpath: PathBuf,
}
