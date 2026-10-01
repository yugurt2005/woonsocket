use clap::Parser;
use std::net::SocketAddr;
use std::time::Duration;
use woonsocket::client;
use woonsocket::settings::{Mode, OpenLoopKind, RunSettings};
use woonsocket_work::args::{self, ClientMode, WoonsocketClientOpt};

fn main() -> std::io::Result<()> {
    let opt = WoonsocketClientOpt::parse();
    let (mode, num_threads) = match opt.mode {
        ClientMode::ClosedLoop { num_threads } => (Mode::ClosedLoop, num_threads),
        ClientMode::OpenLoop { interval_us, kind } => {
            let kind = match kind {
                args::OpenLoopKind::Constant => OpenLoopKind::Constant,
                args::OpenLoopKind::Poisson => OpenLoopKind::Poisson,
            };
            let interval = Duration::from_micros(interval_us);
            (Mode::OpenLoop { interval, kind }, 1)
        }
    };
    client::run(&RunSettings {
        addr: SocketAddr::from((opt.ip, opt.port)),
        mode,
        num_threads,
        work: opt.work,
        duration: Duration::from_secs(opt.runtime_secs),
        outpath: opt.outpath,
    })
}
