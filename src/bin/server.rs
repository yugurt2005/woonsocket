use clap::Parser;
use std::time::Duration;
use woonsocket::server;
use woonsocket_work::args::WoonsocketServerOpt;

fn main() -> std::io::Result<()> {
    let opt = WoonsocketServerOpt::parse();
    server::run(opt.port, Duration::from_secs(opt.runtime_secs))
}
