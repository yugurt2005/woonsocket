use clap::Parser;
use woonsocket_work::args::WoonsocketServerOpt;

fn main() -> std::io::Result<()> {
    woonsocket::server::run(WoonsocketServerOpt::parse())
}
