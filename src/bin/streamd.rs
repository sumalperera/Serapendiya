use std::path::PathBuf;

use clap::Parser;
use serapendiya::{VERSION, config::Config, error::Result};

#[derive(Debug, Parser)]
#[command(name = "streamd", version = VERSION, about = "Serapendiya service command")]
struct Args {
    /// Load and validate a configuration file, then exit.
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
}

fn run(args: Args) -> Result<()> {
    if let Some(path) = args.config {
        Config::load(path)?;
    }
    Ok(())
}

fn main() {
    if let Err(error) = run(Args::parse()) {
        eprintln!("streamd: {error}");
        std::process::exit(2);
    }
}
