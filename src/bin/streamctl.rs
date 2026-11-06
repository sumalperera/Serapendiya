use clap::{Parser, Subcommand};
use serapendiya::VERSION;

#[derive(Debug, Parser)]
#[command(name = "streamctl", version = VERSION, about = "Serapendiya command-line client")]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Show the client surface available in this build.
    Info,
}

fn main() {
    let args = Args::parse();
    if let Some(Command::Info) = args.command {
        println!("streamctl {VERSION}");
    }
}
