//! Small, dependency-light G0 boundaries for Serapendiya.

pub mod config;
pub mod domain;
pub mod error;
pub mod storage;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
