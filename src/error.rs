use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("could not read configuration {path}: {source}")]
    ConfigRead { path: String, source: io::Error },
    #[error("configuration syntax is invalid: {0}")]
    ConfigSyntax(#[from] toml::de::Error),
    #[error("configuration is invalid: {0}")]
    ConfigValidation(String),
    #[error("invalid {kind}: {value:?}")]
    InvalidValue { kind: &'static str, value: String },
    #[error("storage I/O failed: {0}")]
    StorageIo(#[from] io::Error),
    #[error("storage checkpoint is invalid: {0}")]
    InvalidCheckpoint(String),
    #[error("certified storage corruption at byte {position}: {reason}")]
    CertifiedCorruption { position: u64, reason: String },
    #[error("storage batch is invalid: {0}")]
    InvalidBatch(String),
}

pub type Result<T> = std::result::Result<T, Error>;
