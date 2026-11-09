use serapendiya::{
    domain::Offset,
    storage::{Log, RealStorageIo, StorageIo},
};
use std::{env, fs::File, io, process::Command};

#[derive(Debug)]
struct Marked;
fn mark(name: &str) -> io::Result<()> {
    let s = Command::new("dmsetup")
        .args(["message", "serapendiya-replay", "0", "mark", name])
        .status()?;
    if s.success() {
        Ok(())
    } else {
        Err(io::Error::other("dmsetup mark failed"))
    }
}
impl StorageIo for Marked {
    fn write_segment(&self, f: &mut File, b: &[u8]) -> io::Result<()> {
        RealStorageIo.write_segment(f, b)?;
        mark("segment-write")
    }
    fn sync_segment(&self, f: &File) -> io::Result<()> {
        RealStorageIo.sync_segment(f)?;
        mark("segment-sync")
    }
    fn truncate_segment(&self, f: &File, n: u64) -> io::Result<()> {
        RealStorageIo.truncate_segment(f, n)
    }
    fn write_checkpoint(&self, f: &mut File, b: &[u8]) -> io::Result<()> {
        RealStorageIo.write_checkpoint(f, b)?;
        mark("checkpoint-write")
    }
    fn sync_checkpoint(&self, f: &File) -> io::Result<()> {
        RealStorageIo.sync_checkpoint(f)?;
        mark("checkpoint-sync")
    }
    fn rename_checkpoint(&self, d: &File) -> io::Result<()> {
        RealStorageIo.rename_checkpoint(d)?;
        mark("checkpoint-rename")
    }
    fn sync_directory(&self, d: &File) -> io::Result<()> {
        RealStorageIo.sync_directory(d)?;
        mark("directory-sync")
    }
}
#[test]
#[ignore]
fn generate_trace() {
    let path = env::var("REPLAY_LOG_DIR").unwrap();
    let mut log = Log::open(&path).unwrap();
    assert_eq!(log.append(b"acknowledged").unwrap(), Offset::ZERO);
    drop(log);
    mark("acknowledged").unwrap();
    let mut log = Log::open_with_io(&path, Box::new(Marked)).unwrap();
    assert_eq!(log.append(b"second").unwrap(), Offset::new(1));
    drop(log);
    mark("second-ack").unwrap();
}
#[test]
#[ignore]
fn verify_replay() {
    let path = env::var("REPLAY_LOG_DIR").unwrap();
    let mut log = Log::open(&path).unwrap();
    assert_eq!(
        log.read_batch(Offset::ZERO).unwrap().unwrap().records,
        b"acknowledged"
    );
    if matches!(
        env::var("REPLAY_MARK").unwrap().as_str(),
        "directory-sync" | "second-ack"
    ) {
        assert_eq!(
            log.read_batch(Offset::new(1)).unwrap().unwrap().records,
            b"second"
        );
    }
}
