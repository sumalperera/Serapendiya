use serapendiya::{
    domain::Offset,
    storage::{Log, RealStorageIo, StorageIo},
};
use std::{env, fs::File, io, process::Command};

#[derive(Debug)]
struct Marked(&'static str);
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
        mark(&format!("{}segment-write", self.0))
    }
    fn sync_segment(&self, f: &File) -> io::Result<()> {
        RealStorageIo.sync_segment(f)?;
        mark(&format!("{}segment-sync", self.0))
    }
    fn truncate_segment(&self, f: &File, n: u64) -> io::Result<()> {
        RealStorageIo.truncate_segment(f, n)
    }
    fn write_checkpoint(&self, f: &mut File, b: &[u8]) -> io::Result<()> {
        RealStorageIo.write_checkpoint(f, b)?;
        mark(&format!("{}checkpoint-write", self.0))
    }
    fn sync_checkpoint(&self, f: &File) -> io::Result<()> {
        RealStorageIo.sync_checkpoint(f)?;
        mark(&format!("{}checkpoint-sync", self.0))
    }
    fn rename_checkpoint(&self, d: &File) -> io::Result<()> {
        RealStorageIo.rename_checkpoint(d)?;
        mark(&format!("{}checkpoint-rename", self.0))
    }
    fn sync_directory(&self, d: &File) -> io::Result<()> {
        RealStorageIo.sync_directory(d)?;
        mark(&format!("{}directory-sync", self.0))
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
    let mut log = Log::open_with_io(&path, Box::new(Marked(""))).unwrap();
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

#[test]
#[ignore]
fn generate_initialization_trace() {
    let path = env::var("REPLAY_LOG_DIR").unwrap();
    mark("init-before-open").unwrap();
    let log = Log::open_with_io(&path, Box::new(Marked("init-"))).unwrap();
    drop(log);
    mark("init-return").unwrap();
    let mut log = Log::open_with_io(&path, Box::new(Marked("first-"))).unwrap();
    assert_eq!(log.append(b"first").unwrap(), Offset::ZERO);
    drop(log);
    mark("first-ack").unwrap();
}

// For a refused, checkpoint-less initialization, compare names and contents
// before and after open; metadata such as atime is not part of this check.
fn snapshot(path: &std::path::Path) -> Vec<(std::ffi::OsString, Vec<u8>)> {
    let mut entries: Vec<_> = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (entry.file_name(), std::fs::read(entry.path()).unwrap())
        })
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}

#[test]
#[ignore]
fn verify_initialization_replay() {
    let path = std::path::PathBuf::from(env::var("REPLAY_LOG_DIR").unwrap());
    let mark = env::var("REPLAY_MARK").unwrap();
    let existing = path.exists().then(|| snapshot(&path));
    if matches!(mark.as_str(), "init-directory-sync" | "init-return") || mark.starts_with("first-")
    {
        assert!(
            path.join("checkpoint").is_file(),
            "durable initialization lost its checkpoint at {mark}"
        );
    }
    if let Some(before) = &existing {
        if !before.is_empty() && !path.join("checkpoint").exists() {
            println!("REPLAY_CLASS=ambiguous-checkpoint-missing");
            assert!(
                Log::open(&path).is_err(),
                "ambiguous initialization must fail closed"
            );
            assert_eq!(
                &snapshot(&path),
                before,
                "refusal modified existing contents"
            );
            return;
        }
    }
    let mut log = match Log::open(&path) {
        Ok(log) => log,
        Err(error) => {
            assert!(
                mark.starts_with("init-")
                    && !matches!(mark.as_str(), "init-directory-sync" | "init-return"),
                "unexpected refusal after durable initialization: {error}"
            );
            println!("REPLAY_CLASS=other-incomplete-refusal");
            assert_eq!(
                Some(snapshot(&path)),
                existing,
                "refusal modified existing contents"
            );
            return;
        }
    };
    println!("REPLAY_CLASS=opened");
    let first = log.read_batch(Offset::ZERO).unwrap();
    if matches!(mark.as_str(), "first-directory-sync" | "first-ack") {
        assert_eq!(first.unwrap().records, b"first");
    } else if let Some(batch) = first {
        assert_eq!(batch.records, b"first");
    }
}
