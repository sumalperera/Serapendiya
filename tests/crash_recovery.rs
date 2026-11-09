use std::{
    fs::{self, File},
    io,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use serapendiya::{
    domain::Offset,
    storage::{Log, RealStorageIo, StorageIo},
};

#[derive(Debug)]
struct CrashAfterSecondSegmentSync {
    real: RealStorageIo,
    segment_syncs: AtomicUsize,
}

#[derive(Debug)]
struct CrashAtBoundary {
    real: RealStorageIo,
    boundary: String,
    calls: AtomicUsize,
}

impl CrashAtBoundary {
    fn after(&self, boundary: &str) {
        if self.boundary == boundary && self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
            std::process::abort();
        }
    }
}

impl StorageIo for CrashAtBoundary {
    fn write_segment(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
        self.real.write_segment(file, bytes)?;
        self.after("segment-write");
        Ok(())
    }
    fn sync_segment(&self, file: &File) -> io::Result<()> {
        self.real.sync_segment(file)?;
        self.after("segment-sync");
        Ok(())
    }
    fn truncate_segment(&self, file: &File, length: u64) -> io::Result<()> {
        self.real.truncate_segment(file, length)
    }
    fn write_checkpoint(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
        self.real.write_checkpoint(file, bytes)?;
        self.after("checkpoint-write");
        Ok(())
    }
    fn sync_checkpoint(&self, file: &File) -> io::Result<()> {
        self.real.sync_checkpoint(file)?;
        self.after("checkpoint-sync");
        Ok(())
    }
    fn rename_checkpoint(&self, temporary: &Path, final_path: &Path) -> io::Result<()> {
        self.real.rename_checkpoint(temporary, final_path)?;
        self.after("checkpoint-rename");
        Ok(())
    }
    fn sync_directory(&self, directory: &Path) -> io::Result<()> {
        self.real.sync_directory(directory)?;
        self.after("directory-sync");
        Ok(())
    }
}

impl StorageIo for CrashAfterSecondSegmentSync {
    fn write_segment(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
        self.real.write_segment(file, bytes)
    }

    fn sync_segment(&self, file: &File) -> io::Result<()> {
        self.real.sync_segment(file)?;
        if self.segment_syncs.fetch_add(1, Ordering::SeqCst) == 1 {
            std::process::abort();
        }
        Ok(())
    }

    fn truncate_segment(&self, file: &File, length: u64) -> io::Result<()> {
        self.real.truncate_segment(file, length)
    }

    fn write_checkpoint(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
        self.real.write_checkpoint(file, bytes)
    }

    fn sync_checkpoint(&self, file: &File) -> io::Result<()> {
        self.real.sync_checkpoint(file)
    }

    fn rename_checkpoint(&self, temporary: &Path, final_path: &Path) -> io::Result<()> {
        self.real.rename_checkpoint(temporary, final_path)
    }

    fn sync_directory(&self, directory: &Path) -> io::Result<()> {
        self.real.sync_directory(directory)
    }
}

fn temp_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("serapendiya-crash-{nonce}"))
}

#[test]
fn lock_child() {
    let Some(directory) = std::env::var_os("SERAPENDIYA_LOCK_CHILD") else {
        return;
    };
    assert!(Log::open(PathBuf::from(directory)).is_err());
}

#[test]
fn data_directory_lock_excludes_another_process() {
    let directory = temp_dir();
    let log = Log::open(&directory).unwrap();
    let status = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("lock_child")
        .env("SERAPENDIYA_LOCK_CHILD", &directory)
        .status()
        .unwrap();
    assert!(status.success());
    drop(log);
    let reopened = Log::open(&directory).unwrap();
    drop(reopened);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn boundary_crash_child() {
    let Some(directory) = std::env::var_os("SERAPENDIYA_BOUNDARY_DIR") else {
        return;
    };
    let boundary = std::env::var("SERAPENDIYA_BOUNDARY").unwrap();
    let mut log = Log::open_with_io(
        PathBuf::from(directory),
        Box::new(CrashAtBoundary {
            real: RealStorageIo,
            boundary,
            calls: AtomicUsize::new(0),
        }),
    )
    .unwrap();
    log.append(b"acknowledged").unwrap();
    let _ = log.append(b"interrupted");
    panic!("requested crash boundary was not reached");
}

#[test]
fn acknowledged_record_survives_process_abort_at_each_append_boundary() {
    for boundary in [
        "segment-write",
        "segment-sync",
        "checkpoint-write",
        "checkpoint-sync",
        "checkpoint-rename",
        "directory-sync",
    ] {
        let directory = temp_dir();
        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "boundary_crash_child"])
            .env("SERAPENDIYA_BOUNDARY_DIR", &directory)
            .env("SERAPENDIYA_BOUNDARY", boundary)
            .status()
            .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(6), "boundary: {boundary}");
        }
        #[cfg(not(unix))]
        assert!(!status.success(), "boundary: {boundary}");
        let mut log = Log::open(&directory).unwrap();
        assert_eq!(
            log.read_batch(Offset::ZERO).unwrap().unwrap().records,
            b"acknowledged",
            "boundary: {boundary}"
        );
        let second = log.read_batch(Offset::new(1)).unwrap();
        if matches!(boundary, "checkpoint-rename" | "directory-sync") {
            assert_eq!(
                second.unwrap().records,
                b"interrupted",
                "boundary: {boundary}"
            );
        } else {
            assert_eq!(second, None, "boundary: {boundary}");
        }
        drop(log);
        fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn crash_child() {
    let Some(directory) = std::env::var_os("SERAPENDIYA_CRASH_CHILD") else {
        return;
    };
    let mut log = Log::open_with_io(
        PathBuf::from(directory),
        Box::new(CrashAfterSecondSegmentSync {
            real: RealStorageIo,
            segment_syncs: AtomicUsize::new(0),
        }),
    )
    .unwrap();
    log.append(b"certified").unwrap();
    let _ = log.append(b"uncertified-after-crash");
}

#[test]
fn process_crash_recovers_certified_prefix() {
    let directory = temp_dir();
    let status = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("crash_child")
        .arg("--nocapture")
        .env("SERAPENDIYA_CRASH_CHILD", &directory)
        .status()
        .unwrap();
    assert!(
        !status.success(),
        "crash child unexpectedly exited successfully"
    );

    let mut log = Log::open(&directory).unwrap();
    assert_eq!(log.batch_count(), 1);
    assert_eq!(log.next_offset(), Offset::new(1));
    assert_eq!(
        log.read_batch(Offset::ZERO).unwrap().unwrap().records,
        b"certified"
    );
    assert_eq!(log.read_batch(Offset::new(1)).unwrap(), None);
    assert_eq!(
        fs::metadata(directory.join("segment-000000.log"))
            .unwrap()
            .len(),
        log.certified_len()
    );
    fs::remove_dir_all(directory).unwrap();
}
