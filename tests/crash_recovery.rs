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
