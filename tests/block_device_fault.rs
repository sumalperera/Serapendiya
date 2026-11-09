//! Runs only on the disposable filesystem created by scripts/run-g1-block-tests.sh.

use serapendiya::{domain::Offset, storage::Log};
use std::{
    env,
    fs::{self, File},
    io::{ErrorKind, Write},
    path::PathBuf,
};

#[test]
#[ignore = "requires a disposable, size-limited ext4 filesystem"]
fn actual_full_ext4_preserves_acknowledged_record() {
    let root = PathBuf::from(env::var_os("SERAPENDIYA_BLOCK_TEST_DIR").expect("test directory"));
    assert_eq!(
        fs::read(root.join(".serapendiya-disposable")).unwrap(),
        b"disposable\n"
    );
    let dir = root.join("log");
    let mut log = Log::open(&dir).unwrap();
    assert_eq!(log.append(b"acknowledged").unwrap(), Offset::ZERO);
    let mut filler = File::create(root.join("filler")).unwrap();
    let block = vec![0u8; 1024 * 1024];
    loop {
        match filler.write_all(&block) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::StorageFull => break,
            Err(error) => panic!("unexpected fill error: {error}"),
        }
    }
    assert!(log.append(&vec![0u8; 1024 * 1024]).is_err());
    assert!(log.append(b"retry").is_err());
    drop(log);
    drop(filler);
    fs::remove_file(root.join("filler")).unwrap();
    let mut recovered = Log::open(&dir).unwrap();
    assert_eq!(
        recovered.read_batch(Offset::ZERO).unwrap().unwrap().records,
        b"acknowledged"
    );
    assert_eq!(recovered.read_batch(Offset::new(1)).unwrap(), None);
}

#[test]
#[ignore = "requires a disposable device-mapper device"]
fn block_error_during_append() {
    use std::process::Command;
    let root = PathBuf::from(env::var_os("SERAPENDIYA_BLOCK_TEST_DIR").expect("test directory"));
    assert_eq!(
        fs::read(root.join(".serapendiya-disposable")).unwrap(),
        b"disposable\n"
    );
    let name = env::var("SERAPENDIYA_BLOCK_TEST_DM_NAME").unwrap();
    let loop_device = env::var("SERAPENDIYA_BLOCK_TEST_LOOP").unwrap();
    let sectors: u64 = env::var("SERAPENDIYA_BLOCK_TEST_SECTORS")
        .unwrap()
        .parse()
        .unwrap();
    assert!(name.starts_with("serapendiya-g1-test-"));
    assert!(loop_device.starts_with("/dev/loop"));
    let apply = |table: &str| {
        for args in [
            vec!["suspend", "--noflush", name.as_str()],
            vec!["reload", name.as_str(), "--table", table],
            vec!["resume", name.as_str()],
        ] {
            assert!(
                Command::new("dmsetup")
                    .args(args)
                    .status()
                    .unwrap()
                    .success()
            );
        }
    };
    let mut log = Log::open(root.join("eio-log")).unwrap();
    log.append(b"acknowledged before device failure").unwrap();
    apply(&format!("0 {sectors} error"));
    let result = log.append(b"during device failure");
    apply(&format!("0 {sectors} linear {loop_device} 0"));
    assert!(result.is_err(), "I/O failure must not be acknowledged");
    drop(log);
}

#[test]
#[ignore = "requires the remounted disposable filesystem after block_error_during_append"]
fn block_error_recovered() {
    let root = PathBuf::from(env::var_os("SERAPENDIYA_BLOCK_TEST_DIR").expect("test directory"));
    let mut log = Log::open(root.join("eio-log")).unwrap();
    assert_eq!(
        log.read_batch(Offset::ZERO).unwrap().unwrap().records,
        b"acknowledged before device failure"
    );
}
