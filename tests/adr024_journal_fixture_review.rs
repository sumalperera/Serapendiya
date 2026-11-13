//! Fixture-specific verifier; this is not the proposed recovery implementation.
#![cfg(unix)]
use std::{collections::BTreeMap, fs, path::Path};

fn u32be(b: &[u8]) -> u32 {
    u32::from_be_bytes(b.try_into().unwrap())
}
fn u64be(b: &[u8]) -> u64 {
    u64::from_be_bytes(b.try_into().unwrap())
}
fn checksum(b: &[u8]) -> bool {
    b.len() >= 4 && crc32c::crc32c(&b[..b.len() - 4]) == u32be(&b[b.len() - 4..])
}

// Fixture-focused replay, independently written from the Python generator.
// This does not classify a directory, count frames in every append range, or
// establish any durability outcome; frame count is checked for these examples.
const MAX_JOURNAL_BYTES: u64 = 16 * 1024 * 1024; // Candidate bound in ADR-024.
type SegmentEntry = (u64, u64, u64);
fn replay_fixture(journal: &[u8]) -> Option<(u64, Vec<SegmentEntry>)> {
    if journal.len() < 40 || &journal[..8] != b"SRJ1\x01\0\0\0" || !checksum(&journal[..40]) {
        return None;
    }
    let mut position = 40usize;
    let mut sequence: Option<u64> = None;
    let mut entries: Vec<SegmentEntry> = Vec::new();
    while position < journal.len() {
        let header = journal.get(position..position.checked_add(32)?)?;
        if &header[..5] != b"SRJR\x01" || header[6..8] != [0, 0] || !checksum(header) {
            return None;
        }
        let size = u32be(&header[8..12]) as usize;
        if size < 32 || size != 32usize.checked_add(u32be(&header[20..24]) as usize)? {
            return None;
        }
        let end = position.checked_add(size)?;
        let payload = journal.get(position + 32..end)?;
        if crc32c::crc32c(payload) != u32be(&header[24..28]) {
            return None;
        }
        let seq = u64be(&header[12..20]);
        if let Some(previous) = sequence {
            if seq != previous.checked_add(1)? {
                return None;
            }
        } else if header[5] != 1 {
            return None;
        }
        match header[5] {
            1 if sequence.is_none() && payload.len() >= 16 => {
                let next = u64be(&payload[..8]);
                let count = u32be(&payload[8..12]) as usize;
                if count == 0
                    || count > 4096
                    || payload[12..16] != [0; 4]
                    || payload.len() != 16usize.checked_add(count.checked_mul(24)?)?
                {
                    return None;
                }
                let mut expected = 0;
                for (i, entry) in payload[16..].chunks_exact(24).enumerate() {
                    let base = u64be(&entry[..8]);
                    let end = u64be(&entry[8..16]);
                    let len = u64be(&entry[16..]);
                    if base != expected
                        || end < base
                        || ((end == base) != (len == 0))
                        || (i + 1 < count && end == base)
                    {
                        return None;
                    }
                    entries.push((base, end, len));
                    expected = end;
                }
                if expected != next {
                    return None;
                }
            }
            2 if payload.len() == 40 && !entries.is_empty() => {
                let active = entries.last_mut()?;
                let fields: Vec<u64> = payload.chunks_exact(8).map(u64be).collect();
                if active.0 != fields[0]
                    || active.2 != fields[1]
                    || active.1 != fields[3]
                    || fields[2] <= fields[1]
                    || fields[4] <= fields[3]
                {
                    return None;
                }
                active.2 = fields[2];
                active.1 = fields[4];
            }
            3 if payload.len() == 16 && !entries.is_empty() => {
                if entries.len() >= 4096 {
                    return None;
                }
                let active = *entries.last()?;
                let next = u64be(&payload[8..]);
                if active.0 != u64be(&payload[..8]) || active.0 == active.1 || next != active.1 {
                    return None;
                }
                entries.push((next, next, 0));
            }
            _ => return None,
        }
        sequence = Some(seq);
        position = end;
    }
    Some((sequence?, entries))
}

// Verify a checkpoint and the active snapshot framing for this fixture family.
// It intentionally does not classify files or provide a general journal parser.
fn checkpoint_and_active_valid(dir: &Path) -> bool {
    let Ok(cp) = fs::read(dir.join("journal-checkpoint")) else {
        return false;
    };
    if cp.len() != 96 || &cp[..8] != b"SRJC\x02\0\0\0" || !checksum(&cp) {
        return false;
    }
    let epoch = u64be(&cp[24..32]);
    let Ok(active) = fs::read(dir.join(format!("journal-{epoch:020}.log"))) else {
        return false;
    };
    let raw_len = u64be(&cp[32..40]);
    if !(40..=MAX_JOURNAL_BYTES).contains(&raw_len) {
        return false;
    }
    let Ok(len) = usize::try_from(raw_len) else {
        return false;
    };
    if len > active.len() {
        return false;
    }
    let active = &active[..len];
    if active.len() < 72
        || &active[..8] != b"SRJ1\x01\0\0\0"
        || cp[8..24] != active[8..24]
        || u64be(&active[24..32]) != epoch
        || u32be(&active[32..36]) != 40
        || !checksum(&active[..40])
    {
        return false;
    }
    let record = &active[40..];
    if &record[..8] != b"SRJR\x01\x01\0\0"
        || u32be(&record[8..12]) as usize != record.len()
        || u32be(&record[20..24]) as usize != record.len() - 32
        || !checksum(&record[..32])
        || crc32c::crc32c(&record[32..]) != u32be(&record[24..28])
        || u64be(&record[12..20]) != u64be(&cp[40..48])
        || record.len() < 48
        || u64be(&record[32..40]) != u64be(&cp[48..56])
    {
        return false;
    }
    let count = u32be(&record[40..44]) as usize;
    count > 0 && count <= 4096 && record.len() == 48 + 24 * count
}

fn snapshot(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    fs::read_dir(dir)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (
                entry.file_name().into_string().unwrap(),
                fs::read(entry.path()).unwrap(),
            )
        })
        .collect()
}

#[test]
#[ignore = "requires explicitly generated disposable fixture directories"]
fn multi_epoch_fixture_checks_and_old_reader_refusal() {
    let root = std::path::PathBuf::from(
        std::env::var_os("ADR024_JOURNAL_FIXTURES")
            .expect("set ADR024_JOURNAL_FIXTURES to a disposable generated fixture directory"),
    );
    for name in [
        "rotation-with-retired",
        "rotation-after-cleanup",
        "stale-temporary",
        "corrupt-retired",
        "journal-and-segment-suffix",
        "valid-uncertified-commit",
        "retired-length-mismatch",
        "certified-segment-short",
    ] {
        assert!(checkpoint_and_active_valid(&root.join(name)), "{name}");
    }
    for name in [
        "old-authority-with-orphan",
        "missing-active-journal",
        "bad-checkpoint-crc",
        "short-active",
        "checkpoint-mid-record",
    ] {
        // The old authority's journal has multiple records; this fixture-specific
        // snapshot-only verifier intentionally does not parse it.
        assert!(!checkpoint_and_active_valid(&root.join(name)), "{name}");
    }
    let old =
        fs::read(root.join("rotation-with-retired/journal-00000000000000000000.log")).unwrap();
    let cp = fs::read(root.join("rotation-with-retired/journal-checkpoint")).unwrap();
    let new =
        fs::read(root.join("rotation-with-retired/journal-00000000000000000001.log")).unwrap();
    let (old_seq, old_state) = replay_fixture(&old).unwrap();
    let (new_seq, new_state) = replay_fixture(&new).unwrap();
    assert_eq!((old_seq, new_seq), (3, 4));
    assert_eq!(old_state, new_state);
    for (base, end, length) in old_state {
        assert_eq!(end, base + 1);
        let frame = fs::read(
            root.join("rotation-with-retired")
                .join(format!("segment-{base:020}.log")),
        )
        .unwrap();
        assert_eq!(frame.len() as u64, length);
        assert_eq!(&frame[..4], b"SRB1");
        assert_eq!(u64be(&frame[12..20]), base);
        assert_eq!(crc32c::crc32c(&frame[..28]), u32be(&frame[28..32]));
        assert_eq!(crc32c::crc32c(&frame[32..]), u32be(&frame[24..28]));
        assert_eq!(crc32c::crc32c(&frame[53..]), u32be(&frame[49..53]));
    }
    assert_eq!(u64be(&cp[68..76]), old.len() as u64);
    assert_eq!(u32be(&cp[84..88]), crc32c::crc32c(&old));
    let damaged = fs::read(root.join("corrupt-retired/journal-00000000000000000000.log")).unwrap();
    assert_ne!(u32be(&cp[84..88]), crc32c::crc32c(&damaged));
    assert!(replay_fixture(&damaged).is_none());
    let suffix_dir = root.join("valid-uncertified-commit");
    let suffix_cp = fs::read(suffix_dir.join("journal-checkpoint")).unwrap();
    let suffix_journal = fs::read(suffix_dir.join("journal-00000000000000000001.log")).unwrap();
    let certified = usize::try_from(u64be(&suffix_cp[32..40])).unwrap();
    assert_eq!(replay_fixture(&suffix_journal[..certified]).unwrap().0, 4);
    let (full_seq, full_state) = replay_fixture(&suffix_journal).unwrap();
    assert_eq!(full_seq, 5);
    assert_eq!(suffix_journal.len() - certified, 72);
    let active = fs::read(suffix_dir.join("segment-00000000000000000001.log")).unwrap();
    assert_eq!(full_state.last().unwrap().2, active.len() as u64);
    assert_eq!(full_state.last().unwrap().1, 3);
    let prefix_len = usize::try_from(old_state_for_active_length(&root)).unwrap();
    let suffix_frame = &active[prefix_len..];
    assert_eq!(u64be(&suffix_frame[12..20]), 2);
    assert_eq!(u64be(&suffix_frame[32..40]), 2);
    assert_eq!(
        crc32c::crc32c(&suffix_frame[..28]),
        u32be(&suffix_frame[28..32])
    );
    assert_eq!(
        crc32c::crc32c(&suffix_frame[32..]),
        u32be(&suffix_frame[24..28])
    );
    let mismatch_cp = fs::read(root.join("retired-length-mismatch/journal-checkpoint")).unwrap();
    assert!(checksum(&mismatch_cp));
    assert_ne!(u64be(&mismatch_cp[68..76]), old.len() as u64);
    let short =
        fs::read(root.join("certified-segment-short/segment-00000000000000000001.log")).unwrap();
    let certified_active_length = u64::try_from(
        fs::read(root.join("rotation-with-retired/segment-00000000000000000001.log"))
            .unwrap()
            .len(),
    )
    .unwrap();
    assert!(u64::try_from(short.len()).unwrap() < certified_active_length);
    for name in [
        "rotation-with-retired",
        "rotation-after-cleanup",
        "old-authority-with-orphan",
        "stale-temporary",
        "missing-active-journal",
        "bad-checkpoint-crc",
        "corrupt-retired",
        "short-active",
        "journal-and-segment-suffix",
        "valid-uncertified-commit",
        "checkpoint-mid-record",
        "retired-length-mismatch",
        "certified-segment-short",
    ] {
        let dir = root.join(name);
        let before = snapshot(&dir);
        assert!(serapendiya::storage::Log::open(&dir).is_err(), "{name}");
        assert_eq!(snapshot(&dir), before, "old reader changed {name}");
    }
}

fn old_state_for_active_length(root: &Path) -> u64 {
    let new =
        fs::read(root.join("rotation-with-retired/journal-00000000000000000001.log")).unwrap();
    replay_fixture(&new).unwrap().1.last().unwrap().2
}
