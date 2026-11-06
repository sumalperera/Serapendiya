//! A small crash-conscious append log for the first storage gate.
//!
//! The segment contains Kafka RecordBatch v2-shaped frames. The `records` input
//! is kept opaque here; record encoding and protocol admission remain outside
//! this bounded storage slice. The checkpoint certifies only bytes covered by a
//! successful sync and atomic checkpoint update.

use std::{
    convert::TryInto,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::{
    domain::Offset,
    error::{Error, Result},
};

const MAGIC: &[u8; 4] = b"SRLG";
const CHECKPOINT_VERSION: u8 = 1;
const HEADER_LEN: usize = 61;
const FIXED_BATCH_REMAINDER: usize = 49;
const CHECKPOINT_LEN: usize = 36;

#[derive(Debug)]
pub struct Log {
    directory: PathBuf,
    segment: File,
    certified_len: u64,
    next_offset: Offset,
    generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Checkpoint {
    certified_len: u64,
    next_offset: Offset,
    generation: u64,
}

impl Log {
    pub fn open(directory: impl AsRef<Path>) -> Result<Self> {
        let directory = directory.as_ref().to_owned();
        fs::create_dir_all(&directory)?;
        let checkpoint_path = directory.join("checkpoint");
        let checkpoint = if checkpoint_path.exists() {
            read_checkpoint(&checkpoint_path)?
        } else {
            Checkpoint {
                certified_len: 0,
                next_offset: Offset::ZERO,
                generation: 0,
            }
        };

        let segment_path = directory.join("segment-000000.log");
        let mut segment = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&segment_path)?;
        let actual_len = segment.metadata()?.len();
        if checkpoint.certified_len > actual_len {
            return Err(Error::InvalidCheckpoint(
                "certified length exceeds segment length".into(),
            ));
        }
        recover_certified_prefix(&mut segment, checkpoint)?;
        if actual_len > checkpoint.certified_len {
            segment.set_len(checkpoint.certified_len)?;
            segment.sync_all()?;
        }
        segment.seek(SeekFrom::End(0))?;
        Ok(Self {
            directory,
            segment,
            certified_len: checkpoint.certified_len,
            next_offset: checkpoint.next_offset,
            generation: checkpoint.generation,
        })
    }

    pub fn append(&mut self, records: &[u8]) -> Result<Offset> {
        let records_len = records.len();
        let batch_length = FIXED_BATCH_REMAINDER
            .checked_add(records_len)
            .ok_or_else(|| Error::InvalidBatch("batch length overflow".into()))?;
        let batch_length_i32: i32 = batch_length
            .try_into()
            .map_err(|_| Error::InvalidBatch("batch exceeds signed 32-bit length".into()))?;
        let base_offset = self.next_offset;
        let frame = encode_batch(base_offset, batch_length_i32, records);
        self.segment.seek(SeekFrom::Start(self.certified_len))?;
        self.segment.write_all(&frame)?;
        self.segment.sync_all()?;

        let new_checkpoint = Checkpoint {
            certified_len: self.certified_len + frame.len() as u64,
            next_offset: base_offset
                .checked_next()
                .ok_or_else(|| Error::InvalidBatch("offset overflow".into()))?,
            generation: self
                .generation
                .checked_add(1)
                .ok_or_else(|| Error::InvalidCheckpoint("generation overflow".into()))?,
        };
        write_checkpoint(&self.directory, new_checkpoint)?;
        self.certified_len = new_checkpoint.certified_len;
        self.next_offset = new_checkpoint.next_offset;
        self.generation = new_checkpoint.generation;
        Ok(base_offset)
    }

    pub fn certified_len(&self) -> u64 {
        self.certified_len
    }
    pub fn next_offset(&self) -> Offset {
        self.next_offset
    }
}

fn encode_batch(base_offset: Offset, batch_length: i32, records: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(HEADER_LEN + records.len());
    frame.extend_from_slice(&(base_offset.get() as i64).to_be_bytes());
    frame.extend_from_slice(&batch_length.to_be_bytes());
    frame.extend_from_slice(&0i32.to_be_bytes());
    frame.push(2);
    frame.extend_from_slice(&0i32.to_be_bytes());
    frame.extend_from_slice(&0i16.to_be_bytes());
    frame.extend_from_slice(&0i32.to_be_bytes());
    frame.extend_from_slice(&0i64.to_be_bytes());
    frame.extend_from_slice(&0i64.to_be_bytes());
    frame.extend_from_slice(&(-1i64).to_be_bytes());
    frame.extend_from_slice(&(-1i16).to_be_bytes());
    frame.extend_from_slice(&(-1i32).to_be_bytes());
    frame.extend_from_slice(&1i32.to_be_bytes());
    frame.extend_from_slice(records);
    let crc = crc32c(&frame[21..]);
    frame[17..21].copy_from_slice(&(crc as i32).to_be_bytes());
    frame
}

fn recover_certified_prefix(segment: &mut File, checkpoint: Checkpoint) -> Result<()> {
    segment.seek(SeekFrom::Start(0))?;
    let mut position = 0u64;
    let mut expected_offset = Offset::ZERO;
    while position < checkpoint.certified_len {
        let frame = read_frame(segment, position, checkpoint.certified_len)?;
        if frame.base_offset != expected_offset {
            return Err(Error::CertifiedCorruption {
                position,
                reason: "base offset is not contiguous".into(),
            });
        }
        validate_frame(&frame, position)?;
        position += frame.bytes as u64;
        expected_offset =
            expected_offset
                .checked_next()
                .ok_or_else(|| Error::CertifiedCorruption {
                    position,
                    reason: "offset overflow".into(),
                })?;
    }
    if position != checkpoint.certified_len || expected_offset != checkpoint.next_offset {
        return Err(Error::InvalidCheckpoint(
            "checkpoint does not match certified batches".into(),
        ));
    }
    Ok(())
}

struct Frame {
    base_offset: Offset,
    bytes: usize,
    data: Vec<u8>,
}

fn read_frame(file: &mut File, position: u64, limit: u64) -> Result<Frame> {
    let mut prefix = [0u8; 12];
    file.read_exact(&mut prefix)
        .map_err(|error| certified_io(position, error))?;
    let base = i64::from_be_bytes(prefix[0..8].try_into().unwrap());
    let length = i32::from_be_bytes(prefix[8..12].try_into().unwrap());
    if base < 0 || length < FIXED_BATCH_REMAINDER as i32 {
        return Err(Error::CertifiedCorruption {
            position,
            reason: "negative or undersized batch header".into(),
        });
    }
    let bytes = HEADER_LEN
        .checked_add(length as usize - FIXED_BATCH_REMAINDER)
        .ok_or_else(|| Error::CertifiedCorruption {
            position,
            reason: "batch length overflow".into(),
        })?;
    if position
        .checked_add(bytes as u64)
        .is_none_or(|end| end > limit)
    {
        return Err(Error::CertifiedCorruption {
            position,
            reason: "batch crosses certified boundary".into(),
        });
    }
    let mut data = vec![0u8; bytes];
    data[..12].copy_from_slice(&prefix);
    file.read_exact(&mut data[12..])
        .map_err(|error| certified_io(position, error))?;
    Ok(Frame {
        base_offset: Offset::new(base as u64),
        bytes,
        data,
    })
}

fn validate_frame(frame: &Frame, position: u64) -> Result<()> {
    if frame.data[16] != 2 {
        return Err(Error::CertifiedCorruption {
            position,
            reason: "unsupported batch magic".into(),
        });
    }
    let expected = i32::from_be_bytes(frame.data[17..21].try_into().unwrap()) as u32;
    let actual = crc32c(&frame.data[21..]);
    if expected != actual {
        return Err(Error::CertifiedCorruption {
            position,
            reason: "CRC32C mismatch".into(),
        });
    }
    Ok(())
}

fn certified_io(position: u64, error: io::Error) -> Error {
    Error::CertifiedCorruption {
        position,
        reason: error.to_string(),
    }
}

fn read_checkpoint(path: &Path) -> Result<Checkpoint> {
    let bytes = fs::read(path)?;
    if bytes.len() != CHECKPOINT_LEN || &bytes[..4] != MAGIC || bytes[4] != CHECKPOINT_VERSION {
        return Err(Error::InvalidCheckpoint("bad header or length".into()));
    }
    let expected = u32::from_be_bytes(bytes[32..36].try_into().unwrap());
    if crc32c(&bytes[..32]) != expected {
        return Err(Error::InvalidCheckpoint("checksum mismatch".into()));
    }
    let certified_len = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
    let next_offset = u64::from_be_bytes(bytes[16..24].try_into().unwrap());
    let generation = u64::from_be_bytes(bytes[24..32].try_into().unwrap());
    Ok(Checkpoint {
        certified_len,
        next_offset: Offset::new(next_offset),
        generation,
    })
}

fn write_checkpoint(directory: &Path, checkpoint: Checkpoint) -> Result<()> {
    let mut bytes = [0u8; CHECKPOINT_LEN];
    bytes[..4].copy_from_slice(MAGIC);
    bytes[4] = CHECKPOINT_VERSION;
    bytes[8..16].copy_from_slice(&checkpoint.certified_len.to_be_bytes());
    bytes[16..24].copy_from_slice(&checkpoint.next_offset.get().to_be_bytes());
    bytes[24..32].copy_from_slice(&checkpoint.generation.to_be_bytes());
    let checksum = crc32c(&bytes[..32]);
    bytes[32..36].copy_from_slice(&checksum.to_be_bytes());

    let temporary = directory.join("checkpoint.tmp");
    let final_path = directory.join("checkpoint");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, &final_path)?;
    File::open(directory)?.sync_all()?;
    Ok(())
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in bytes {
        crc ^= byte as u32;
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f63b78 & (!((crc & 1).wrapping_sub(1))));
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("serapendiya-storage-{nonce}"))
    }

    #[test]
    fn crc32c_known_vector() {
        assert_eq!(crc32c(b"123456789"), 0xe3069283);
    }

    #[test]
    fn append_reopen_and_recover() {
        let dir = temp_dir();
        let offset = {
            let mut log = Log::open(&dir).unwrap();
            log.append(b"one").unwrap()
        };
        assert_eq!(offset, Offset::ZERO);
        let log = Log::open(&dir).unwrap();
        assert_eq!(log.next_offset(), Offset::new(1));
        assert!(log.certified_len() > 0);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rejects_corruption_below_certified_boundary() {
        let dir = temp_dir();
        {
            let mut log = Log::open(&dir).unwrap();
            log.append(b"one").unwrap();
        }
        let path = dir.join("segment-000000.log");
        let mut file = OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start(24)).unwrap();
        file.write_all(&[1]).unwrap();
        file.sync_all().unwrap();
        assert!(matches!(
            Log::open(&dir),
            Err(Error::CertifiedCorruption { .. })
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn discards_uncertified_tail() {
        let dir = temp_dir();
        let certified_len = {
            let mut log = Log::open(&dir).unwrap();
            log.append(b"one").unwrap();
            log.certified_len()
        };
        let path = dir.join("segment-000000.log");
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"uncertified tail").unwrap();
        file.sync_all().unwrap();
        let log = Log::open(&dir).unwrap();
        assert_eq!(log.certified_len(), certified_len);
        assert_eq!(fs::metadata(path).unwrap().len(), certified_len);
        fs::remove_dir_all(dir).unwrap();
    }
}
