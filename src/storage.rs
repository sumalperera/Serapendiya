//! A small crash-conscious append log for the first storage gate.
//!
//! Segments use an experimental Serapendiya container around a narrow Kafka
//! RecordBatch v2 shape. Append accepts opaque values and encodes a single
//! record; admission of client-produced batches is not implemented. The
//! checkpoint certifies bytes covered by sync and an atomic checkpoint update.

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
// Version 2 identifies the experimental container. Version 1 was used by the
// legacy Kafka-shaped segment and must never authorize truncation in this format.
const CHECKPOINT_VERSION: u8 = 2;
const HEADER_LEN: usize = 61;
const CONTAINER_HEADER_LEN: usize = 32;
const CONTAINER_MAGIC: &[u8; 4] = b"SRB1";
const FIXED_BATCH_REMAINDER: usize = 49;
const CHECKPOINT_LEN: usize = 36;
const INDEX_STRIDE: u64 = 16;
const MAX_GROUP_BATCHES: usize = 1024;
const MAX_GROUP_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug)]
pub struct Log {
    _lock: File,
    directory: PathBuf,
    segment: File,
    io: Box<dyn StorageIo>,
    certified_len: u64,
    next_offset: Offset,
    generation: u64,
    batch_count: u64,
    index: Vec<IndexEntry>,
    append_position: u64,
    visible_offset: Option<Offset>,
    durable_offset: Option<Offset>,
    terminal_error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredBatch {
    pub offset: Offset,
    pub records: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexEntry {
    pub offset: Offset,
    pub position: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogPositions {
    pub append_position: u64,
    pub visible_offset: Option<Offset>,
    pub durable_offset: Option<Offset>,
}

struct RecoverySummary {
    batch_count: u64,
    index: Vec<IndexEntry>,
}

/// Filesystem operations that define the durable boundary of the append log.
///
/// Production uses [`RealStorageIo`]. The seam exists so failure ordering can
/// be tested without pretending that an in-memory mock proves filesystem
/// crash-safety.
pub trait StorageIo: Send + Sync + std::fmt::Debug {
    fn write_segment(&self, file: &mut File, bytes: &[u8]) -> io::Result<()>;
    fn sync_segment(&self, file: &File) -> io::Result<()>;
    fn truncate_segment(&self, file: &File, length: u64) -> io::Result<()>;
    fn write_checkpoint(&self, file: &mut File, bytes: &[u8]) -> io::Result<()>;
    fn sync_checkpoint(&self, file: &File) -> io::Result<()>;
    fn rename_checkpoint(&self, temporary: &Path, final_path: &Path) -> io::Result<()>;
    fn sync_directory(&self, directory: &Path) -> io::Result<()>;
}

#[derive(Debug, Default)]
pub struct RealStorageIo;

impl StorageIo for RealStorageIo {
    fn write_segment(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
        file.write_all(bytes)
    }

    fn sync_segment(&self, file: &File) -> io::Result<()> {
        file.sync_all()
    }

    fn truncate_segment(&self, file: &File, length: u64) -> io::Result<()> {
        file.set_len(length)
    }

    fn write_checkpoint(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
        file.write_all(bytes)
    }

    fn sync_checkpoint(&self, file: &File) -> io::Result<()> {
        file.sync_all()
    }

    fn rename_checkpoint(&self, temporary: &Path, final_path: &Path) -> io::Result<()> {
        fs::rename(temporary, final_path)
    }

    fn sync_directory(&self, directory: &Path) -> io::Result<()> {
        File::open(directory)?.sync_all()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Checkpoint {
    certified_len: u64,
    next_offset: Offset,
    generation: u64,
}

impl Log {
    pub fn open(directory: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_io(directory, Box::new(RealStorageIo))
    }

    pub fn open_with_io(directory: impl AsRef<Path>, io: Box<dyn StorageIo>) -> Result<Self> {
        let directory = directory.as_ref().to_owned();
        if let Ok(metadata) = fs::symlink_metadata(&directory) {
            if !metadata.file_type().is_dir() {
                return Err(Error::InvalidCheckpoint(
                    "data directory is not a real directory".into(),
                ));
            }
        }
        fs::create_dir_all(&directory)?;
        let empty = validate_directory_entries(&directory)?;
        let lock_path = directory.join(".lock");
        let lock_exists = lock_path.try_exists()?;
        if !lock_exists && !empty {
            return Err(Error::InvalidCheckpoint(
                "unrecognized directory without a lock file; data preserved".into(),
            ));
        }
        // Sync every ancestor so newly created directory entries are durable too.
        let canonical_directory = fs::canonicalize(&directory)?;
        for ancestor in canonical_directory.ancestors() {
            File::open(ancestor)?.sync_all()?;
        }
        let mut lock_options = OpenOptions::new();
        lock_options.write(true);
        if !lock_exists {
            lock_options.create_new(true);
        }
        let lock = lock_options.open(lock_path)?;
        lock_directory(&lock)?;
        validate_directory_entries(&directory)?;
        let checkpoint_path = directory.join("checkpoint");
        let checkpoint_exists = checkpoint_path.exists();
        if !checkpoint_exists && directory.join("checkpoint.tmp").try_exists()? {
            return Err(Error::InvalidCheckpoint(
                "uncommitted checkpoint without durable identity; data preserved".into(),
            ));
        }
        let checkpoint = if checkpoint_exists {
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
            .create(!checkpoint_exists)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&segment_path)?;
        let actual_len = segment.metadata()?.len();
        if !checkpoint_exists && actual_len > 0 {
            return Err(Error::InvalidCheckpoint(
                "checkpoint is missing for a nonempty segment".into(),
            ));
        }
        if checkpoint.certified_len > 0 && actual_len > 0 {
            let mut magic = [0u8; 4];
            segment
                .read_exact(&mut magic)
                .map_err(|error| certified_io(0, error))?;
            if &magic != CONTAINER_MAGIC {
                return Err(Error::InvalidCheckpoint(
                    "unsupported or legacy segment format; data preserved".into(),
                ));
            }
        }
        if checkpoint.certified_len > actual_len {
            return Err(Error::InvalidCheckpoint(
                "certified length exceeds segment length".into(),
            ));
        }
        let recovery = recover_certified_prefix(&mut segment, checkpoint)?;
        if !checkpoint_exists {
            // A committed empty checkpoint precedes any append. A crash before
            // initialization completes cannot have acknowledged an append.
            io.sync_segment(&segment)?;
            write_checkpoint(&directory, checkpoint, io.as_ref())?;
        }
        if actual_len > checkpoint.certified_len {
            io.truncate_segment(&segment, checkpoint.certified_len)?;
            io.sync_segment(&segment)?;
        }
        segment.seek(SeekFrom::End(0))?;
        let durable_offset = checkpoint.next_offset.get().checked_sub(1).map(Offset::new);
        Ok(Self {
            _lock: lock,
            directory,
            segment,
            io,
            certified_len: checkpoint.certified_len,
            next_offset: checkpoint.next_offset,
            generation: checkpoint.generation,
            batch_count: recovery.batch_count,
            index: recovery.index,
            append_position: checkpoint.certified_len,
            visible_offset: durable_offset,
            durable_offset,
            terminal_error: None,
        })
    }

    /// Append and certify one value.
    ///
    /// A terminal I/O error has an unknown outcome: the value may be present
    /// after reopening. Retrying without application-level reconciliation or
    /// idempotency can produce duplicates.
    pub fn append(&mut self, records: &[u8]) -> Result<Offset> {
        self.append_group(&[records])?
            .into_iter()
            .next()
            .ok_or_else(|| Error::InvalidBatch("single append produced no offset".into()))
    }

    /// Append a group. A terminal I/O error may have an unknown outcome for
    /// the entire group; callers must reconcile before retrying.
    pub fn append_group(&mut self, batches: &[&[u8]]) -> Result<Vec<Offset>> {
        if let Some(reason) = &self.terminal_error {
            return Err(Error::StorageTerminal(reason.clone()));
        }
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        if batches.len() > MAX_GROUP_BATCHES {
            return Err(Error::InvalidBatch(
                "group batch count limit exceeded".into(),
            ));
        }
        let next_generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::InvalidCheckpoint("generation overflow".into()))?;

        let mut group_len = 0usize;
        for records in batches {
            group_len = group_len
                .checked_add(CONTAINER_HEADER_LEN + HEADER_LEN + 16)
                .and_then(|length| length.checked_add(records.len()))
                .ok_or_else(|| Error::InvalidBatch("group length overflow".into()))?;
            if group_len > MAX_GROUP_BYTES {
                return Err(Error::InvalidBatch("group byte limit exceeded".into()));
            }
        }

        let mut next_offset = self.next_offset;
        let mut offsets = Vec::with_capacity(batches.len());
        let mut frames = Vec::with_capacity(batches.len());
        let mut total_len = 0usize;
        for records in batches {
            let base_offset = next_offset;
            if base_offset.get() > i64::MAX as u64 {
                return Err(Error::InvalidBatch(
                    "offset exceeds signed 64-bit range".into(),
                ));
            }
            let frame = encode_container(base_offset, records)?;
            total_len = total_len
                .checked_add(frame.len())
                .ok_or_else(|| Error::InvalidBatch("group length overflow".into()))?;
            offsets.push(base_offset);
            frames.push(frame);
            next_offset = next_offset
                .checked_next()
                .ok_or_else(|| Error::InvalidBatch("offset overflow".into()))?;
        }
        let next_batch_count = self
            .batch_count
            .checked_add(batches.len() as u64)
            .ok_or_else(|| Error::InvalidBatch("batch count overflow".into()))?;
        let mut group = Vec::with_capacity(total_len);
        for frame in &frames {
            group.extend_from_slice(frame);
        }
        let append_position = self
            .certified_len
            .checked_add(group.len() as u64)
            .ok_or_else(|| Error::InvalidBatch("append position overflow".into()))?;
        self.segment.seek(SeekFrom::Start(self.certified_len))?;
        if let Err(error) = self.io.write_segment(&mut self.segment, &group) {
            return Err(self.terminal(error));
        }
        self.append_position = append_position;
        if let Err(error) = self.io.sync_segment(&self.segment) {
            return Err(self.terminal(error));
        }

        let new_checkpoint = Checkpoint {
            certified_len: append_position,
            next_offset,
            generation: next_generation,
        };
        if let Err(error) = write_checkpoint(&self.directory, new_checkpoint, self.io.as_ref()) {
            return Err(self.terminal(error));
        }
        self.certified_len = new_checkpoint.certified_len;
        self.next_offset = new_checkpoint.next_offset;
        self.generation = new_checkpoint.generation;
        self.durable_offset = offsets.last().copied();
        self.visible_offset = self.durable_offset;
        let mut position = self.certified_len - group.len() as u64;
        for (index, frame) in frames.iter().enumerate() {
            if (self.batch_count + index as u64) % INDEX_STRIDE == 0 {
                self.index.push(IndexEntry {
                    offset: offsets[index],
                    position,
                });
            }
            position += frame.len() as u64;
        }
        self.batch_count = next_batch_count;
        Ok(offsets)
    }

    fn terminal(&mut self, error: io::Error) -> Error {
        let reason = error.to_string();
        self.terminal_error = Some(reason.clone());
        Error::StorageTerminal(reason)
    }

    pub fn certified_len(&self) -> u64 {
        self.certified_len
    }
    pub fn next_offset(&self) -> Offset {
        self.next_offset
    }
    pub fn batch_count(&self) -> u64 {
        self.batch_count
    }
    pub fn index_entries(&self) -> &[IndexEntry] {
        &self.index
    }
    pub fn positions(&self) -> LogPositions {
        LogPositions {
            append_position: self.append_position,
            visible_offset: self.visible_offset,
            durable_offset: self.durable_offset,
        }
    }

    pub fn read_batch(&mut self, requested: Offset) -> Result<Option<StoredBatch>> {
        let index = self
            .index
            .partition_point(|entry| entry.offset <= requested);
        let start = index
            .checked_sub(1)
            .and_then(|i| self.index.get(i))
            .copied()
            .unwrap_or(IndexEntry {
                offset: Offset::ZERO,
                position: 0,
            });
        self.segment.seek(SeekFrom::Start(start.position))?;
        let mut position = start.position;
        let mut expected_offset = start.offset;
        while position < self.certified_len {
            let frame = read_frame(&mut self.segment, position, self.certified_len)?;
            if frame.base_offset != expected_offset {
                return Err(Error::CertifiedCorruption {
                    position,
                    reason: "base offset is not contiguous".into(),
                });
            }
            validate_frame(&frame, position)?;
            if frame.base_offset == requested {
                return Ok(Some(StoredBatch {
                    offset: frame.base_offset,
                    records: decode_record(&frame.data[HEADER_LEN..])
                        .map_err(|reason| Error::CertifiedCorruption { position, reason })?
                        .to_vec(),
                }));
            }
            if frame.base_offset > requested {
                return Ok(None);
            }
            position += frame.bytes as u64;
            expected_offset =
                expected_offset
                    .checked_next()
                    .ok_or_else(|| Error::CertifiedCorruption {
                        position,
                        reason: "offset overflow".into(),
                    })?;
        }
        Ok(None)
    }
}

fn validate_directory_entries(directory: &Path) -> Result<bool> {
    let mut empty = true;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        empty = false;
        let name = entry.file_name();
        if !matches!(
            name.to_str(),
            Some(".lock" | "checkpoint" | "checkpoint.tmp" | "segment-000000.log")
        ) || !entry.file_type()?.is_file()
        {
            return Err(Error::InvalidCheckpoint(
                "unrecognized directory entry; data preserved".into(),
            ));
        }
    }
    Ok(empty)
}

#[cfg(unix)]
fn lock_directory(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    unsafe extern "C" {
        fn flock(fd: i32, operation: i32) -> i32;
    }
    const LOCK_EX: i32 = 2;
    const LOCK_NB: i32 = 4;
    // The descriptor belongs to this live File. The OS releases the lock when
    // the File is dropped; the lock file itself must never be unlinked in use.
    if unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn lock_directory(_file: &File) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "directory locking is only supported on Unix",
    ))
}

fn put_varint(mut value: u32, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn encode_container(base_offset: Offset, value: &[u8]) -> Result<Vec<u8>> {
    let value_len = u32::try_from(value.len())
        .ok()
        .and_then(|n| n.checked_mul(2))
        .ok_or_else(|| Error::InvalidBatch("record too large".into()))?;
    let mut record = vec![0, 0, 0, 1]; // attributes, timestamp delta, offset delta, null key
    put_varint(value_len, &mut record);
    record.extend_from_slice(value);
    record.push(0); // no headers
    let record_len = u32::try_from(record.len())
        .ok()
        .and_then(|n| n.checked_mul(2))
        .ok_or_else(|| Error::InvalidBatch("record too large".into()))?;
    let mut records = Vec::with_capacity(record.len() + 5);
    put_varint(record_len, &mut records);
    records.extend_from_slice(&record);
    let batch_len = i32::try_from(FIXED_BATCH_REMAINDER + records.len())
        .map_err(|_| Error::InvalidBatch("batch too large".into()))?;
    let batch = encode_batch(base_offset, batch_len, &records);
    let mut header = [0u8; CONTAINER_HEADER_LEN];
    header[..4].copy_from_slice(CONTAINER_MAGIC);
    header[4] = 1;
    header[8..12].copy_from_slice(&(batch.len() as u32).to_be_bytes());
    header[12..20].copy_from_slice(&base_offset.get().to_be_bytes());
    header[20..24].copy_from_slice(&1u32.to_be_bytes());
    header[24..28].copy_from_slice(&crc32c(&batch).to_be_bytes());
    let checksum = crc32c(&header[..28]);
    header[28..32].copy_from_slice(&checksum.to_be_bytes());
    let mut frame = Vec::with_capacity(header.len() + batch.len());
    frame.extend_from_slice(&header);
    frame.extend_from_slice(&batch);
    Ok(frame)
}

fn get_varint(bytes: &[u8], pos: &mut usize) -> std::result::Result<u32, String> {
    let mut value = 0u32;
    for shift in (0..35).step_by(7) {
        let byte = *bytes.get(*pos).ok_or("truncated varint")?;
        *pos += 1;
        if shift == 28 && byte > 15 {
            return Err("varint overflow".into());
        }
        value |= u32::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err("varint overflow".into())
}

fn decode_record(bytes: &[u8]) -> std::result::Result<&[u8], String> {
    let mut pos = 0;
    let encoded_len = get_varint(bytes, &mut pos)?;
    if encoded_len & 1 != 0 || (encoded_len / 2) as usize != bytes.len() - pos {
        return Err("invalid record length".into());
    }
    if bytes.get(pos..pos + 4) != Some(&[0, 0, 0, 1][..]) {
        return Err("unsupported record attributes, offsets, or key".into());
    }
    pos += 4;
    let encoded_value_len = get_varint(bytes, &mut pos)?;
    if encoded_value_len & 1 != 0 {
        return Err("null or invalid value".into());
    }
    let end = pos
        .checked_add((encoded_value_len / 2) as usize)
        .ok_or("value length overflow")?;
    let value = bytes.get(pos..end).ok_or("truncated value")?;
    if bytes.get(end..) != Some(&[0][..]) {
        return Err("unsupported record headers".into());
    }
    Ok(value)
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

fn recover_certified_prefix(segment: &mut File, checkpoint: Checkpoint) -> Result<RecoverySummary> {
    segment.seek(SeekFrom::Start(0))?;
    let mut position = 0u64;
    let mut expected_offset = Offset::ZERO;
    let mut batch_count = 0u64;
    let mut index = Vec::new();
    while position < checkpoint.certified_len {
        let frame = read_frame(segment, position, checkpoint.certified_len)?;
        if frame.base_offset != expected_offset {
            return Err(Error::CertifiedCorruption {
                position,
                reason: "base offset is not contiguous".into(),
            });
        }
        validate_frame(&frame, position)?;
        if batch_count % INDEX_STRIDE == 0 {
            index.push(IndexEntry {
                offset: frame.base_offset,
                position,
            });
        }
        position += frame.bytes as u64;
        expected_offset =
            expected_offset
                .checked_next()
                .ok_or_else(|| Error::CertifiedCorruption {
                    position,
                    reason: "offset overflow".into(),
                })?;
        batch_count = batch_count
            .checked_add(1)
            .ok_or_else(|| Error::CertifiedCorruption {
                position,
                reason: "batch count overflow".into(),
            })?;
    }
    if position != checkpoint.certified_len || expected_offset != checkpoint.next_offset {
        return Err(Error::InvalidCheckpoint(
            "checkpoint does not match certified batches".into(),
        ));
    }
    Ok(RecoverySummary { batch_count, index })
}

struct Frame {
    base_offset: Offset,
    bytes: usize,
    data: Vec<u8>,
}

fn read_frame(file: &mut File, position: u64, limit: u64) -> Result<Frame> {
    let mut header = [0u8; CONTAINER_HEADER_LEN];
    file.read_exact(&mut header)
        .map_err(|error| certified_io(position, error))?;
    if &header[..4] != CONTAINER_MAGIC {
        return Err(Error::CertifiedCorruption {
            position,
            reason: "unsupported or legacy segment format; data preserved".into(),
        });
    }
    if crc32c(&header[..28]) != u32::from_be_bytes(header[28..32].try_into().unwrap())
        || header[4..8] != [1, 0, 0, 0]
        || u32::from_be_bytes(header[20..24].try_into().unwrap()) != 1
    {
        return Err(Error::CertifiedCorruption {
            position,
            reason: "invalid container header".into(),
        });
    }
    let base = u64::from_be_bytes(header[12..20].try_into().unwrap());
    let length = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
    if base > i64::MAX as u64
        || !(HEADER_LEN..=MAX_GROUP_BYTES - CONTAINER_HEADER_LEN).contains(&length)
    {
        return Err(Error::CertifiedCorruption {
            position,
            reason: "invalid container length or offset".into(),
        });
    }
    let bytes = CONTAINER_HEADER_LEN + length;
    if position
        .checked_add(bytes as u64)
        .is_none_or(|end| end > limit)
    {
        return Err(Error::CertifiedCorruption {
            position,
            reason: "batch crosses certified boundary".into(),
        });
    }
    let mut data = vec![0u8; length];
    file.read_exact(&mut data)
        .map_err(|error| certified_io(position, error))?;
    if crc32c(&data) != u32::from_be_bytes(header[24..28].try_into().unwrap())
        || i64::from_be_bytes(data[..8].try_into().unwrap()) != base as i64
        || i32::from_be_bytes(data[8..12].try_into().unwrap()) as i64 != length as i64 - 12
    {
        return Err(Error::CertifiedCorruption {
            position,
            reason: "container payload mismatch".into(),
        });
    }
    Ok(Frame {
        base_offset: Offset::new(base),
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
    if frame.data[21..27] != [0; 6]
        || frame.data[43..51] != (-1i64).to_be_bytes()
        || frame.data[51..53] != (-1i16).to_be_bytes()
        || frame.data[53..57] != (-1i32).to_be_bytes()
        || frame.data[57..61] != 1i32.to_be_bytes()
    {
        return Err(Error::CertifiedCorruption {
            position,
            reason: "unsupported Kafka batch shape".into(),
        });
    }
    decode_record(&frame.data[HEADER_LEN..])
        .map_err(|reason| Error::CertifiedCorruption { position, reason })?;
    Ok(())
}

fn certified_io(position: u64, error: io::Error) -> Error {
    if error.kind() == io::ErrorKind::UnexpectedEof {
        Error::CertifiedCorruption {
            position,
            reason: error.to_string(),
        }
    } else {
        Error::StorageIo(error)
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

fn write_checkpoint(
    directory: &Path,
    checkpoint: Checkpoint,
    io: &dyn StorageIo,
) -> io::Result<()> {
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
    io.write_checkpoint(&mut file, &bytes)?;
    io.sync_checkpoint(&file)?;
    io.rename_checkpoint(&temporary, &final_path)?;
    io.sync_directory(directory)?;
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
    use std::{
        sync::Arc,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[derive(Clone, Copy, Debug)]
    enum FailurePoint {
        SegmentWrite,
        PartialSegmentWrite,
        SegmentSync,
        CheckpointWrite,
        CheckpointSync,
        CheckpointRename,
        DirectorySync,
    }

    #[derive(Debug)]
    struct FaultIo {
        point: FailurePoint,
    }

    impl FaultIo {
        fn error(point: FailurePoint) -> io::Error {
            io::Error::other(format!("injected {point:?}"))
        }
    }

    impl StorageIo for FaultIo {
        fn write_segment(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
            match self.point {
                FailurePoint::SegmentWrite => Err(Self::error(self.point)),
                FailurePoint::PartialSegmentWrite => {
                    file.write_all(&bytes[..bytes.len() / 2])?;
                    Err(Self::error(self.point))
                }
                _ => file.write_all(bytes),
            }
        }

        fn sync_segment(&self, file: &File) -> io::Result<()> {
            if matches!(self.point, FailurePoint::SegmentSync) {
                Err(Self::error(self.point))
            } else {
                file.sync_all()
            }
        }

        fn truncate_segment(&self, file: &File, length: u64) -> io::Result<()> {
            file.set_len(length)
        }

        fn write_checkpoint(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
            if matches!(self.point, FailurePoint::CheckpointWrite) {
                Err(Self::error(self.point))
            } else {
                file.write_all(bytes)
            }
        }

        fn sync_checkpoint(&self, file: &File) -> io::Result<()> {
            if matches!(self.point, FailurePoint::CheckpointSync) {
                Err(Self::error(self.point))
            } else {
                file.sync_all()
            }
        }

        fn rename_checkpoint(&self, temporary: &Path, final_path: &Path) -> io::Result<()> {
            if matches!(self.point, FailurePoint::CheckpointRename) {
                Err(Self::error(self.point))
            } else {
                fs::rename(temporary, final_path)
            }
        }

        fn sync_directory(&self, directory: &Path) -> io::Result<()> {
            if matches!(self.point, FailurePoint::DirectorySync) {
                Err(Self::error(self.point))
            } else {
                File::open(directory)?.sync_all()
            }
        }
    }

    #[derive(Debug)]
    struct CountingIo {
        segment_syncs: Arc<AtomicU64>,
        checkpoint_syncs: Arc<AtomicU64>,
    }

    #[derive(Debug)]
    struct SimulatedDisk {
        directory: PathBuf,
        stop_after: &'static str,
        stable_segment: Vec<u8>,
        stable_checkpoint: Vec<u8>,
        synced_temporary: Option<Vec<u8>>,
    }

    #[derive(Debug)]
    struct PowerCutIo(Arc<std::sync::Mutex<SimulatedDisk>>);

    impl PowerCutIo {
        fn stop(&self, point: &str) -> io::Result<()> {
            if self.0.lock().unwrap().stop_after == point {
                Err(io::Error::other("simulated power cut"))
            } else {
                Ok(())
            }
        }
    }

    impl StorageIo for PowerCutIo {
        fn write_segment(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
            file.write_all(bytes)?;
            let mut disk = self.0.lock().unwrap();
            if disk.stop_after == "segment-partial" {
                disk.stable_segment
                    .extend_from_slice(&bytes[..bytes.len() / 2]);
                return Err(io::Error::other("simulated partial persistence"));
            }
            if disk.stop_after == "segment-out-of-order" {
                let start = disk.stable_segment.len();
                disk.stable_segment.resize(start + bytes.len(), 0);
                disk.stable_segment[start + bytes.len() / 2..]
                    .copy_from_slice(&bytes[bytes.len() / 2..]);
                return Err(io::Error::other("simulated reordered persistence"));
            }
            drop(disk);
            self.stop("segment-write")
        }
        fn sync_segment(&self, file: &File) -> io::Result<()> {
            file.sync_all()?;
            let mut disk = self.0.lock().unwrap();
            disk.stable_segment = fs::read(disk.directory.join("segment-000000.log"))?;
            drop(disk);
            self.stop("segment-sync")
        }
        fn truncate_segment(&self, file: &File, length: u64) -> io::Result<()> {
            file.set_len(length)
        }
        fn write_checkpoint(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
            if self.0.lock().unwrap().stop_after == "checkpoint-partial" {
                file.write_all(&bytes[..bytes.len() / 2])?;
                return Err(io::Error::other("simulated torn checkpoint"));
            }
            file.write_all(bytes)?;
            self.stop("checkpoint-write")
        }
        fn sync_checkpoint(&self, file: &File) -> io::Result<()> {
            file.sync_all()?;
            let mut disk = self.0.lock().unwrap();
            disk.synced_temporary = Some(fs::read(disk.directory.join("checkpoint.tmp"))?);
            drop(disk);
            self.stop("checkpoint-sync")
        }
        fn rename_checkpoint(&self, temporary: &Path, final_path: &Path) -> io::Result<()> {
            fs::rename(temporary, final_path)?;
            let mut disk = self.0.lock().unwrap();
            if disk.stop_after == "rename-early-persistence" {
                disk.stable_checkpoint = disk.synced_temporary.take().unwrap();
                return Err(io::Error::other("simulated early rename persistence"));
            }
            drop(disk);
            self.stop("checkpoint-rename")
        }
        fn sync_directory(&self, directory: &Path) -> io::Result<()> {
            File::open(directory)?.sync_all()?;
            let mut disk = self.0.lock().unwrap();
            disk.stable_checkpoint = disk.synced_temporary.take().unwrap();
            drop(disk);
            self.stop("directory-sync")
        }
    }

    impl StorageIo for CountingIo {
        fn write_segment(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
            RealStorageIo.write_segment(file, bytes)
        }

        fn sync_segment(&self, file: &File) -> io::Result<()> {
            self.segment_syncs.fetch_add(1, Ordering::Relaxed);
            RealStorageIo.sync_segment(file)
        }

        fn truncate_segment(&self, file: &File, length: u64) -> io::Result<()> {
            RealStorageIo.truncate_segment(file, length)
        }

        fn write_checkpoint(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
            RealStorageIo.write_checkpoint(file, bytes)
        }

        fn sync_checkpoint(&self, file: &File) -> io::Result<()> {
            self.checkpoint_syncs.fetch_add(1, Ordering::Relaxed);
            RealStorageIo.sync_checkpoint(file)
        }

        fn rename_checkpoint(&self, temporary: &Path, final_path: &Path) -> io::Result<()> {
            RealStorageIo.rename_checkpoint(temporary, final_path)
        }

        fn sync_directory(&self, directory: &Path) -> io::Result<()> {
            RealStorageIo.sync_directory(directory)
        }
    }

    fn temp_dir() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("serapendiya-storage-{nonce}-{counter}"))
    }

    #[test]
    fn crc32c_known_vector() {
        assert_eq!(crc32c(b"123456789"), 0xe3069283);
    }

    #[test]
    fn simulated_power_cut_preserves_acknowledged_prefix() {
        for point in [
            "segment-write",
            "segment-partial",
            "segment-out-of-order",
            "segment-sync",
            "checkpoint-write",
            "checkpoint-partial",
            "checkpoint-sync",
            "checkpoint-rename",
            "rename-early-persistence",
            "directory-sync",
        ] {
            let dir = temp_dir();
            let mut initial = Log::open(&dir).unwrap();
            let acknowledged: [&[u8]; 3] = [b"first", b"second", b"third"];
            for value in acknowledged {
                initial.append(value).unwrap();
            }
            drop(initial);
            let state = Arc::new(std::sync::Mutex::new(SimulatedDisk {
                directory: dir.clone(),
                stop_after: point,
                stable_segment: fs::read(dir.join("segment-000000.log")).unwrap(),
                stable_checkpoint: fs::read(dir.join("checkpoint")).unwrap(),
                synced_temporary: None,
            }));
            let mut log =
                Log::open_with_io(&dir, Box::new(PowerCutIo(Arc::clone(&state)))).unwrap();
            assert!(log.append(b"interrupted").is_err(), "point: {point}");
            drop(log);
            let disk = state.lock().unwrap();
            fs::write(dir.join("segment-000000.log"), &disk.stable_segment).unwrap();
            fs::write(dir.join("checkpoint"), &disk.stable_checkpoint).unwrap();
            drop(disk);
            let mut recovered = Log::open(&dir).unwrap();
            for (offset, value) in acknowledged.iter().enumerate() {
                assert_eq!(
                    recovered
                        .read_batch(Offset::new(offset as u64))
                        .unwrap()
                        .unwrap()
                        .records,
                    *value,
                    "point: {point}"
                );
            }
            let interrupted = recovered.read_batch(Offset::new(3)).unwrap();
            if matches!(point, "directory-sync" | "rename-early-persistence") {
                assert_eq!(interrupted.unwrap().records, b"interrupted");
            } else {
                assert_eq!(interrupted, None, "point: {point}");
            }
            drop(recovered);
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn read_io_errors_are_not_misreported_as_corruption() {
        assert!(matches!(
            certified_io(7, io::Error::from(io::ErrorKind::UnexpectedEof)),
            Error::CertifiedCorruption { position: 7, .. }
        ));
        assert!(
            matches!(certified_io(7, io::Error::from(io::ErrorKind::PermissionDenied)), Error::StorageIo(error) if error.kind() == io::ErrorKind::PermissionDenied)
        );
    }

    #[test]
    fn initialization_failure_never_accepts_append_or_overwrites_ambiguous_temporary() {
        for point in [
            FailurePoint::SegmentSync,
            FailurePoint::CheckpointWrite,
            FailurePoint::CheckpointSync,
            FailurePoint::CheckpointRename,
            FailurePoint::DirectorySync,
        ] {
            let dir = temp_dir();
            assert!(Log::open_with_io(&dir, Box::new(FaultIo { point })).is_err());
            let temporary = dir.join("checkpoint.tmp");
            if temporary.exists() && !dir.join("checkpoint").exists() {
                let before = fs::read(&temporary).unwrap();
                assert!(Log::open(&dir).is_err());
                assert_eq!(fs::read(&temporary).unwrap(), before);
            } else {
                let mut log = Log::open(&dir).unwrap();
                assert_eq!(log.append(b"first").unwrap(), Offset::ZERO);
                drop(log);
            }
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn partial_initial_frame_is_discarded_after_versioned_empty_checkpoint() {
        let dir = temp_dir();
        drop(Log::open(&dir).unwrap());
        let path = dir.join("segment-000000.log");
        fs::write(&path, b"S").unwrap();
        let mut recovered = Log::open(&dir).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);
        assert_eq!(recovered.append(b"first").unwrap(), Offset::ZERO);
        drop(recovered);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn legacy_checkpoint_never_authorizes_tail_truncation() {
        let dir = temp_dir();
        drop(Log::open(&dir).unwrap());
        let checkpoint_path = dir.join("checkpoint");
        let mut checkpoint = fs::read(&checkpoint_path).unwrap();
        checkpoint[4] = 1;
        let checksum = crc32c(&checkpoint[..32]);
        checkpoint[32..36].copy_from_slice(&checksum.to_be_bytes());
        fs::write(&checkpoint_path, &checkpoint).unwrap();
        let segment_path = dir.join("segment-000000.log");
        fs::write(&segment_path, b"legacy data").unwrap();
        assert!(Log::open(&dir).is_err());
        assert_eq!(fs::read(segment_path).unwrap(), b"legacy data");
        assert_eq!(fs::read(checkpoint_path).unwrap(), checkpoint);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn directory_sync_failure_has_unknown_append_outcome() {
        let dir = temp_dir();
        drop(Log::open(&dir).unwrap());
        let mut log = Log::open_with_io(
            &dir,
            Box::new(FaultIo {
                point: FailurePoint::DirectorySync,
            }),
        )
        .unwrap();
        assert!(matches!(
            log.append(b"maybe"),
            Err(Error::StorageTerminal(_))
        ));
        drop(log);
        let mut recovered = Log::open(&dir).unwrap();
        assert_eq!(
            recovered.read_batch(Offset::ZERO).unwrap().unwrap().records,
            b"maybe"
        );
        drop(recovered);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn container_contains_expected_kafka_single_record_bytes() {
        let frame = encode_container(Offset::ZERO, b"one").unwrap();
        assert_eq!(
            &frame[CONTAINER_HEADER_LEN + HEADER_LEN..],
            &[18, 0, 0, 0, 1, 6, b'o', b'n', b'e', 0]
        );
        assert_eq!(&frame[..4], b"SRB1");
        assert_eq!(
            crc32c(&frame[..28]),
            u32::from_be_bytes(frame[28..32].try_into().unwrap())
        );
        assert_eq!(
            crc32c(&frame[CONTAINER_HEADER_LEN..]),
            u32::from_be_bytes(frame[24..28].try_into().unwrap())
        );
    }

    #[test]
    fn matches_kafka_python_3_0_13_batch_fixture() {
        // Generated independently by kafka-python 3.0.13 DefaultRecordBatchBuilder
        // for offset 0, timestamp 0, null key, value "one", no headers or compression.
        let fixture = "00000000000000000000003b00000000024690bf8600000000000000000000000000000000000000000000ffffffffffffffffffffffffffff000000011200000001066f6e6500";
        let expected: Vec<u8> = fixture
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        let frame = encode_container(Offset::ZERO, b"one").unwrap();
        assert_eq!(&frame[CONTAINER_HEADER_LEN..], expected);
    }

    #[test]
    fn matches_kafka_python_multibyte_record_lengths() {
        // Independent kafka-python 3.0.13 fixture: bytes 0..80 as the value.
        let fixture = "00000000000000000000008a00000000029b14360c00000000000000000000000000000000000000000000ffffffffffffffffffffffffffff00000001ae0100000001a001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f00";
        let expected: Vec<u8> = fixture
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        let value: Vec<u8> = (0..80).collect();
        let frame = encode_container(Offset::ZERO, &value).unwrap();
        assert_eq!(&frame[CONTAINER_HEADER_LEN..], expected);
        assert_eq!(
            decode_record(&frame[CONTAINER_HEADER_LEN + HEADER_LEN..]).unwrap(),
            value
        );
    }

    #[test]
    fn rejects_malformed_record_lengths_without_panicking() {
        for bytes in [
            &[][..],
            &[0x80][..],
            &[0xff, 0xff, 0xff, 0xff, 0xff][..],
            &[0x0a, 0, 0][..],
        ] {
            assert!(decode_record(bytes).is_err());
        }
    }

    #[test]
    fn legacy_segment_rejected_without_changing_data() {
        let dir = temp_dir();
        fs::create_dir_all(&dir).unwrap();
        let original = encode_batch(Offset::ZERO, (FIXED_BATCH_REMAINDER + 3) as i32, b"old");
        fs::write(dir.join("segment-000000.log"), &original).unwrap();
        write_checkpoint(
            &dir,
            Checkpoint {
                certified_len: original.len() as u64,
                next_offset: Offset::new(1),
                generation: 1,
            },
            &RealStorageIo,
        )
        .unwrap();
        assert!(matches!(Log::open(&dir), Err(Error::InvalidCheckpoint(_))));
        assert_eq!(fs::read(dir.join("segment-000000.log")).unwrap(), original);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn unrecognized_directory_is_refused_without_creating_lock() {
        let dir = temp_dir();
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("unexpected"), b"keep").unwrap();
        assert!(Log::open(&dir).is_err());
        assert!(!dir.join(".lock").exists());
        assert_eq!(fs::read(dir.join("unexpected")).unwrap(), b"keep");
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn symlink_data_directory_is_refused() {
        use std::os::unix::fs::symlink;
        let dir = temp_dir();
        let link = temp_dir();
        fs::create_dir_all(&dir).unwrap();
        symlink(&dir, &link).unwrap();
        assert!(Log::open(&link).is_err());
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);
        fs::remove_file(link).unwrap();
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rejects_corrupted_container_header() {
        let dir = temp_dir();
        let mut log = Log::open(&dir).unwrap();
        log.append(b"one").unwrap();
        drop(log);
        let mut file = OpenOptions::new()
            .write(true)
            .open(dir.join("segment-000000.log"))
            .unwrap();
        file.seek(SeekFrom::Start(19)).unwrap();
        file.write_all(&[1]).unwrap();
        file.sync_all().unwrap();
        assert!(matches!(
            Log::open(&dir),
            Err(Error::CertifiedCorruption { .. })
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rejects_unknown_container_version_type_and_flags_with_valid_crc() {
        for offset in [4usize, 5, 6] {
            let dir = temp_dir();
            let mut log = Log::open(&dir).unwrap();
            log.append(b"one").unwrap();
            drop(log);
            let path = dir.join("segment-000000.log");
            let mut bytes = fs::read(&path).unwrap();
            bytes[offset] = if offset == 4 { 2 } else { 1 };
            let checksum = crc32c(&bytes[..28]);
            bytes[28..32].copy_from_slice(&checksum.to_be_bytes());
            fs::write(path, bytes).unwrap();
            assert!(
                matches!(Log::open(&dir), Err(Error::CertifiedCorruption { .. })),
                "field byte: {offset}"
            );
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn refuses_second_handle_until_first_is_dropped() {
        let dir = temp_dir();
        let mut first = Log::open(&dir).unwrap();
        first.append(b"first").unwrap();
        assert!(Log::open(&dir).is_err());
        drop(first);
        let mut reopened = Log::open(&dir).unwrap();
        assert_eq!(
            reopened.read_batch(Offset::ZERO).unwrap().unwrap().records,
            b"first"
        );
        drop(reopened);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn missing_checkpoint_never_truncates_nonempty_segment() {
        let dir = temp_dir();
        let mut log = Log::open(&dir).unwrap();
        log.append(b"certified").unwrap();
        drop(log);
        fs::remove_file(dir.join("checkpoint")).unwrap();
        let segment = dir.join("segment-000000.log");
        let original = fs::read(&segment).unwrap();
        assert!(matches!(Log::open(&dir), Err(Error::InvalidCheckpoint(_))));
        assert_eq!(fs::read(segment).unwrap(), original);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rejects_oversized_groups_and_unrepresentable_offsets() {
        let dir = temp_dir();
        let mut log = Log::open(&dir).unwrap();
        assert!(matches!(
            log.append_group(&vec![&b""[..]; MAX_GROUP_BATCHES + 1]),
            Err(Error::InvalidBatch(_))
        ));
        assert!(matches!(
            log.append(&vec![0; MAX_GROUP_BYTES]),
            Err(Error::InvalidBatch(_))
        ));
        log.next_offset = Offset::new(i64::MAX as u64 + 1);
        assert!(matches!(log.append(b"x"), Err(Error::InvalidBatch(_))));
        assert_eq!(log.certified_len(), 0);
        drop(log);
        fs::remove_dir_all(dir).unwrap();
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

    #[test]
    fn rejects_torn_checkpoint() {
        let dir = temp_dir();
        {
            let mut log = Log::open(&dir).unwrap();
            log.append(b"one").unwrap();
        }
        let checkpoint = dir.join("checkpoint");
        let mut file = OpenOptions::new().write(true).open(checkpoint).unwrap();
        file.seek(SeekFrom::Start(8)).unwrap();
        file.write_all(&[1]).unwrap();
        file.sync_all().unwrap();
        assert!(matches!(Log::open(&dir), Err(Error::InvalidCheckpoint(_))));
        fs::remove_dir_all(dir).unwrap();
    }

    fn assert_terminal_after_failure(point: FailurePoint) {
        let dir = temp_dir();
        drop(Log::open(&dir).unwrap());
        let mut log = Log::open_with_io(&dir, Box::new(FaultIo { point })).unwrap();
        assert!(matches!(log.append(b"one"), Err(Error::StorageTerminal(_))));
        assert!(matches!(log.append(b"two"), Err(Error::StorageTerminal(_))));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn segment_failures_are_terminal() {
        assert_terminal_after_failure(FailurePoint::SegmentWrite);
        assert_terminal_after_failure(FailurePoint::PartialSegmentWrite);
        assert_terminal_after_failure(FailurePoint::SegmentSync);
    }

    #[test]
    fn checkpoint_failures_are_terminal() {
        assert_terminal_after_failure(FailurePoint::CheckpointWrite);
        assert_terminal_after_failure(FailurePoint::CheckpointSync);
        assert_terminal_after_failure(FailurePoint::CheckpointRename);
        assert_terminal_after_failure(FailurePoint::DirectorySync);
    }

    #[test]
    fn reads_batches_by_certified_offset() {
        let dir = temp_dir();
        {
            let mut log = Log::open(&dir).unwrap();
            log.append(b"one").unwrap();
            log.append(b"two").unwrap();
        }
        let mut log = Log::open(&dir).unwrap();
        assert_eq!(log.batch_count(), 2);
        assert_eq!(
            log.read_batch(Offset::ZERO).unwrap(),
            Some(StoredBatch {
                offset: Offset::ZERO,
                records: b"one".to_vec()
            })
        );
        assert_eq!(
            log.read_batch(Offset::new(1)).unwrap(),
            Some(StoredBatch {
                offset: Offset::new(1),
                records: b"two".to_vec()
            })
        );
        assert_eq!(log.read_batch(Offset::new(2)).unwrap(), None);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rebuilds_sparse_index_on_reopen() {
        let dir = temp_dir();
        {
            let mut log = Log::open(&dir).unwrap();
            for number in 0..17u8 {
                log.append(&[number]).unwrap();
            }
        }
        let mut log = Log::open(&dir).unwrap();
        assert_eq!(log.batch_count(), 17);
        assert_eq!(
            log.index_entries(),
            &[
                IndexEntry {
                    offset: Offset::ZERO,
                    position: 0,
                },
                IndexEntry {
                    offset: Offset::new(16),
                    position: (16 * encode_container(Offset::ZERO, &[0]).unwrap().len()) as u64,
                }
            ]
        );
        assert_eq!(
            log.read_batch(Offset::new(16)).unwrap(),
            Some(StoredBatch {
                offset: Offset::new(16),
                records: vec![16]
            })
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn tracks_append_visible_and_durable_positions() {
        let dir = temp_dir();
        let mut log = Log::open(&dir).unwrap();
        assert_eq!(
            log.positions(),
            LogPositions {
                append_position: 0,
                visible_offset: None,
                durable_offset: None,
            }
        );
        log.append(b"one").unwrap();
        let positions = log.positions();
        assert!(positions.append_position > 0);
        assert_eq!(positions.visible_offset, Some(Offset::ZERO));
        assert_eq!(positions.durable_offset, Some(Offset::ZERO));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn checkpoint_failure_does_not_make_uncertified_data_visible() {
        let dir = temp_dir();
        drop(Log::open(&dir).unwrap());
        let mut log = Log::open_with_io(
            &dir,
            Box::new(FaultIo {
                point: FailurePoint::CheckpointSync,
            }),
        )
        .unwrap();
        assert!(matches!(log.append(b"one"), Err(Error::StorageTerminal(_))));
        let positions = log.positions();
        assert!(positions.append_position > 0);
        assert_eq!(positions.visible_offset, None);
        assert_eq!(positions.durable_offset, None);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn group_commit_syncs_once_for_multiple_batches() {
        let dir = temp_dir();
        drop(Log::open(&dir).unwrap());
        let segment_syncs = Arc::new(AtomicU64::new(0));
        let checkpoint_syncs = Arc::new(AtomicU64::new(0));
        let mut log = Log::open_with_io(
            &dir,
            Box::new(CountingIo {
                segment_syncs: Arc::clone(&segment_syncs),
                checkpoint_syncs: Arc::clone(&checkpoint_syncs),
            }),
        )
        .unwrap();
        let offsets = log.append_group(&[b"one", b"two", b"three"]).unwrap();
        assert_eq!(offsets, vec![Offset::ZERO, Offset::new(1), Offset::new(2)]);
        assert_eq!(segment_syncs.load(Ordering::Relaxed), 1);
        assert_eq!(checkpoint_syncs.load(Ordering::Relaxed), 1);
        assert_eq!(log.durable_offset, Some(Offset::new(2)));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rejects_checkpoint_offset_mismatch() {
        let dir = temp_dir();
        let (certified_len, generation) = {
            let mut log = Log::open(&dir).unwrap();
            log.append(b"one").unwrap();
            (log.certified_len(), 1)
        };
        write_checkpoint(
            &dir,
            Checkpoint {
                certified_len,
                next_offset: Offset::new(9),
                generation,
            },
            &RealStorageIo,
        )
        .unwrap();
        assert!(matches!(
            Log::open(&dir),
            Err(Error::InvalidCheckpoint(reason)) if reason == "checkpoint does not match certified batches"
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rejects_checkpoint_boundary_inside_batch() {
        let dir = temp_dir();
        let (certified_len, generation) = {
            let mut log = Log::open(&dir).unwrap();
            log.append(b"one").unwrap();
            (log.certified_len(), 1)
        };
        write_checkpoint(
            &dir,
            Checkpoint {
                certified_len: certified_len - 1,
                next_offset: Offset::ZERO,
                generation,
            },
            &RealStorageIo,
        )
        .unwrap();
        assert!(matches!(
            Log::open(&dir),
            Err(Error::CertifiedCorruption { .. })
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn truncates_valid_uncertified_batch_and_rebuilds_metadata() {
        let dir = temp_dir();
        let certified_len = {
            let mut log = Log::open(&dir).unwrap();
            for number in 0..17u8 {
                log.append(&[number]).unwrap();
            }
            log.certified_len()
        };
        let path = dir.join("segment-000000.log");
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&encode_container(Offset::new(17), b"tail").unwrap())
            .unwrap();
        file.sync_all().unwrap();

        let mut log = Log::open(&dir).unwrap();
        assert_eq!(log.certified_len(), certified_len);
        assert_eq!(log.batch_count(), 17);
        assert_eq!(log.index_entries().len(), 2);
        assert_eq!(log.read_batch(Offset::new(17)).unwrap(), None);
        assert_eq!(fs::metadata(path).unwrap().len(), certified_len);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rejects_truncated_certified_segment() {
        let dir = temp_dir();
        let certified_len = {
            let mut log = Log::open(&dir).unwrap();
            log.append(b"one").unwrap();
            log.certified_len()
        };
        let path = dir.join("segment-000000.log");
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(certified_len - 1).unwrap();
        file.sync_all().unwrap();
        assert!(matches!(Log::open(&dir), Err(Error::InvalidCheckpoint(_))));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rejects_non_contiguous_certified_batch() {
        let dir = temp_dir();
        {
            let mut log = Log::open(&dir).unwrap();
            log.append(b"one").unwrap();
            log.append(b"two").unwrap();
        }
        let path = dir.join("segment-000000.log");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let second_position = encode_container(Offset::ZERO, b"one").unwrap().len() as u64;
        file.seek(SeekFrom::Start(second_position)).unwrap();
        file.write_all(&encode_container(Offset::ZERO, b"two").unwrap())
            .unwrap();
        file.sync_all().unwrap();
        assert!(matches!(
            Log::open(&dir),
            Err(Error::CertifiedCorruption { reason, .. }) if reason == "base offset is not contiguous"
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rejects_malformed_certified_length() {
        let dir = temp_dir();
        {
            let mut log = Log::open(&dir).unwrap();
            log.append(b"one").unwrap();
        }
        let path = dir.join("segment-000000.log");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(8)).unwrap();
        file.write_all(&i32::MAX.to_be_bytes()).unwrap();
        file.sync_all().unwrap();
        assert!(matches!(
            Log::open(&dir),
            Err(Error::CertifiedCorruption { .. })
        ));
        fs::remove_dir_all(dir).unwrap();
    }
}
