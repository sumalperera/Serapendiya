#!/usr/bin/env python3
"""Generate review-only journal fixture directories at a new path.

The two event frames come from the previously recorded ADR-024 fixtures. No
production journal, migration or recovery code is exercised by this generator.
"""
import pathlib
import hashlib
import struct
import sys

U32 = lambda value: struct.pack(">I", value)
U64 = lambda value: struct.pack(">Q", value)
IDENTITY = bytes.fromhex("00112233445566778899aabbccddeeff")
ROOT = pathlib.Path(__file__).resolve().parents[1]


def crc(data):
    value = 0xFFFFFFFF
    for byte in data:
        value ^= byte
        for _ in range(8):
            value = (value >> 1) ^ (0x82F63B78 if value & 1 else 0)
    return value ^ 0xFFFFFFFF


def header(epoch):
    raw = b"SRJ1\x01\0\0\0" + IDENTITY + U64(epoch) + U32(40)
    return raw + U32(crc(raw))


def record(kind, sequence, payload):
    raw = b"SRJR\x01" + bytes([kind]) + b"\0\0" + U32(32 + len(payload))
    raw += U64(sequence) + U32(len(payload)) + U32(crc(payload))
    return raw + U32(crc(raw)) + payload


def snapshot(entries):
    data = U64(entries[-1][1]) + U32(len(entries)) + bytes(4)
    return data + b"".join(U64(base) + U64(end) + U64(length) for base, end, length in entries)


def checkpoint(epoch, journal, seq, next_offset, retired=None):
    data = b"SRJC\x02\0\0\0" + IDENTITY + U64(epoch) + U64(len(journal))
    data += U64(seq) + U64(next_offset)
    if retired is None:
        data += bytes(32)
    else:
        old_epoch, old_journal, old_seq = retired
        data += U32(1) + U64(old_epoch) + U64(len(old_journal))
        data += U64(old_seq) + U32(crc(old_journal))
    data += bytes(4)
    assert len(data) == 92
    return data + U32(crc(data))


def put(root, name, files):
    directory = root / name
    directory.mkdir()
    for filename, data in files.items():
        (directory / filename).write_bytes(data)


assert crc(b"123456789") == 0xE3069283
if len(sys.argv) == 3 and sys.argv[1] == "--verify":
    root = pathlib.Path(sys.argv[2])
    inventory = ROOT / "docs/verification/adr024-journal-fixtures.sha256"
    expected = {}
    for line in inventory.read_text().splitlines():
        digest, name = line.split("  ", 1)
        expected[name] = digest
    actual = {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*") if path.is_file()
    }
    if actual != expected:
        raise SystemExit("fixture files or SHA-256 values do not match the inventory")
    print(f"Verified {len(actual)} fixture files against the inventory")
    sys.exit(0)
if len(sys.argv) != 2:
    raise SystemExit("usage: adr024-journal-fixtures.py OUTPUT | --verify DIRECTORY")
root = pathlib.Path(sys.argv[1])
try:
    root.mkdir()  # never overwrite an existing review set
except FileExistsError:
    raise SystemExit(f"output already exists; choose a new path: {root}") from None
source = ROOT / "docs/verification/adr024-fixtures/two"
segment0 = "segment-00000000000000000000.log"
segment1 = "segment-00000000000000000001.log"
f0, f1 = (source / segment0).read_bytes(), (source / segment1).read_bytes()
journal0 = "journal-00000000000000000000.log"
journal1 = "journal-00000000000000000001.log"
old = header(0) + record(1, 0, snapshot([(0, 0, 0)]))
old += record(2, 1, U64(0) + U64(0) + U64(len(f0)) + U64(0) + U64(1))
old += record(3, 2, U64(0) + U64(1))
old += record(2, 3, U64(1) + U64(0) + U64(len(f1)) + U64(1) + U64(2))
new = header(1) + record(1, 4, snapshot([(0, 1, len(f0)), (1, 2, len(f1))]))
cp = checkpoint(1, new, 4, 2, (0, old, 3))
files = {".lock": b"", "journal-checkpoint": cp, journal0: old, journal1: new,
         segment0: f0, segment1: f1}
put(root, "rotation-with-retired", files)
put(root, "rotation-after-cleanup", {k: v for k, v in files.items() if k != journal0})
put(root, "old-authority-with-orphan", dict(files, **{"journal-checkpoint": checkpoint(0, old, 3, 2)}))
put(root, "stale-temporary", dict(files, **{"journal-checkpoint.tmp": cp}))
put(root, "missing-active-journal", {k: v for k, v in files.items() if k != journal1})
broken = bytearray(cp)
broken[-1] ^= 1
put(root, "bad-checkpoint-crc", dict(files, **{"journal-checkpoint": bytes(broken)}))
broken = bytearray(old)
broken[-1] ^= 1
put(root, "corrupt-retired", dict(files, **{journal0: bytes(broken)}))
put(root, "short-active", dict(files, **{journal1: new[:-1]}))
put(root, "journal-and-segment-suffix", dict(files, **{journal1: new + b"tail", segment1: f1 + b"tail"}))

# A whole CRC-valid journal commit and matching event frame lie after the
# checkpoint. They are deliberately uncertified even though they parse.
f2 = bytearray(f1)
f2[12:20] = U64(2)  # container base offset
f2[32:40] = U64(2)  # Kafka batch base offset
f2[24:28] = U32(crc(f2[32:]))
f2[28:32] = U32(crc(f2[:28]))
tail = record(2, 5, U64(1) + U64(len(f1)) + U64(len(f1) + len(f2)) + U64(2) + U64(3))
assert len(tail) == 72
without_retired = {k: v for k, v in files.items() if k != journal0}
put(root, "valid-uncertified-commit", dict(without_retired, **{journal1: new + tail, segment1: f1 + bytes(f2)}))

# The checkpoint checksum is valid, but its certified length ends inside a record.
partial = bytearray(cp)
partial[32:40] = U64(len(new) - 1)
partial[-4:] = U32(crc(partial[:-4]))
put(root, "checkpoint-mid-record", dict(files, **{"journal-checkpoint": bytes(partial)}))

# Checkpoint retirement binding disagrees with the physical retired file length.
wrong_retired_len = bytearray(cp)
wrong_retired_len[68:76] = U64(len(old) + 1)
wrong_retired_len[-4:] = U32(crc(wrong_retired_len[:-4]))
put(root, "retired-length-mismatch", dict(files, **{"journal-checkpoint": bytes(wrong_retired_len)}))

# The snapshot certifies the original frame length but the segment is shorter.
put(root, "certified-segment-short", dict(files, **{segment1: f1[:-1]}))
