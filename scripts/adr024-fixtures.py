#!/usr/bin/env python3
"""Generate proposed ADR-024 fixtures in a new disposable directory.

This is a specification aid, not the storage implementation or a migrator.
"""
import pathlib
import struct
import sys


def crc(data):
    n = 0xffffffff
    for byte in data:
        n ^= byte
        for _ in range(8):
            n = (n >> 1) ^ (0x82f63b78 if n & 1 else 0)
    return n ^ 0xffffffff


def u32(n): return struct.pack('>I', n)
def u64(n): return struct.pack('>Q', n)


def frame(offset, value):
    # Both record-length varints remain one byte for these fixture values.
    assert len(value) < 50
    record = bytes([2 * (6 + len(value)), 0, 0, 0, 1, 2 * len(value)]) + value + b'\0'
    batch = (struct.pack('>qiibIhiqqqhii', offset, 49 + len(record), 0, 2, 0,
                         0, 0, 0, 0, -1, -1, -1, 1) + record)
    batch = batch[:17] + u32(crc(batch[21:])) + batch[21:]
    header = b'SRB1\x01\0\0\0' + u32(len(batch)) + u64(offset) + u32(1) + u32(crc(batch))
    return header + u32(crc(header)) + batch


def manifest(entries, version=1):
    data = (b'SRSM' + bytes([version, 0, 0, 0]) + u32(52 + 24 * len(entries))
            + u32(len(entries)) + bytes.fromhex('00112233445566778899aabbccddeeff')
            + u64(entries[-1][1]) + u64(0))
    data += b''.join(u64(a) + u64(b) + u64(c) for a, b, c in entries)
    return data + u32(crc(data))


def directory(root, name, files):
    path = root / name
    path.mkdir()
    for filename, data in files.items():
        (path / filename).write_bytes(data)


assert crc(b'123456789') == 0xe3069283
root = pathlib.Path(sys.argv[1])
root.mkdir()  # exclusive: never replace existing fixtures
f0, f1 = frame(0, b'alpha'), frame(1, b'beta')
seg0, seg1 = 'segment-00000000000000000000.log', 'segment-00000000000000000001.log'
two = {'.lock': b'', 'manifest': manifest([(0, 1, len(f0)), (1, 2, len(f1))]), seg0: f0, seg1: f1}
directory(root, 'empty', {'.lock': b'', 'manifest': manifest([(0, 0, 0)]), seg0: b''})
directory(root, 'two', two)
directory(root, 'migrated', two)
five = b''.join(frame(i, b'alpha') for i in range(5))
sixth = frame(5, b'beta')
directory(root, 'nonunit-base', {'.lock': b'', 'manifest': manifest([(0, 5, len(five)), (5, 6, len(sixth))]), seg0: five, 'segment-00000000000000000005.log': sixth})
checkpoint = b'SRLG\x02\0\0\0' + u64(len(f0 + f1)) + u64(2) + u64(1)
directory(root, 'legacy-source', {'.lock': b'', 'checkpoint': checkpoint + u32(crc(checkpoint)), 'segment-000000.log': f0 + f1})
bad = dict(two)
bad['manifest'] = two['manifest'][:-1] + bytes([two['manifest'][-1] ^ 1])
directory(root, 'bad-checksum', bad)
future = dict(two)
future['manifest'] = manifest([(0, 1, len(f0)), (1, 2, len(f1))], 2)
directory(root, 'future-version', future)
gap = dict(two)
gap['manifest'] = manifest([(0, 1, len(f0)), (2, 3, len(f1))])
directory(root, 'offset-gap', gap)
reserved = bytearray(two['manifest'])
reserved[5] = 1
reserved[-4:] = u32(crc(reserved[:-4]))
directory(root, 'reserved-byte', dict(two, manifest=bytes(reserved)))
corrupt = dict(two)
corrupt[seg0] = f0[:-2] + bytes([f0[-2] ^ 1]) + f0[-1:]
directory(root, 'corrupt-payload', corrupt)
duplicate = dict(two)
duplicate['manifest'] = manifest([(0, 1, len(f0)), (0, 1, len(f0))])
directory(root, 'duplicate-entry', duplicate)
length = bytearray(two['manifest'])
length[11] ^= 1
length[-4:] = u32(crc(length[:-4]))
directory(root, 'bad-length', dict(two, manifest=bytes(length)))
directory(root, 'truncated-manifest', dict(two, manifest=two['manifest'][:-1]))
identity = bytearray(two['manifest'])
identity[16:32] = bytes(16)
identity[-4:] = u32(crc(identity[:-4]))
directory(root, 'zero-identity', dict(two, manifest=bytes(identity)))
