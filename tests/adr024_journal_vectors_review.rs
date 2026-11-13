//! Independent checks of preliminary ADR-024 journal vectors, not a parser.

fn decode_hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0);
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(text, 16).unwrap()
        })
        .collect()
}

fn be32(value: &[u8]) -> u32 {
    u32::from_be_bytes(value.try_into().unwrap())
}

fn be64(value: &[u8]) -> u64 {
    u64::from_be_bytes(value.try_into().unwrap())
}

#[test]
fn preliminary_empty_journal_and_checkpoint_vectors() {
    let packet = include_str!("../docs/adr/ADR-024-metadata-journal-decision-packet.md");
    let vector = |prefix| {
        decode_hex(
            packet
                .lines()
                .find_map(|line| line.strip_prefix(prefix))
                .unwrap(),
        )
    };
    let journal = vector("journal (112 bytes): ");
    let checkpoint = vector("checkpoint (64 bytes): ");
    assert_eq!(journal.len(), 112);
    assert_eq!(checkpoint.len(), 64);
    assert_eq!(&journal[..8], b"SRJ1\x01\0\0\0");
    assert_eq!(&checkpoint[..8], b"SRJC\x01\0\0\0");
    assert_eq!(&checkpoint[8..24], &journal[8..24]);
    assert_eq!(be64(&checkpoint[24..32]), be64(&journal[24..32]));
    assert_eq!(be32(&journal[32..36]), 40);
    assert_eq!(crc32c::crc32c(&journal[..36]), be32(&journal[36..40]));
    let record = &journal[40..];
    assert_eq!(&record[..8], b"SRJR\x01\x01\0\0");
    assert_eq!(be32(&record[8..12]) as usize, record.len());
    assert_eq!(be64(&record[12..20]), 0);
    assert_eq!(be32(&record[20..24]) as usize, record.len() - 32);
    assert_eq!(crc32c::crc32c(&record[..28]), be32(&record[28..32]));
    assert_eq!(crc32c::crc32c(&record[32..]), be32(&record[24..28]));
    assert_eq!(be64(&record[32..40]), 0);
    assert_eq!(be32(&record[40..44]), 1);
    assert!(record[44..].iter().all(|byte| *byte == 0));
    assert_eq!(be64(&checkpoint[32..40]), journal.len() as u64);
    assert_eq!(be64(&checkpoint[40..48]), 0);
    assert_eq!(be64(&checkpoint[48..56]), 0);
    assert!(checkpoint[56..60].iter().all(|byte| *byte == 0));
    assert_eq!(crc32c::crc32c(&checkpoint[..60]), be32(&checkpoint[60..]));
}

#[test]
fn preliminary_rotation_vectors_bind_retired_journal() {
    let packet = include_str!("../docs/adr/ADR-024-journal-rotation-candidate.md");
    let vector = |prefix| {
        decode_hex(
            packet
                .lines()
                .find_map(|line| line.strip_prefix(prefix))
                .unwrap(),
        )
    };
    let old = vector("old journal (112 bytes): ");
    let new = vector("new journal (112 bytes): ");
    let checkpoint = vector("checkpoint (96 bytes): ");
    let initial = vector("initial checkpoint (96 bytes): ");
    assert_eq!(old.len(), 112);
    assert_eq!(new.len(), 112);
    assert_eq!(checkpoint.len(), 96);
    assert_eq!(initial.len(), 96);
    assert_eq!(&initial[..8], b"SRJC\x02\0\0\0");
    assert_eq!(&initial[8..24], &old[8..24]);
    assert_eq!(be64(&initial[24..32]), 0);
    assert_eq!(be64(&initial[32..40]), old.len() as u64);
    assert!(initial[40..92].iter().all(|byte| *byte == 0));
    assert_eq!(crc32c::crc32c(&initial[..92]), be32(&initial[92..]));
    assert_eq!(&checkpoint[..8], b"SRJC\x02\0\0\0");
    assert_eq!(&old[8..24], &new[8..24]);
    assert_eq!(&old[8..24], &checkpoint[8..24]);
    assert_eq!(be64(&old[24..32]), 0);
    assert_eq!(be64(&new[24..32]), 1);
    assert_eq!(be64(&checkpoint[24..32]), 1);
    assert_eq!(crc32c::crc32c(&old[..36]), be32(&old[36..40]));
    assert_eq!(crc32c::crc32c(&new[..36]), be32(&new[36..40]));
    for journal in [&old, &new] {
        let record = &journal[40..];
        assert_eq!(&record[..8], b"SRJR\x01\x01\0\0");
        assert_eq!(be32(&record[8..12]) as usize, record.len());
        assert_eq!(crc32c::crc32c(&record[..28]), be32(&record[28..32]));
        assert_eq!(crc32c::crc32c(&record[32..]), be32(&record[24..28]));
    }
    assert_eq!(be64(&old[52..60]), 0);
    assert_eq!(be64(&new[52..60]), 1);
    assert_eq!(&old[72..], &new[72..]);
    assert_eq!(be64(&checkpoint[32..40]), new.len() as u64);
    assert_eq!(be64(&checkpoint[40..48]), 1);
    assert_eq!(be64(&checkpoint[48..56]), 0);
    assert_eq!(be32(&checkpoint[56..60]), 1);
    assert_eq!(be64(&checkpoint[60..68]), 0);
    assert_eq!(be64(&checkpoint[68..76]), old.len() as u64);
    assert_eq!(be64(&checkpoint[76..84]), 0);
    assert_eq!(be32(&checkpoint[84..88]), crc32c::crc32c(&old));
    assert!(checkpoint[88..92].iter().all(|byte| *byte == 0));
    assert_eq!(crc32c::crc32c(&checkpoint[..92]), be32(&checkpoint[92..]));
    let mut damaged = checkpoint.clone();
    damaged[68] ^= 1;
    assert_ne!(crc32c::crc32c(&damaged[..92]), be32(&damaged[92..]));
    let mut damaged = old.clone();
    damaged[80] ^= 1;
    assert_ne!(crc32c::crc32c(&damaged), be32(&checkpoint[84..88]));
}
