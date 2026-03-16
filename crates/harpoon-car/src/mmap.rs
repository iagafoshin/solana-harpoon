use {
    crate::{
        node::{NodeError, RawNode},
        varint,
    },
    memmap2::Mmap,
    std::{fs::File, io, path::Path},
};

const MAX_ALLOWED_HEADER_SIZE: usize = 1024;
const MAX_ALLOWED_SECTION_SIZE: usize = 32 << 20; // 32MiB
const RECOVERY_SCAN_LIMIT: usize = 1 << 20; // 1MiB

/// Synchronous CAR reader backed by a memory-mapped file.
///
/// Uses `memmap2::Mmap` for zero-copy access — the OS handles
/// prefetch and page faults, avoiding async overhead entirely.
/// This is the fast path for locally downloaded CAR files.
pub struct MmapNodeReader {
    mmap: Mmap,
    pos: usize,
    header_end: usize,
}

impl MmapNodeReader {
    /// Open a CAR file via mmap.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, NodeError> {
        let file = File::open(path).map_err(NodeError::Io)?;
        // SAFETY: We treat the mmap as read-only. The caller must ensure
        // the file is not truncated or modified while mapped.
        let mmap = unsafe { Mmap::map(&file) }.map_err(NodeError::Io)?;
        let mut reader = Self {
            mmap,
            pos: 0,
            header_end: 0,
        };
        reader.read_header()?;
        Ok(reader)
    }

    /// Create a reader from raw bytes (useful for testing).
    pub fn from_bytes(data: &[u8]) -> Result<Self, NodeError> {
        let mut mmap_mut = memmap2::MmapMut::map_anon(data.len()).map_err(NodeError::Io)?;
        mmap_mut.copy_from_slice(data);
        let mmap = mmap_mut.make_read_only().map_err(NodeError::Io)?;
        let mut reader = Self {
            mmap,
            pos: 0,
            header_end: 0,
        };
        reader.read_header()?;
        Ok(reader)
    }

    /// Returns the CAR header bytes.
    pub fn header(&self) -> &[u8] {
        &self.mmap[self.header_end - self.varint_len_at(0)..self.header_end]
    }

    fn varint_len_at(&self, start: usize) -> usize {
        let buf = &self.mmap[start..];
        for (i, &b) in buf.iter().enumerate() {
            if b < 0x80 {
                return i + 1;
            }
        }
        0
    }

    fn read_header(&mut self) -> Result<(), NodeError> {
        let (header_len, _varint_start) = self.read_varint()?;
        let header_len = header_len as usize;
        if header_len > MAX_ALLOWED_HEADER_SIZE {
            return Err(NodeError::HeaderTooLong(header_len));
        }
        // self.pos is already past the varint after read_varint()
        if self.pos + header_len > self.mmap.len() {
            return Err(NodeError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "CAR header truncated",
            )));
        }
        self.header_end = self.pos + header_len;
        self.pos = self.header_end;
        Ok(())
    }

    fn read_varint(&mut self) -> Result<(u64, usize), NodeError> {
        let remaining = &self.mmap[self.pos..];
        if remaining.is_empty() {
            return Err(NodeError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF reading varint",
            )));
        }
        let mut buf = remaining;
        let value = varint::decode_varint(&mut buf)?;
        let consumed = remaining.len() - buf.len();
        let start = self.pos;
        self.pos += consumed;
        Ok((value, start))
    }

    /// Read the next node from the CAR file, or `None` at EOF.
    pub fn read_node(&mut self) -> Result<Option<RawNode>, NodeError> {
        if self.pos >= self.mmap.len() {
            return Ok(None);
        }

        let saved_pos = self.pos;
        let (section_size, _) = match self.read_varint() {
            Ok(v) => v,
            Err(NodeError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => {
                eprintln!("[warn] mmap read error at pos {saved_pos}: {e}");
                self.pos = saved_pos;
                if self.try_recover() {
                    return self.read_node();
                }
                return Ok(None);
            }
        };
        let section_size = section_size as usize;
        if section_size > MAX_ALLOWED_SECTION_SIZE {
            eprintln!(
                "[warn] mmap section too long ({section_size}) at pos {saved_pos}"
            );
            self.pos = saved_pos;
            if self.try_recover() {
                return self.read_node();
            }
            return Ok(None);
        }
        if self.pos + section_size > self.mmap.len() {
            return Ok(None);
        }

        let section = self.mmap[self.pos..self.pos + section_size].to_vec();
        self.pos += section_size;
        match RawNode::new_from_vec(section) {
            Ok(node) => Ok(Some(node)),
            Err(e) => {
                eprintln!(
                    "[warn] mmap node parse error at pos {saved_pos}: {e}"
                );
                // pos already advanced past this section, just skip the bad node
                Ok(self.read_node()?.or(None))
            }
        }
    }

    /// Scan forward byte-by-byte to find the next valid CAR section.
    ///
    /// A valid section starts with a varint encoding a reasonable size
    /// (≤ `MAX_ALLOWED_SECTION_SIZE`) followed by a CID whose version byte
    /// is 0 or 1. Returns `true` if a candidate was found and `self.pos`
    /// is rewound to the start of that section's varint.
    fn try_recover(&mut self) -> bool {
        let start = self.pos;
        let limit = (self.pos + RECOVERY_SCAN_LIMIT).min(self.mmap.len());
        while self.pos < limit {
            self.pos += 1;
            let saved = self.pos;
            if let Ok((section_size, _)) = self.read_varint() {
                let section_size = section_size as usize;
                if section_size > 0
                    && section_size <= MAX_ALLOWED_SECTION_SIZE
                    && self.pos + section_size <= self.mmap.len()
                {
                    // Peek at CID version byte
                    if let Some(&first_byte) = self.mmap.get(self.pos) {
                        if first_byte <= 1 {
                            self.pos = saved;
                            eprintln!(
                                "[warn] mmap recovery: skipped {} bytes at offset {start}",
                                saved - start
                            );
                            return true;
                        }
                    }
                }
            }
            self.pos = saved;
        }
        false
    }
}

impl Iterator for MmapNodeReader {
    type Item = Result<RawNode, NodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        // read_node handles recovery internally and returns Ok(None) on
        // unrecoverable errors, so the iterator never gets stuck.
        match self.read_node() {
            Ok(Some(node)) => Some(Ok(node)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal CIDv1 section: CID (v1, dag-cbor, sha2-256, 32-byte zero digest) + payload.
    fn make_section(payload: &[u8]) -> Vec<u8> {
        // CID: version=1, codec=0x71 (dag-cbor), hash=0x12 (sha2-256), digest_len=32
        let mut cid_bytes = vec![0x01, 0x71, 0x12, 0x20];
        cid_bytes.extend_from_slice(&[0u8; 32]); // 32-byte zero digest
        let section_len = cid_bytes.len() + payload.len();
        let mut out = Vec::new();
        encode_varint(section_len as u64, &mut out);
        out.extend_from_slice(&cid_bytes);
        out.extend_from_slice(payload);
        out
    }

    /// Build a minimal CAR byte stream: header + sections.
    fn make_car(sections: &[Vec<u8>]) -> Vec<u8> {
        // Minimal CAR header (CBOR map: {version: 1, roots: []})
        let header = serde_cbor::to_vec(&serde_cbor::Value::Map(
            vec![
                (
                    serde_cbor::Value::Text("version".into()),
                    serde_cbor::Value::Integer(1),
                ),
                (
                    serde_cbor::Value::Text("roots".into()),
                    serde_cbor::Value::Array(vec![]),
                ),
            ]
            .into_iter()
            .collect(),
        ))
        .unwrap();
        let mut data = Vec::new();
        encode_varint(header.len() as u64, &mut data);
        data.extend_from_slice(&header);
        for section in sections {
            data.extend_from_slice(section);
        }
        data
    }

    fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
        while value >= 0x80 {
            out.push((value as u8) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
    }

    #[test]
    fn reads_valid_sections() {
        let s1 = make_section(&[0xAA]);
        let s2 = make_section(&[0xBB]);
        let car = make_car(&[s1, s2]);

        let mut reader = MmapNodeReader::from_bytes(&car).unwrap();
        let n1 = reader.read_node().unwrap();
        assert!(n1.is_some());
        let n2 = reader.read_node().unwrap();
        assert!(n2.is_some());
        let n3 = reader.read_node().unwrap();
        assert!(n3.is_none());
    }

    #[test]
    fn recovers_from_corrupt_varint() {
        let s1 = make_section(&[0xAA]);
        let mut car = make_car(&[s1]);
        // Inject corrupt bytes (all high bits set → invalid varint)
        car.extend_from_slice(&[0xFF; 12]);
        // Append a valid section after the corruption
        car.extend_from_slice(&make_section(&[0xBB]));

        let reader = MmapNodeReader::from_bytes(&car).unwrap();
        let nodes: Vec<_> = reader.filter_map(Result::ok).collect();
        // Should recover and read both valid sections
        assert_eq!(nodes.len(), 2, "expected 2 nodes, got {}", nodes.len());
        assert_eq!(nodes[0].get_data(), &[0xAA]);
        assert_eq!(nodes[1].get_data(), &[0xBB]);
    }

    #[test]
    fn recovers_from_oversized_section() {
        let s1 = make_section(&[0xAA]);
        let mut car = make_car(&[s1]);
        // Inject a varint encoding a huge section size (0x7FFFFFFF = ~2GB)
        encode_varint(0x7FFF_FFFF, &mut car);
        // Then a valid section
        car.extend_from_slice(&make_section(&[0xCC]));

        let reader = MmapNodeReader::from_bytes(&car).unwrap();
        let nodes: Vec<_> = reader.filter_map(Result::ok).collect();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].get_data(), &[0xAA]);
        assert_eq!(nodes[1].get_data(), &[0xCC]);
    }

    #[test]
    fn iterator_terminates_on_unrecoverable_corruption() {
        // All garbage — no valid section to recover to
        let mut car = make_car(&[make_section(&[0xAA])]);
        car.extend_from_slice(&[0xFF; 64]);

        let reader = MmapNodeReader::from_bytes(&car).unwrap();
        let nodes: Vec<_> = reader.collect::<Vec<_>>();
        // Should get the first valid node, then terminate (not loop forever)
        let ok_count = nodes.iter().filter(|r| r.is_ok()).count();
        assert_eq!(ok_count, 1);
    }

    #[test]
    fn eof_is_not_treated_as_error() {
        let car = make_car(&[make_section(&[0xAA])]);
        let mut reader = MmapNodeReader::from_bytes(&car).unwrap();
        assert!(reader.read_node().unwrap().is_some());
        assert!(reader.read_node().unwrap().is_none());
        // Calling again after EOF should still be None, not error
        assert!(reader.read_node().unwrap().is_none());
    }
}
