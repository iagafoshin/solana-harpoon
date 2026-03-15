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
        let (header_len, consumed) = self.read_varint()?;
        let header_len = header_len as usize;
        if header_len > MAX_ALLOWED_HEADER_SIZE {
            return Err(NodeError::HeaderTooLong(header_len));
        }
        if self.pos + header_len > self.mmap.len() {
            return Err(NodeError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "CAR header truncated",
            )));
        }
        self.header_end = self.pos + header_len;
        // consumed already advanced pos via read_varint
        // but we need to store the header_end correctly
        self.header_end = consumed + header_len;
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

        let (section_size, _) = match self.read_varint() {
            Ok(v) => v,
            Err(NodeError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        let section_size = section_size as usize;
        if section_size > MAX_ALLOWED_SECTION_SIZE {
            return Err(NodeError::SectionTooLong(section_size));
        }
        if self.pos + section_size > self.mmap.len() {
            return Err(NodeError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "CAR section truncated",
            )));
        }

        let section = self.mmap[self.pos..self.pos + section_size].to_vec();
        self.pos += section_size;
        RawNode::new_from_vec(section).map(Some)
    }
}

impl Iterator for MmapNodeReader {
    type Item = Result<RawNode, NodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_node() {
            Ok(Some(node)) => Some(Ok(node)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
