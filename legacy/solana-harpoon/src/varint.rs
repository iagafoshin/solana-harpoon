// Based on https://github.com/tokio-rs/prost/blob/master/prost/src/encoding/varint.rs
use {
    bytes::Buf,
    std::io,
    thiserror::Error,
    tokio::io::{AsyncRead, AsyncReadExt},
};

#[derive(Debug, Error)]
pub enum VarIntError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("invalid varint")]
    Invalid,
}

const MAX_VARINT_LEN_64: usize = 10;

#[inline]
pub async fn read<R: AsyncRead + Unpin>(reader: &mut R) -> Result<u64, VarIntError> {
    let mut x = 0u64;
    let mut s = 0u32;
    for i in 0..MAX_VARINT_LEN_64 {
        let b = reader.read_u8().await?;
        if b < 0x80 {
            return if i == MAX_VARINT_LEN_64 - 1 && b > 1 {
                Err(VarIntError::Invalid)
            } else {
                Ok(x | ((b as u64) << s))
            };
        }
        x |= ((b & 0x7f) as u64) << s;
        s += 7;

        if s > 63 {
            return Err(VarIntError::Invalid);
        }
    }
    Err(VarIntError::Invalid)
}

/// Decodes a LEB128-encoded variable length integer from the buffer.
#[inline]
pub fn decode_varint(buf: &mut impl Buf) -> Result<u64, VarIntError> {
    let bytes = buf.chunk();
    let len = bytes.len();
    if len == 0 {
        return Err(VarIntError::Invalid);
    }

    let byte = bytes[0];
    if byte < 0x80 {
        buf.advance(1);
        Ok(u64::from(byte))
    } else if len > 10 || bytes[len - 1] < 0x80 {
        let (value, advance) = decode_varint_slice(bytes)?;
        buf.advance(advance);
        Ok(value)
    } else {
        decode_varint_slow(buf)
    }
}

/// Decodes a LEB128-encoded variable length integer from the slice, returning the value and the
/// number of bytes read.
///
/// Based loosely on [`ReadVarint64FromArray`][1] with a varint overflow check from
/// [`ConsumeVarint`][2].
///
/// ## Safety
///
/// The caller must ensure that `bytes` is non-empty and either `bytes.len() >= 10` or the last
/// element in bytes is < `0x80`.
///
/// [1]: https://github.com/google/protobuf/blob/3.3.x/src/google/protobuf/io/coded_stream.cc#L365-L406
/// [2]: https://github.com/protocolbuffers/protobuf-go/blob/v1.27.1/encoding/protowire/wire.go#L358
#[inline]
fn decode_varint_slice(bytes: &[u8]) -> Result<(u64, usize), VarIntError> {
    // Fully unrolled varint decoding loop. Splitting into 32-bit pieces gives better performance.

    // Use assertions to ensure memory safety, but it should always be optimized after inline.
    assert!(!bytes.is_empty());
    assert!(bytes.len() > 10 || bytes[bytes.len() - 1] < 0x80);

    let mut b: u8 = unsafe { *bytes.get_unchecked(0) };
    let mut part0: u32 = u32::from(b);
    if b < 0x80 {
        return Ok((u64::from(part0), 1));
    };
    part0 -= 0x80;
    b = unsafe { *bytes.get_unchecked(1) };
    part0 += u32::from(b) << 7;
    if b < 0x80 {
        return Ok((u64::from(part0), 2));
    };
    part0 -= 0x80 << 7;
    b = unsafe { *bytes.get_unchecked(2) };
    part0 += u32::from(b) << 14;
    if b < 0x80 {
        return Ok((u64::from(part0), 3));
    };
    part0 -= 0x80 << 14;
    b = unsafe { *bytes.get_unchecked(3) };
    part0 += u32::from(b) << 21;
    if b < 0x80 {
        return Ok((u64::from(part0), 4));
    };
    part0 -= 0x80 << 21;
    let value = u64::from(part0);

    b = unsafe { *bytes.get_unchecked(4) };
    let mut part1: u32 = u32::from(b);
    if b < 0x80 {
        return Ok((value + (u64::from(part1) << 28), 5));
    };
    part1 -= 0x80;
    b = unsafe { *bytes.get_unchecked(5) };
    part1 += u32::from(b) << 7;
    if b < 0x80 {
        return Ok((value + (u64::from(part1) << 28), 6));
    };
    part1 -= 0x80 << 7;
    b = unsafe { *bytes.get_unchecked(6) };
    part1 += u32::from(b) << 14;
    if b < 0x80 {
        return Ok((value + (u64::from(part1) << 28), 7));
    };
    part1 -= 0x80 << 14;
    b = unsafe { *bytes.get_unchecked(7) };
    part1 += u32::from(b) << 21;
    if b < 0x80 {
        return Ok((value + (u64::from(part1) << 28), 8));
    };
    part1 -= 0x80 << 21;
    let value = value + ((u64::from(part1)) << 28);

    b = unsafe { *bytes.get_unchecked(8) };
    let mut part2: u32 = u32::from(b);
    if b < 0x80 {
        return Ok((value + (u64::from(part2) << 56), 9));
    };
    part2 -= 0x80;
    b = unsafe { *bytes.get_unchecked(9) };
    part2 += u32::from(b) << 7;
    // Check for u64::MAX overflow. See [`ConsumeVarint`][1] for details.
    // [1]: https://github.com/protocolbuffers/protobuf-go/blob/v1.27.1/encoding/protowire/wire.go#L358
    if b < 0x02 {
        return Ok((value + (u64::from(part2) << 56), 10));
    };

    // We have overrun the maximum size of a varint (10 bytes) or the final byte caused an overflow.
    // Assume the data is corrupt.
    Err(VarIntError::Invalid)
}

/// Decodes a LEB128-encoded variable length integer from the buffer, advancing the buffer as
/// necessary.
///
/// Contains a varint overflow check from [`ConsumeVarint`][1].
///
/// [1]: https://github.com/protocolbuffers/protobuf-go/blob/v1.27.1/encoding/protowire/wire.go#L358
#[inline(never)]
#[cold]
fn decode_varint_slow(buf: &mut impl Buf) -> Result<u64, VarIntError> {
    let mut value = 0;
    for count in 0..std::cmp::min(10, buf.remaining()) {
        let byte = buf.get_u8();
        value |= u64::from(byte & 0x7F) << (count * 7);
        if byte <= 0x7F {
            // Check for u64::MAX overflow. See [`ConsumeVarint`][1] for details.
            // [1]: https://github.com/protocolbuffers/protobuf-go/blob/v1.27.1/encoding/protowire/wire.go#L358
            if count == 9 && byte >= 0x02 {
                return Err(VarIntError::Invalid);
            } else {
                return Ok(value);
            }
        }
    }

    Err(VarIntError::Invalid)
}
