use {
    std::io,
    tokio::io::{AsyncRead, AsyncReadExt},
};

#[inline]
pub async fn read_exact<R: AsyncRead + Unpin>(reader: &mut R, size: usize) -> io::Result<Vec<u8>> {
    let mut buffer = Vec::with_capacity(size);
    // SAFETY: All `size` bytes would be initialized by read_exact
    #[allow(clippy::uninit_vec)]
    unsafe {
        buffer.set_len(size)
    }
    reader.read_exact(&mut buffer).await?;
    Ok(buffer)
}

pub mod cbor {
    use {crate::node::NodeError, cid::Cid, serde_cbor::Value};

    #[inline]
    pub fn get_array(value: Value, path: &'static str) -> Result<Vec<Value>, NodeError> {
        match value {
            Value::Array(vec) => Ok(vec),
            _ => Err(NodeError::UnexpectedCborValue {
                path,
                kind: "Array",
            }),
        }
    }

    #[inline]
    pub fn get_array_opt(
        value: Value,
        path: &'static str,
    ) -> Result<Option<Vec<Value>>, NodeError> {
        match value {
            Value::Array(vec) => Ok(Some(vec)),
            Value::Null => Ok(None),
            _ => Err(NodeError::UnexpectedCborValue {
                path,
                kind: "Array/Null",
            }),
        }
    }

    #[inline]
    pub fn get_int(value: Value, path: &'static str) -> Result<i128, NodeError> {
        match value {
            Value::Integer(value) => Ok(value),
            _ => Err(NodeError::UnexpectedCborValue {
                path,
                kind: "Integer",
            }),
        }
    }

    #[inline]
    pub fn get_int_opt(value: Value, path: &'static str) -> Result<Option<i128>, NodeError> {
        match value {
            Value::Integer(value) => Ok(Some(value)),
            Value::Null => Ok(None),
            _ => Err(NodeError::UnexpectedCborValue {
                path,
                kind: "Integer/Null",
            }),
        }
    }

    #[inline]
    pub fn get_bytes(value: Value, path: &'static str) -> Result<Vec<u8>, NodeError> {
        match value {
            Value::Bytes(value) => Ok(value),
            _ => Err(NodeError::UnexpectedCborValue {
                path,
                kind: "Bytes",
            }),
        }
    }

    #[inline]
    pub fn get_cid(value: Value, path: &'static str) -> Result<Cid, NodeError> {
        Cid::try_from(&get_bytes(value, path)?[1..]).map_err(Into::into)
    }

    #[inline]
    pub fn get_array_cids(
        value: Value,
        path: &'static str,
        path2: &'static str,
    ) -> Result<Vec<Cid>, NodeError> {
        get_array(value, path)?
            .into_iter()
            .map(|value| get_cid(value, path2))
            .collect::<Result<Vec<Cid>, NodeError>>()
    }
}

#[cfg(test)]
pub mod tests {
    use {cid::Cid, const_hex::decode};

    pub fn decode_hex<T: AsRef<[u8]>>(input: T) -> Vec<u8> {
        decode(input).unwrap()
    }

    pub fn decode_cid(cid: &'static str) -> Cid {
        Cid::try_from(cid).unwrap()
    }

    pub fn decode_cids<const N: usize>(cids: [&'static str; N]) -> Vec<Cid> {
        cids.into_iter().map(decode_cid).collect()
    }
}
