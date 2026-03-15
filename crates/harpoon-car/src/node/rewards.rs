use crate::{
    node::{DataFrame, Kind, NodeError},
    util,
};

// type Rewards struct {
//   kind       Int
//   # The slot number for which these rewards are for.
//   slot       Int
//   # The raw rewards data.
//   data       DataFrame
// } representation tuple
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Rewards {
    pub slot: u64,
    pub data: DataFrame,
}

impl TryFrom<&[u8]> for Rewards {
    type Error = NodeError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from(serde_cbor::from_slice::<serde_cbor::Value>(value)?)
    }
}

impl TryFrom<serde_cbor::Value> for Rewards {
    type Error = NodeError;

    fn try_from(value: serde_cbor::Value) -> Result<Self, Self::Error> {
        let mut node = Self::default();
        for (index, value) in util::cbor::get_array(value, "Rewards")?
            .into_iter()
            .enumerate()
        {
            match index {
                0 => NodeError::assert_invalid_kind(
                    util::cbor::get_int(value, "Rewards::kind")? as u64,
                    Kind::Rewards,
                )?,
                1 => node.slot = util::cbor::get_int(value, "Rewards::slot")? as u64,
                2 => node.data = DataFrame::try_from(value)?,
                _ => return Err(NodeError::UnexpectedCborValues),
            }
        }
        Ok(node)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        node::{DataFrame, Rewards},
        util::tests::decode_hex,
    };

    #[test]
    fn test_decoding() {
        #[allow(clippy::single_element_loop)]
        for (bytes, frame) in [(
            decode_hex("83051a010114848506f6f6f65528b52ffd04004100000000000000000000bb1bdbca"),
            Rewards {
                slot: 16848004,
                data: DataFrame {
                    hash: None,
                    index: None,
                    total: None,
                    data: decode_hex("28b52ffd04004100000000000000000000bb1bdbca"),
                    next: vec![],
                },
            },
        )] {
            let node = Rewards::try_from(bytes.as_ref()).expect("valid node");
            assert_eq!(node, frame);
        }
    }
}
