use {
    crate::{
        node::{Kind, NodeError},
        util,
    },
    cid::Cid,
};

// type Entry struct {
//   kind         Int
//   numHashes    Int
//   hash         Hash
//   # The list of transactions in this entry.
//   transactions [ Link ] # [ &Transaction ]
// } representation tuple
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Entry {
    pub num_hashes: u64,
    pub hash: Vec<u8>,
    pub transactions: Vec<Cid>,
}

impl TryFrom<&[u8]> for Entry {
    type Error = NodeError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from(serde_cbor::from_slice::<serde_cbor::Value>(value)?)
    }
}

impl TryFrom<serde_cbor::Value> for Entry {
    type Error = NodeError;

    fn try_from(value: serde_cbor::Value) -> Result<Self, Self::Error> {
        let mut node = Self::default();
        for (index, value) in util::cbor::get_array(value, "Entry")?
            .into_iter()
            .enumerate()
        {
            match index {
                0 => NodeError::assert_invalid_kind(
                    util::cbor::get_int(value, "Entry::kind")? as u64,
                    Kind::Entry,
                )?,
                1 => node.num_hashes = util::cbor::get_int(value, "Entry::num_hashes")? as u64,
                2 => node.hash = util::cbor::get_bytes(value, "Entry::hash")?,
                3 => {
                    node.transactions = util::cbor::get_array_cids(
                        value,
                        "Entry::transactions",
                        "Entry::transactions[]",
                    )?
                }
                _ => return Err(NodeError::UnexpectedCborValues),
            }
        }
        Ok(node)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        node::Entry,
        util::tests::{decode_cids, decode_hex},
    };

    #[test]
    fn test_decoding() {
        for (bytes, frame) in [
            (
                decode_hex(
                    "84011930d458203a43cd82e140873740fde924da4125ac30e2fec5eb92344dbb2bb4776973feec80",
                ),
                Entry {
                    num_hashes: 12500,
                    hash: decode_hex(
                        "3a43cd82e140873740fde924da4125ac30e2fec5eb92344dbb2bb4776973feec",
                    ),
                    transactions: vec![],
                },
            ),
            (
                decode_hex(
                    "84011930d45820b12c324e55fb861ce6ef0d315ed3115bea52f6bec83cf09c9872c70de69fdfea80",
                ),
                Entry {
                    num_hashes: 12500,
                    hash: decode_hex(
                        "b12c324e55fb861ce6ef0d315ed3115bea52f6bec83cf09c9872c70de69fdfea",
                    ),
                    transactions: vec![],
                },
            ),
            (
                decode_hex(
                    "84011930d45820475c39d0431d1479a35fa3499e0a8dd6e472254f5f734408a896a9fda521999580",
                ),
                Entry {
                    num_hashes: 12500,
                    hash: decode_hex(
                        "475c39d0431d1479a35fa3499e0a8dd6e472254f5f734408a896a9fda5219995",
                    ),
                    transactions: vec![],
                },
            ),
            (
                decode_hex(
                    "8401192f93582087b3f95ad785a5e8c7b5ffae44b37c200c27d5464870545489560c217a48d79881d82a582500017112203894a7fbed75c8e2b5864f7383dce88f1443e0b33082c57be255553826546ae1",
                ),
                Entry {
                    num_hashes: 12179,
                    hash: decode_hex(
                        "87b3f95ad785a5e8c7b5ffae44b37c200c27d5464870545489560c217a48d798",
                    ),
                    transactions: decode_cids([
                        "bafyreibysst7x3lvzdrllbspoob5z2epcrb6bmzqqlcxxysvku4cmvdk4e",
                    ]),
                },
            ),
        ] {
            let node = Entry::try_from(bytes.as_ref()).expect("valid node");
            assert_eq!(node, frame);
        }
    }
}
