use {
    crate::{
        node::{Kind, NodeError},
        util,
    },
    cid::Cid,
};

// type Subset struct {
//   kind   Int
//   # First slot in this subset.
//   first  Int
//   # Last slot in this subset.
//   last   Int
//   # The list of blocks in this subset.
//   blocks [ Link ] # [ &Block ]
// } representation tuple
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Subset {
    pub first: u64,
    pub last: u64,
    pub blocks: Vec<Cid>,
}

impl TryFrom<&[u8]> for Subset {
    type Error = NodeError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from(serde_cbor::from_slice::<serde_cbor::Value>(value)?)
    }
}

impl TryFrom<serde_cbor::Value> for Subset {
    type Error = NodeError;

    fn try_from(value: serde_cbor::Value) -> Result<Self, Self::Error> {
        let mut node = Self::default();
        for (index, value) in util::cbor::get_array(value, "Subset")?
            .into_iter()
            .enumerate()
        {
            match index {
                0 => NodeError::assert_invalid_kind(
                    util::cbor::get_int(value, "Subset::kind")? as u64,
                    Kind::Subset,
                )?,
                1 => node.first = util::cbor::get_int(value, "Subset::first")? as u64,
                2 => node.last = util::cbor::get_int(value, "Subset::last")? as u64,
                3 => {
                    node.blocks =
                        util::cbor::get_array_cids(value, "Subset::blocks", "Subset::blocks[]")?
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
        node::Subset,
        util::tests::{decode_cids, decode_hex},
    };

    #[test]
    fn test_decoding() {
        for (bytes, frame) in [
            (
                decode_hex(
                    "84031a010114841a0101937a99000ad82a58250001711220ab2c6543301eb5332c108f07bc3ee9f20d7e83b1ce5327086d376a6cf644bcbed82a582500017112202967b25da38503c5f67bae202c374bd16f76b9f6aed3d1567f24874e54911255d82a58250001711220e889d892d96f7606049d199532fcb485466bfca7b87636c01175f4755edd3e48d82a58250001711220b69c510735757d3880d2abed3b12cbeaf988003c87cd4bc9887c621ff7be4fb2d82a582500017112204a6b59bd3f04fc70e1fa7f8855606978c7f5750a88bafe9c6affaee2eecbcc87d82a58250001711220d67fdbe7ac914e108ccb6116496b4294c4c6b317e8f8251a82d97d9d8b9eb18fd82a58250001711220c056ee5c5ed002fb5413976433fad3933aaf465f3c7997afd2e54b4fcdcd799cd82a58250001711220b8078200dbf4eb333ec5e38ae80cb5c7f23e6f7977b724a3fcc77b92b52df4f6d82a58250001711220c09562a9cb40336a05b8286f78bc6735338bf52440fa591e5e97384e5d627f51d82a5825000171122063294ec3eddc4a554d1a0b4d149c0bbc376b065cb299fa7b2d887485ff447724",
                ),
                Subset {
                    first: 16848004,
                    last: 16880506,
                    blocks: decode_cids([
                        "bafyreiflfrsugma6wuzsyeepa66d52psbv7ihmookmtqq3jxnjwpmrf4xy",
                        "bafyreibjm6zf3i4fapc7m65oeawdos6rn53lt5vo2pivm7zeq5hfjeisku",
                        "bafyreihirhmjfwlpoydajhizsuzpznefizv7zj5yoy3maelv6r2v5xj6ja",
                        "bafyreifwtriqonlvpu4ibuvl5u5rfs7k7geaapehzvf4tcd4mip7ppspwi",
                        "bafyreicknnm32pye7ryod6t7rbkwa2lyy72xkcuixl7jy2x7v3ro5s6mq4",
                        "bafyreigwp7n6plerjyiizs3bczewwquuytdlgf7i7asrvawzpwoyxhvrr4",
                        "bafyreigak3xfyxwqal5vie4xmqz7vu4thkxumxz4pgl27uxfjnh43tlztq",
                        "bafyreifya6babw7u5mzt5rpdrluaznoh6i7g66lxw4skh7ghpojlklpu6y",
                        "bafyreigasvrkts2agnvalobin54lyzzvgof7kjca7jmr4xuxhbhf2yt7ke",
                        "bafyreiddffhmh3o4jjku2gqljukjyc54g5vqmxfsth5hwlmiosc76rdxeq",
                    ]),
                },
            ),
            (
                decode_hex(
                    "84031a0101937b1a0101f65f99000ad82a58250001711220dfe417f29d967098c6990550863ab10d1ffe40c6f49dd9a41be01f3017e5f9f6d82a58250001711220aa358b3fef4f114b326bfaca0a72c5eca6ccd452d4caa7269379da0a6d318ba5d82a5825000171122068aabfe57e66c386d50e1ccad6b4a6e537845fa28b33434096991d87313c66d2d82a5825000171122093f1e7d2018df1f385a113d7321647e4b0909e80618b5d7c13225805aa10527ed82a58250001711220c0a0f17f5e4bf169b148d8ed8fed50b17b1a03a386376adc8206314b653a75b9d82a58250001711220a6ec4c47d6cf60060c98f7859242866a3c6e37449e92b727773da9cadc158aafd82a5825000171122092e8a61244ffc650eab6c7de6a6ec89a05762889414fc70bf594323292c40ba7d82a582500017112206f9e9f0709ebb6f80a668f56a0daa52b36c8e320da2c24e6bcf50369d7d07811d82a58250001711220d90e4d3d8e41f059b8f51b102325b5288e56e5db1013043b09188422a70e0eedd82a582500017112200ce0140f61861630ba9c0fed6964368cb04641ed535fe0c9a35363e2c48ff03f",
                ),
                Subset {
                    first: 16880507,
                    last: 16905823,
                    blocks: decode_cids([
                        "bafyreig74ql7fhmwocmmngifkcddvmind77ebrxutxm2ig7ad4ybpzpz6y",
                        "bafyreifkgwft732pcffte272zifhfrpmu3gniuwuzktsne3z3ifg2mmluu",
                        "bafyreidivk76k7tgyodnkdq4zllljjxfg6cf7iulgnbubfuzdwdtcpdg2i",
                        "bafyreiet6ht5eamn6hzyliit24zbmr7ewcij5adbrnoxyezclac2uecspy",
                        "bafyreigaudyx6xsl6fu3csgy5wh62ufrpmnahi4gg5vnzaqggffwkotvxe",
                        "bafyreifg5rgepvwpmadazghxqwjefbtkhrxdore6sk3so5z5vhfnyfmkv4",
                        "bafyreies5ctberh7yziovnwh3zvg5se2av3crckbj7dqx5mugizjfralu4",
                        "bafyreidpt2pqocplw34auzupk2qnvjjlg3eogig2fqsonphvanu5pudyce",
                        "bafyreigzbzgt3dsb6bm3r5i3carslnjirzlolwyqcmcdwciyqqrkodqo5u",
                        "bafyreiam4aka6ymgcyylvhap5vuwinumwbded3ktl7qmti2tmprmjd7qh4",
                    ]),
                },
            ),
        ] {
            let node = Subset::try_from(bytes.as_ref()).expect("valid node");
            assert_eq!(node, frame);
        }
    }
}
