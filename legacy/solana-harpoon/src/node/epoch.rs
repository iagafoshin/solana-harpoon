use {
    crate::{
        node::{Kind, NodeError},
        util,
    },
    cid::Cid,
};

// # Epoch is the top-level data structure in the DAG. It contains a list of
// # subsets, which in turn contain a list of blocks. Each block contains a list
// # of entries, which in turn contain a list of transactions.
// type Epoch struct {
//   # The kind of this object. This is used to determine which fields are
//   # present, and how to interpret them. This is useful for knowing which
//   # type of object to deserialize.
//   kind   Int
//   # The epoch number.
//   epoch  Int
//   # The list of subsets in this epoch.
//   subsets [ Link ] # [ &Subset ]
// } representation tuple
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Epoch {
    pub epoch: u64,
    pub subsets: Vec<Cid>,
}

impl TryFrom<&[u8]> for Epoch {
    type Error = NodeError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from(serde_cbor::from_slice::<serde_cbor::Value>(value)?)
    }
}

impl TryFrom<serde_cbor::Value> for Epoch {
    type Error = NodeError;

    fn try_from(value: serde_cbor::Value) -> Result<Self, Self::Error> {
        let mut node = Self::default();
        for (index, value) in util::cbor::get_array(value, "Epoch")?
            .into_iter()
            .enumerate()
        {
            match index {
                0 => NodeError::assert_invalid_kind(
                    util::cbor::get_int(value, "Epoch::kind")? as u64,
                    Kind::Epoch,
                )?,
                1 => node.epoch = util::cbor::get_int(value, "Epoch::epoch")? as u64,
                2 => {
                    node.subsets =
                        util::cbor::get_array_cids(value, "Epoch::subsets", "Epoch::subsets[]")?;
                }
                _ => return Err(NodeError::UnexpectedCborValues),
            }
        }
        Ok(node)
    }
}

#[cfg(test)]
mod epoch_tests {
    use crate::{
        node::Epoch,
        util::tests::{decode_cids, decode_hex},
    };

    #[test]
    fn test_decoding() {
        for (bytes, frame) in [
            (
                decode_hex(
                    "8304182792d82a5825000171122012fac2c2f811a3e3e2495966acc1eee162fc3fa0882543bc8c9ef6f92af0b09ed82a582500017112208de8872079008d34b9877cf41d3008d5ce22a0e285c7fad82e3f7fbf01fcc17ad82a582500017112201cd701f20b63bebb1d866f47b42615e93e92c2b0b12fbdaeec4ef11e5b65b416d82a58250001711220287605543e8fc96e00ebd981780b87e63c7d1cea1fbf13c2097af03c44b2cdb1d82a58250001711220bdc9c9b7cc0d7b6c583fc21a09b1e39e86d508ce2fa51f17bf316c9d99d58358d82a58250001711220fedf995b8e220b82ba33bd1afb43db939013a2530852ac0f71c8f81c585b4aa4d82a582500017112204166b74ade924fbf19601dda7c116e2eac74212f1b7d50b4a4cb7f0b1c3ece4bd82a58250001711220a79a9ac6de2df05f569afb9e442e9de666bb9f67a872376dfa2c1c476c52e773d82a5825000171122052474247c71be080ea78a06b8fa7407ecf2e488d86605a0a9d66548108630938d82a582500017112200ae9337ace584d9f671c81c30c730c6b519217c15629e079256241c4de837b74d82a58250001711220c2977e0f7131b509436b286bc029d573e9710e3563828e7fc8e17a2e35302538d82a58250001711220c10b58bc40088967533ec8fe7efa2f8c74cf107dddd877899cb1d1a4304da688d82a58250001711220a19437b2e599c2318db8dfdb59357fd514ffe1fe221ab5c6e4a64d08184d441ad82a5825000171122003909d5d44f3ffb94b449cfb1205ced253e434abfe094595093f5bd9840f852ad82a582500017112207c6ec145ca55d729c296c6f599841309756e711e89e77526d3339a037d5434e5d82a58250001711220372223bc584b938ae76c11f2359daa175a68f56c67b5346ca04313f5f4c496aad82a58250001711220fe48dafbfa127e5e7d66636e0d5e7012343e416a9b804592154e67f48107b0bdd82a582500017112202ce52cdd8645483d0f95983e5f34ffbe452c2ebc64243da5b336ac83958f8fcb",
                ),
                Epoch {
                    epoch: 39,
                    subsets: decode_cids([
                        "bafyreias7lbmf6arupr6eskzm2wmd3xbml6d7ieievb3zde6634sv4fqty",
                        "bafyreien5cdsa6iaru2ltb346qotacgvzyrkbyufy75nqlr7p67qd7gbpi",
                        "bafyreia424a7ec3dx25r3btpi62cmfpjh2jmfmfrf66253co6epfwznucy",
                        "bafyreibioycvipupzfxab26zqf4axb7ghr6rz2q7x4j4ecl26a6ejmwnwe",
                        "bafyreif5zhe3ptanpnwfqp6cdie3dy46q3kqrtrpuuprppzrnsoztvmdla",
                        "bafyreih636mvxdrcboblum55dl5uhw4tsaj2euyikkwa64oi7aofqw2kuq",
                        "bafyreicbm23uvxusj67rsya53j6bc3rovr2ccly3pviljjglp4frypwojm",
                        "bafyreifhtknmnxrn6bpvngx3tzcc5hpgm25z6z5ioi3w36rmdrdwyuxhom",
                        "bafyreicsi5bepry34caou6fanoh2oqd6z4xerdmgmbnavhlgksaqqyyjha",
                        "bafyreiak5ezxvtsyjwpwohebymghgddlkgjbpqkwfhqhsjlcihcn5a33oq",
                        "bafyreigcs57a64jrwueug2zinpactvlt5fyq4nldqkhh7shbpixdkmbfha",
                        "bafyreigbbnmlyqairftvgpwi7z7pul4mothra7o53b3ythfr2gsdatngra",
                        "bafyreifbsq33fzmzyiyy3og73nmtk76vct76d7rcdk24nzfgjuebqtkedi",
                        "bafyreiadscov2rht764uwre47mjaltwskpsdjk76bfczkcj7lpmyid4ffi",
                        "bafyreid4n3aulssv24u4ffwg6wmyieyjovxhchuj452snuzttibx2vbu4u",
                        "bafyreibxeir3ywclsofoo3ar6i2z3kqxljupk3dhwu2gzicdcp27jrewvi",
                        "bafyreih6jdnpx6qspzph2ztdnygv44asgq7ec2u3qbczefkom72icb5qxu",
                        "bafyreibm4uwn3bsfja6q7fmyhzptj756iuwc5pdeeq62lmzwvsbzld4pzm",
                    ]),
                },
            ),
            (
                decode_hex(
                    "8304187882d82a5825000171122059760f2fd3f4944861167ddf07169a83ef4a44731953b567bcdd4ab8ab31f8afd82a582500017112206ff31291895c0afc711fbfa2ec699ad3b18fb4ad3db49a9b3cf4dd83d59a4446",
                ),
                Epoch {
                    epoch: 120,
                    subsets: decode_cids([
                        "bafyreiczoyhs7u7usregcft534drngud55fei4yzko2wppg5jk4kwmpyv4",
                        "bafyreidp6mjjdck4bl6hch57ulwgtgwtwgh3jlj5wsnjwphu3wb5lgseiy",
                    ]),
                },
            ),
        ] {
            let node = Epoch::try_from(bytes.as_ref()).expect("valid node");
            assert_eq!(node, frame);
        }
    }
}
