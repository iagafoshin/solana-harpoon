use crate::{
    node::{DataFrame, Kind, NodeError},
    util,
};

// type Transaction struct {
//   kind     Int
//   # Raw transaction data.
//   data     DataFrame
//   # Raw tx metadata data.
//   metadata DataFrame
//   # The slot number where this transaction was created.
//   slot     Int
//   # The index of the position of this transaction in the block (0-indexed).
//   index nullable optional  Int
// } representation tuple
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Transaction {
    pub data: DataFrame,
    pub metadata: DataFrame,
    pub slot: u64,
    pub index: Option<u64>,
}

impl TryFrom<&[u8]> for Transaction {
    type Error = NodeError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from(serde_cbor::from_slice::<serde_cbor::Value>(value)?)
    }
}

impl TryFrom<serde_cbor::Value> for Transaction {
    type Error = NodeError;

    fn try_from(value: serde_cbor::Value) -> Result<Self, Self::Error> {
        let mut node = Self::default();
        for (index, value) in util::cbor::get_array(value, "Transaction")?
            .into_iter()
            .enumerate()
        {
            match index {
                0 => NodeError::assert_invalid_kind(
                    util::cbor::get_int(value, "Transaction::kind")? as u64,
                    Kind::Transaction,
                )?,
                1 => node.data = DataFrame::try_from(value)?,
                2 => node.metadata = DataFrame::try_from(value)?,
                3 => node.slot = util::cbor::get_int(value, "Transaction::slot")? as u64,
                4 => {
                    node.index =
                        util::cbor::get_int_opt(value, "Transaction::index")?.map(|v| v as u64)
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
        node::{DataFrame, Transaction},
        util::tests::decode_hex,
    };

    #[test]
    fn test_decoding() {
        for (bytes, frame) in [
            (
                decode_hex(
                    "85008506f6f6f659014a0186d331474ac0e7cb3c57b2f80c3272d681b62cdb9b30381a22a91f08fee19adf289bbec7297aedf8d903a367d4ff1b839ed5dce9ee6559945b2c7c79221d1308010003050519b878d66540b318cc869f2241c41b76c29f0d1f21963e66ab7f8ad9c62ea70519b86ca395d378c9f90207463a258b4251cc3e5503eebbb6386d6492e4234a06a7d517192f0aafc6f265e3fb77cc7ada82c529d0be3b136e2d00552000000006a7d51718c774c928566398691d5eb68b5eb8a39b4b6d5c73555b21000000000761481d357474bb7c4d7624ebd3bdb3d8355e73d11043fc0da3538000000000b63ccf219e96d69095a25e439c0c0b064cf01397d8f6792d5822cad9f0e8f10b010404010203003d0200000002000000000000007d140101000000007e14010100000000f2ab07b3930cc2f69326873efa418252fc869fda1ddabf127a1793282935b858008506f6f6f6583b28b52ffd040075010022420710d047013f3dd2289ffd137a292b8ff27d609cbda5855e0e11eafdc17c0500a77a08325e41d6ce1c6a285fededc4e21a0101148400",
                ),
                Transaction {
                    data: DataFrame {
                        hash: None,
                        index: None,
                        total: None,
                        data: decode_hex(
                            "0186d331474ac0e7cb3c57b2f80c3272d681b62cdb9b30381a22a91f08fee19adf289bbec7297aedf8d903a367d4ff1b839ed5dce9ee6559945b2c7c79221d1308010003050519b878d66540b318cc869f2241c41b76c29f0d1f21963e66ab7f8ad9c62ea70519b86ca395d378c9f90207463a258b4251cc3e5503eebbb6386d6492e4234a06a7d517192f0aafc6f265e3fb77cc7ada82c529d0be3b136e2d00552000000006a7d51718c774c928566398691d5eb68b5eb8a39b4b6d5c73555b21000000000761481d357474bb7c4d7624ebd3bdb3d8355e73d11043fc0da3538000000000b63ccf219e96d69095a25e439c0c0b064cf01397d8f6792d5822cad9f0e8f10b010404010203003d0200000002000000000000007d140101000000007e14010100000000f2ab07b3930cc2f69326873efa418252fc869fda1ddabf127a1793282935b85800",
                        ),
                        next: vec![],
                    },
                    metadata: DataFrame {
                        hash: None,
                        index: None,
                        total: None,
                        data: decode_hex(
                            "28b52ffd040075010022420710d047013f3dd2289ffd137a292b8ff27d609cbda5855e0e11eafdc17c0500a77a08325e41d6ce1c6a285fededc4e2",
                        ),
                        next: vec![],
                    },
                    slot: 16848004,
                    index: Some(0),
                },
            ),
            (
                decode_hex(
                    "85008506f6f6f659014201979f59bb61198e03ae559d7466c5b2d6f64ae28d1f6910254369e18dfe5ce0655d43fbeea9e339286d82c46f26a4be8f8ac9ed9b014f511ec7b42e57e0f4280a01000305ac160a70da65950df658ba0c09dd8f68bd41ca26d68b4e5410538d46d08ef6d37faea161abeabc23963667ed0916b677c8589c386c95f9e8642f84a3ac77e22506a7d517192f0aafc6f265e3fb77cc7ada82c529d0be3b136e2d00552000000006a7d51718c774c928566398691d5eb68b5eb8a39b4b6d5c73555b21000000000761481d357474bb7c4d7624ebd3bdb3d8355e73d11043fc0da353800000000004c91dd45076b6a025fbd93535e919f6fce36597860e94fab3c89e27fc74ae2501040401020300350200000001000000000000007f14010100000000228ceb8dfc438f10b988426cc9ba04f0fa5a33e0dec452f21e6ee9316ec4316d008506f6f6f6583c28b52ffd04007d010022820711e0490180faad52ae0baa4a1d914131be0f0a9dbd9d643e0259e2fda07c0500a77a08325e41d6ce1c6a285f107bdc661a0101148406",
                ),
                Transaction {
                    data: DataFrame {
                        hash: None,
                        index: None,
                        total: None,
                        data: decode_hex(
                            "01979f59bb61198e03ae559d7466c5b2d6f64ae28d1f6910254369e18dfe5ce0655d43fbeea9e339286d82c46f26a4be8f8ac9ed9b014f511ec7b42e57e0f4280a01000305ac160a70da65950df658ba0c09dd8f68bd41ca26d68b4e5410538d46d08ef6d37faea161abeabc23963667ed0916b677c8589c386c95f9e8642f84a3ac77e22506a7d517192f0aafc6f265e3fb77cc7ada82c529d0be3b136e2d00552000000006a7d51718c774c928566398691d5eb68b5eb8a39b4b6d5c73555b21000000000761481d357474bb7c4d7624ebd3bdb3d8355e73d11043fc0da353800000000004c91dd45076b6a025fbd93535e919f6fce36597860e94fab3c89e27fc74ae2501040401020300350200000001000000000000007f14010100000000228ceb8dfc438f10b988426cc9ba04f0fa5a33e0dec452f21e6ee9316ec4316d00",
                        ),
                        next: vec![],
                    },
                    metadata: DataFrame {
                        hash: None,
                        index: None,
                        total: None,
                        data: decode_hex(
                            "28b52ffd04007d010022820711e0490180faad52ae0baa4a1d914131be0f0a9dbd9d643e0259e2fda07c0500a77a08325e41d6ce1c6a285f107bdc66",
                        ),
                        next: vec![],
                    },
                    slot: 16848004,
                    index: Some(6),
                },
            ),
            (
                decode_hex(
                    "85008506f6f6f659014a014d382607c2c01cde335d25b86ba60ba327c7c21688ee6a86f1d2e66f52843a39b6f56714d47f88cf564e912c5fc29634b43a163c267733adc19565243250350e01000305be466418fd1e9f6e509a0be5860b61f08066a2ec777451dec8411668c0f80424ee4fe8b7ae1f01e9bfc9ab337a49b80a63da01474d88c0e2f4040529a5a52bb106a7d517192f0aafc6f265e3fb77cc7ada82c529d0be3b136e2d00552000000006a7d51718c774c928566398691d5eb68b5eb8a39b4b6d5c73555b21000000000761481d357474bb7c4d7624ebd3bdb3d8355e73d11043fc0da3538000000000b63ccf219e96d69095a25e439c0c0b064cf01397d8f6792d5822cad9f0e8f10b010404010203003d0200000002000000000000007d140101000000007e14010100000000f2ab07b3930cc2f69326873efa418252fc869fda1ddabf127a1793282935b858008506f6f6f6583c28b52ffd04007d010022820711d047010f7ea1abd7be88ff1e8dd4237c1f689cbd1d654e8248ebfda07c0500a77a08325e41d6ce1c6a285f16360ba81a0101148408",
                ),
                Transaction {
                    data: DataFrame {
                        hash: None,
                        index: None,
                        total: None,
                        data: decode_hex(
                            "014d382607c2c01cde335d25b86ba60ba327c7c21688ee6a86f1d2e66f52843a39b6f56714d47f88cf564e912c5fc29634b43a163c267733adc19565243250350e01000305be466418fd1e9f6e509a0be5860b61f08066a2ec777451dec8411668c0f80424ee4fe8b7ae1f01e9bfc9ab337a49b80a63da01474d88c0e2f4040529a5a52bb106a7d517192f0aafc6f265e3fb77cc7ada82c529d0be3b136e2d00552000000006a7d51718c774c928566398691d5eb68b5eb8a39b4b6d5c73555b21000000000761481d357474bb7c4d7624ebd3bdb3d8355e73d11043fc0da3538000000000b63ccf219e96d69095a25e439c0c0b064cf01397d8f6792d5822cad9f0e8f10b010404010203003d0200000002000000000000007d140101000000007e14010100000000f2ab07b3930cc2f69326873efa418252fc869fda1ddabf127a1793282935b85800",
                        ),
                        next: vec![],
                    },
                    metadata: DataFrame {
                        hash: None,
                        index: None,
                        total: None,
                        data: decode_hex(
                            "28b52ffd04007d010022820711d047010f7ea1abd7be88ff1e8dd4237c1f689cbd1d654e8248ebfda07c0500a77a08325e41d6ce1c6a285f16360ba8",
                        ),
                        next: vec![],
                    },
                    slot: 16848004,
                    index: Some(8),
                },
            ),
            (
                decode_hex(
                    "85008506f6f6f659014a01b8e13a65526fa74135fec571d591c17bcb0ce995782bc3747e2cad085b291c23d5849ecb1ba1a728204d9970ef4caa5d05fce153381010badbf0435772aa350001000305ac160a70da65950df658ba0c09dd8f68bd41ca26d68b4e5410538d46d08ef6d37faea161abeabc23963667ed0916b677c8589c386c95f9e8642f84a3ac77e22506a7d517192f0aafc6f265e3fb77cc7ada82c529d0be3b136e2d00552000000006a7d51718c774c928566398691d5eb68b5eb8a39b4b6d5c73555b21000000000761481d357474bb7c4d7624ebd3bdb3d8355e73d11043fc0da35380000000003973e330c29b831f3fcb0e49374ed8d0388f410a23e4ebf23328505036efbd03010404010203003d0200000001000000000000000000000000000000ab03405c54cdc42fa51ed682bd381389d60243f57397c4de1e76ad53d3d5624a019b8d6f5e000000008506f6f6f6400101",
                ),
                Transaction {
                    data: DataFrame {
                        hash: None,
                        index: None,
                        total: None,
                        data: decode_hex(
                            "01b8e13a65526fa74135fec571d591c17bcb0ce995782bc3747e2cad085b291c23d5849ecb1ba1a728204d9970ef4caa5d05fce153381010badbf0435772aa350001000305ac160a70da65950df658ba0c09dd8f68bd41ca26d68b4e5410538d46d08ef6d37faea161abeabc23963667ed0916b677c8589c386c95f9e8642f84a3ac77e22506a7d517192f0aafc6f265e3fb77cc7ada82c529d0be3b136e2d00552000000006a7d51718c774c928566398691d5eb68b5eb8a39b4b6d5c73555b21000000000761481d357474bb7c4d7624ebd3bdb3d8355e73d11043fc0da35380000000003973e330c29b831f3fcb0e49374ed8d0388f410a23e4ebf23328505036efbd03010404010203003d0200000001000000000000000000000000000000ab03405c54cdc42fa51ed682bd381389d60243f57397c4de1e76ad53d3d5624a019b8d6f5e00000000",
                        ),
                        next: vec![],
                    },
                    metadata: DataFrame {
                        hash: None,
                        index: None,
                        total: None,
                        data: vec![],
                        next: vec![],
                    },
                    slot: 1,
                    index: Some(1),
                },
            ),
            (
                decode_hex(
                    "85008506f6f6f659014a01d1da50cdb7e22c3abc50a981145a1782efadbdac0262a8911fc18336902c855bc5cb4d873484e48c7b42be8ac168ca88c63c9fe488a1614fb7b5cadbb810d2020100030508ae90b3fd803e8123e89901383d4cf54d2f8cac4863c90aafa34c5045b869c308ae90b3dd08bd4b5887ad3e4aa3d0880fb65a795cff6ce62f8f3df94c5c457406a7d517192f0aafc6f265e3fb77cc7ada82c529d0be3b136e2d00552000000006a7d51718c774c928566398691d5eb68b5eb8a39b4b6d5c73555b21000000000761481d357474bb7c4d7624ebd3bdb3d8355e73d11043fc0da3538000000000b63ccf219e96d69095a25e439c0c0b064cf01397d8f6792d5822cad9f0e8f10b010404010203003d0200000002000000000000007d140101000000007e14010100000000f2ab07b3930cc2f69326873efa418252fc869fda1ddabf127a1793282935b858008506f6f6f6583b28b52ffd040075010022420710e0490100a809189d525ab5da4351bf0f02587d9d7427c88c6b7f281f0500a77a08325e41d6ce1c6a285f8f0a43be1a0101148401",
                ),
                Transaction {
                    data: DataFrame {
                        hash: None,
                        index: None,
                        total: None,
                        data: decode_hex(
                            "01d1da50cdb7e22c3abc50a981145a1782efadbdac0262a8911fc18336902c855bc5cb4d873484e48c7b42be8ac168ca88c63c9fe488a1614fb7b5cadbb810d2020100030508ae90b3fd803e8123e89901383d4cf54d2f8cac4863c90aafa34c5045b869c308ae90b3dd08bd4b5887ad3e4aa3d0880fb65a795cff6ce62f8f3df94c5c457406a7d517192f0aafc6f265e3fb77cc7ada82c529d0be3b136e2d00552000000006a7d51718c774c928566398691d5eb68b5eb8a39b4b6d5c73555b21000000000761481d357474bb7c4d7624ebd3bdb3d8355e73d11043fc0da3538000000000b63ccf219e96d69095a25e439c0c0b064cf01397d8f6792d5822cad9f0e8f10b010404010203003d0200000002000000000000007d140101000000007e14010100000000f2ab07b3930cc2f69326873efa418252fc869fda1ddabf127a1793282935b85800",
                        ),
                        next: vec![],
                    },
                    metadata: DataFrame {
                        hash: None,
                        index: None,
                        total: None,
                        data: decode_hex(
                            "28b52ffd040075010022420710e0490100a809189d525ab5da4351bf0f02587d9d7427c88c6b7f281f0500a77a08325e41d6ce1c6a285f8f0a43be",
                        ),
                        next: vec![],
                    },
                    slot: 16848004,
                    index: Some(1),
                },
            ),
            (
                decode_hex(
                    "85008506f6f6f6590152010781d7b4370c6b00bf647a0666cceee91a5f26329d7566afe728699fd329fc8addf8c9b0442ed5f260ef010df7c73b0fe37f2a90448a2794ba6c9f45ca23a6020100030519ba7cf81e5526524c89d513f114bb7c37652dd740123e43f2c322ee0d839ba6b2ddb8106dba67d432b1b719861427fa256fdbd968d789a2de6ec4c494a8232d06a7d517192f0aafc6f265e3fb77cc7ada82c529d0be3b136e2d00552000000006a7d51718c774c928566398691d5eb68b5eb8a39b4b6d5c73555b21000000000761481d357474bb7c4d7624ebd3bdb3d8355e73d11043fc0da353800000000004c91dd45076b6a025fbd93535e919f6fce36597860e94fab3c89e27fc74ae2501040401020300450200000003000000000000007d140101000000007e140101000000007f14010100000000228ceb8dfc438f10b988426cc9ba04f0fa5a33e0dec452f21e6ee9316ec4316d008506f6f6f6583b28b52ffd040075010022420710e0490138f7686a9deb3e06d6180af97d81587d99533e0e89d47e701f0500a77a08325e41d6ce1c6a285f14abfc081a0101148404",
                ),
                Transaction {
                    data: DataFrame {
                        hash: None,
                        index: None,
                        total: None,
                        data: decode_hex(
                            "010781d7b4370c6b00bf647a0666cceee91a5f26329d7566afe728699fd329fc8addf8c9b0442ed5f260ef010df7c73b0fe37f2a90448a2794ba6c9f45ca23a6020100030519ba7cf81e5526524c89d513f114bb7c37652dd740123e43f2c322ee0d839ba6b2ddb8106dba67d432b1b719861427fa256fdbd968d789a2de6ec4c494a8232d06a7d517192f0aafc6f265e3fb77cc7ada82c529d0be3b136e2d00552000000006a7d51718c774c928566398691d5eb68b5eb8a39b4b6d5c73555b21000000000761481d357474bb7c4d7624ebd3bdb3d8355e73d11043fc0da353800000000004c91dd45076b6a025fbd93535e919f6fce36597860e94fab3c89e27fc74ae2501040401020300450200000003000000000000007d140101000000007e140101000000007f14010100000000228ceb8dfc438f10b988426cc9ba04f0fa5a33e0dec452f21e6ee9316ec4316d00",
                        ),
                        next: vec![],
                    },
                    metadata: DataFrame {
                        hash: None,
                        index: None,
                        total: None,
                        data: decode_hex(
                            "28b52ffd040075010022420710e0490138f7686a9deb3e06d6180af97d81587d99533e0e89d47e701f0500a77a08325e41d6ce1c6a285f14abfc08",
                        ),
                        next: vec![],
                    },
                    slot: 16848004,
                    index: Some(4),
                },
            ),
        ] {
            let node = Transaction::try_from(bytes.as_ref()).expect("valid node");
            assert_eq!(node, frame);
        }
    }
}
