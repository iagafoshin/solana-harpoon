use {
    crate::{
        node::{Kind, NodeError},
        util,
    },
    cid::Cid,
};

// type Block struct {
//   kind      Int
//   # The slot number where this block was created.
//   slot      Int
//   shredding [ Shredding ]
//   entries   [ Link ] # [ &Entry ]
//   # The metadata for this block.
//   meta      SlotMeta
//   # Link to the rewards for this block.
//   rewards   Link     # &Rewards
// } representation tuple
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Block {
    pub slot: u64,
    pub shredding: Vec<Shredding>,
    pub entries: Vec<Cid>,
    pub meta: SlotMeta,
    pub rewards: Cid,
}

impl TryFrom<&[u8]> for Block {
    type Error = NodeError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from(serde_cbor::from_slice::<serde_cbor::Value>(value)?)
    }
}

impl TryFrom<serde_cbor::Value> for Block {
    type Error = NodeError;

    fn try_from(value: serde_cbor::Value) -> Result<Self, Self::Error> {
        let mut node = Self::default();
        for (index, value) in util::cbor::get_array(value, "Block")?
            .into_iter()
            .enumerate()
        {
            match index {
                0 => NodeError::assert_invalid_kind(
                    util::cbor::get_int(value, "Block::kind")? as u64,
                    Kind::Block,
                )?,
                1 => node.slot = util::cbor::get_int(value, "Block::slot")? as u64,
                2 => {
                    for value in util::cbor::get_array(value, "Block::shredding")? {
                        node.shredding.push(Shredding::try_from(value)?);
                    }
                }
                3 => {
                    node.entries =
                        util::cbor::get_array_cids(value, "Block::entries", "Block::entries[]")?
                }
                4 => node.meta = SlotMeta::try_from(value)?,
                5 => node.rewards = util::cbor::get_cid(value, "Block::rewards")?,
                _ => return Err(NodeError::UnexpectedCborValues),
            }
        }
        Ok(node)
    }
}

// type Shredding struct {
//   entryEndIdx Int
//   shredEndIdx Int
// } representation tuple
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Shredding {
    pub entry_end_idx: i64,
    pub shred_end_idx: i64,
}

impl TryFrom<serde_cbor::Value> for Shredding {
    type Error = NodeError;

    fn try_from(value: serde_cbor::Value) -> Result<Self, Self::Error> {
        let mut node = Self::default();
        for (index, value) in util::cbor::get_array(value, "Shredding")?
            .into_iter()
            .enumerate()
        {
            match index {
                0 => {
                    node.entry_end_idx =
                        util::cbor::get_int(value, "Shredding::entry_end_idx")? as i64
                }
                1 => {
                    node.shred_end_idx =
                        util::cbor::get_int(value, "Shredding::shred_end_idx")? as i64
                }
                _ => return Err(NodeError::UnexpectedCborValues),
            }
        }
        Ok(node)
    }
}

// type SlotMeta struct {
//   # The parent slot of this slot.
//   parent_slot         Int
//   # Block time of this slot.
//   blocktime           Int
//   # Block height of this slot.
//   block_height nullable optional Int
// } representation tuple
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct SlotMeta {
    pub parent_slot: u64,
    pub blocktime: u64,
    pub block_height: Option<u64>,
}

impl TryFrom<serde_cbor::Value> for SlotMeta {
    type Error = NodeError;

    fn try_from(value: serde_cbor::Value) -> Result<Self, Self::Error> {
        let mut node = Self::default();
        for (index, value) in util::cbor::get_array(value, "SlotMeta")?
            .into_iter()
            .enumerate()
        {
            match index {
                0 => node.parent_slot = util::cbor::get_int(value, "SlotMeta::parent_slot")? as u64,
                1 => node.blocktime = util::cbor::get_int(value, "SlotMeta::blocktime")? as u64,
                2 => {
                    node.block_height =
                        util::cbor::get_int_opt(value, "SlotMeta::block_height")?.map(|v| v as u64)
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
        node::{Block, Shredding, SlotMeta},
        util::tests::{decode_cid, decode_cids, decode_hex},
    };

    #[test]
    fn test_decoding() {
        for (bytes, frame) in [
            (
                decode_hex(
                    "8602099843820000820101820202820303820404820505820606820707820808820909820a0a820b0b820c0c820d0d820e0e820f0f8210108211118212128213138214148215158216168217178218181818821819181982181a181a82181b181b82181c181c82181d181d82181e181e82181f181f821820182082182118218218221822821823182382182418248218251825821826182682182718278218281828821829182982182a182a82182b182b82182c182c82182d182d82182e182e82182f182f821830183082183118318218321832821833183382183418348218351835821836183682183718378218381838821839183982183a183a82183b183b82183c183c82183d183d82183e183e82183f183f8218401840821841184182184218429843d82a582500017112208e70dfda48a74bd6779fdddd5f553f12d71844633b5544e0093eeef10740c06dd82a58250001711220de2c3acd743fdf886712f449e53a9c894a7acea19e47aab2d6d1e38c2a28ac6ed82a58250001711220bf5af8161b6daab4dd9ad2fad8410ecca5b44961530859372134a68ba2e60d1ed82a58250001711220e6ed8ba06733a305ed842fa4e80d3ccaa8f49a4be204c96a217e4a45f33e3102d82a5825000171122026f3f94322ea0e62f5910fa0164968d551b863a18352aa257bd1f387913341b2d82a58250001711220299dbd4bcfd750a8f557b2ab2d1f0f08d70f787db0f11e9d9ac8c50a089e81cfd82a58250001711220bbd5b7a3c0c1d38e6d2e3829b5910d67d4ba1e1395d87888d0d17db254fc50bed82a582500017112207e91d467807523784982ba1e0c570c7042e88d53ad775625282c88a3d9ee4901d82a58250001711220fd7bb944f3dc4df8c089a13096e7f1330f5b4b05d6274501be1f89428a109d09d82a58250001711220de2acc66085a95b24e038874621dc1c31e6aed53c90d7d0796109704d70f813cd82a58250001711220811372bbbd25304f405832411401b88468172d2fc7550ca9550b3de3a608ac36d82a58250001711220645933322cc05875a48dbf40b9ae03e23c92687d6b972a082eb626d4076ea9dcd82a58250001711220a7bae91d78c3912a20a403f9f26bfc2471b70c750ba661016e4b52022c7f67bad82a582500017112203917ac61e69b00a85a3be837383ffd2e42848f8781b1c3cb7b664712c9d157aad82a582500017112203431ca562c86af13a140578ad5ecec314544cd50de2a30e5f38750155e77b8efd82a5825000171122005f71efedffa30ea85cdeea2d27b3b7d14d55396b50243d7fc8940166ef1471ed82a5825000171122095fd84424b516aaf24548c7332a8ca40d126a89e9803bbe03c2ab7718fc53e43d82a58250001711220b4e42a748f9c563594900ee980c0362d2b3014b2172c582394cb5871265eb94bd82a58250001711220964c8a47d80ae167f7938c476632145f48d7d9488df10bff4714b417e6fc9dd3d82a58250001711220e061951d6f2b1b088060c85fdbdf463778032a165e3aa045c3b948cd1298cdb0d82a58250001711220397044077b436167e3a0d651ceff7b9cbbb12165bf863fc82928d176cc379043d82a58250001711220ee45cf07e975cf04e1f8d7a2fe2d08831314989b9f3e8ed0a4f6dd191c6161d1d82a582500017112206f40421fd683e835427001733ac9296cd5fcfdef8043ee0e15732ac8666e0723d82a58250001711220bcae3697159ee2c71b6d0320e80d588324f35b5fdc7bba7a406f8eeb8be4044ed82a58250001711220bb98457d1d4ae8f761afa4b5fd42d0d85a283dc987091ba5458edae2337b9e61d82a58250001711220ea62c75fdb2e82644bb4e49182ed4ebfd10903c17c4e77f000bd8f5cdf77ec00d82a58250001711220c87547fe183e92ea2d6d229b27d78bea4eb789b2207d32a1aa77d9c9dcc3d260d82a5825000171122049eb356e830866027fe689348955a908bcb9c189a1b78676c6e8d54564486ef1d82a5825000171122046cbf5dab11b80f18a54afb823120493543ec11f95ff0bb5c06e82e9999130dfd82a582500017112203aeb6bc30a676aff9cc408dcdb8d9baaa04413a713e1db3b159ccb00d22cc980d82a582500017112202afe09043254bdcdebdfae754bfebddfe5c1b5ca547383625cb9150c805bf72bd82a58250001711220ae2f824067a97eba186ef32ba996576d4a38a3d10e59f4ae66a45d500ba27f0dd82a58250001711220046e04c9664bade9bdf5311b2961157a5f7bc1bef934b1b6ce49ca234fd083ccd82a58250001711220fb9677b336d7128d2d37fd2e283c06a89c285161e0176b11b071d9c60c6edae5d82a582500017112201b38181f40a765912b63c6b069aa94b30d7470a1817d80c8c034bf0d7e647094d82a58250001711220110a0ad786c4d86638cd455282a3fb80a9ad03d32103eeb57aee78716146c763d82a58250001711220ed7bccf622e7105bd30fd061b40b34bdf529f032020c7d7835bae95b36112695d82a58250001711220478ea5ac630a1a91fcb395ca2500ac567c6fe75475e3f60719a15ed1913b385cd82a58250001711220a6d74d76bf801d67ccab59ac9abe0b3dec41aeaf9ab762f818691c0e66349a30d82a582500017112203d71aaf08d067bc4b3c755caf4a4f049d6ef78dcbeaaf6ed15b5a5f33491321fd82a58250001711220d20613fc1ee4f0d8971eee6ee88ae2a10d4c10705ce14d7093db509db3a6202ed82a5825000171122034bb4e0e339bd4ef1c0a6dfff8972ac27e17dbc14a53bbad4f414df6a60873aed82a58250001711220b29ccec5eba15bff87bf0337b6b5c3232394ad5a02d619c77472a646c3170ca6d82a582500017112207b8bb6ba230c230280aa3129cec041289b101ae6d72b2faceb4ac6f814a3843cd82a58250001711220a7b0e042ac9777a50b6072c92dceb012a47d6ca6199d5b8fc2c4a39484118459d82a58250001711220cb7f80d2656d39520772075b25fe1dc593f60af0c81f69024291d88fe17e8a8dd82a58250001711220120d07cd088009e73dbb76df9a4bbdcd9c2c94fe66e3cdb89f7a1d8e0cdf08ced82a5825000171122005aa838c42341f99118bfa94b2914fd5f9db789af051149cce5f6e2a8cb4ed7dd82a58250001711220d69dde9e11eb7fe6e83ce06bfa768a4cad4e3b351b3cc9dd180f09b0d557aafed82a582500017112209c89883da4a197850241b51cfae9554767e188f5ca51cd2ab992df0dcf249959d82a5825000171122002785313da28b86cf4baeae1150f0118b3eec685bb19ead5ca24e9172a9e5ff4d82a58250001711220fa12a59f25170e1b69ae4c3e1d92bc57be3d8b8dadffe917b8747aaee1a2bcdcd82a582500017112204cae6cea89f375feea52fb652ce9581ff9680dd5a5e75718c0b08bf70182ccfdd82a582500017112206f63cca06b4d90228194e14b01e9219e7f103595cf71999daeb4a0122516e50cd82a5825000171122073b2ee9306aa400e5b258c6ed3bb9dbda2bfc6c6c17c479a247a24f4bd785d94d82a5825000171122021832f96d0b3da1f284d8d4686533b066f242bfc61da4d951b277a5bbe943c12d82a58250001711220e35316af0d272f5307f82d3ca58507a9236e83ef85d55ec3cba1ada0a2124a86d82a5825000171122078fe3651382e8bf0bab87226242617ac45dfeeaf3ba2e1f240c6af8384dc9c76d82a58250001711220178a764b45cc34b1447daa3af63f48b8c875b13328a324d6476640aca2751737d82a5825000171122082a8a1b683c8630fc57de9d9c8cbf627c973ade5807fe08f4931ef9147361809d82a582500017112206cca1813027342a1e4e08defb8aa37af8db85dacc2dff20bba359c59827d7f22d82a58250001711220b07aaf0f70ca180adcecbc3bb9019baea87951b76aecfc1e7cc20418857309efd82a582500017112202cf2361e14eb616e2d67ed1f6fdc3302071dfb0ce7892c0335a7bed4f8152014d82a582500017112209b139015281ea175d134cf169d598c971a0006ad0235db99922a2dde4b00fe74d82a58250001711220a79f901cddc805664f37e905b0a7cd9846322052e8e02450eacbcdd5a96c71a7d82a58250001711220e82cff17aa6beb56a05bd758bd6f39f1953a4a688f415c28f716d66228ace960d82a582500017112203885a73680523567cbb778e13ed8c4106806ec745d85aae42c42407898f0bd4a830800f6d82a450001550000",
                ),
                Block {
                    slot: 9,
                    shredding: (0..=66)
                        .map(|value| Shredding {
                            entry_end_idx: value,
                            shred_end_idx: value,
                        })
                        .collect(),
                    entries: decode_cids([
                        "bafyreieoodp5usfhjplhph653vpvkpys24meiyz3kvcoacj653yqoqganu",
                        "bafyreig6fq5m25b736egoexujhstvhejjj5m5im6i6vlfvwr4ogcukfmny",
                        "bafyreif7ll4bmg3nvk2n3gws7lmecdwmuw2esyktbbmtoijuu2f2fzqndy",
                        "bafyreihg5wf2azztumc63bbputua2pgkvd2jus7catewuil6jjc7gprrai",
                        "bafyreibg6p4ugixkbzrpleipuales2gvkg4ghimdkkvck66r6odzcm2bwi",
                        "bafyreibjtw6uxt6xkcupkv5svmwr6dyi24hxq7nq6epj3gwiyufarhubz4",
                        "bafyreif32w32hqgb2ohg2lryfg2zcdlh2s5b4e4v3b4irugrpwzfj7cqxy",
                        "bafyreid6shkgpadven4etav2dygfoddqilui2u5no5lckkbmrcr5t3sjae",
                        "bafyreih5po4uj464jx4mbcnbgclop4jtb5nuwbowe5cqdpq7rfbiuee5be",
                        "bafyreig6flggmcc2swze4a4iorrb3qoddzvo2u6jbv6qpfqqs4cnod4bhq",
                        "bafyreiebcnzlxpjfgbhuawbsiekadoeenals2l6hkugksvilhxr2mcfmgy",
                        "bafyreideleztelgalb22jdn7ic424a7chsjgq7lls4vaqlvwe3kao3vj3q",
                        "bafyreifhxlur26gdsevcbjad7hzgx7beog3qy5iluzqqc3slkibcy73hxi",
                        "bafyreibzc6wgdzu3acufuo7ig44d77joikci7b4bwhb4w63gi4jmtukxvi",
                        "bafyreibughffmlegv4j2cqcxrlk6z3brivcm2ug6fiyol44hkakv455y54",
                        "bafyreiaf64pp5x72gdviltpouljhwo35ctkvhfvvajb5p7ejialg54khdy",
                        "bafyreiev7wcees2rnkxsivemomzkrssa2etkrhuyao56apbkw5yy7rj6im",
                        "bafyreifu4qvhjd44ky2zjeao5gamanrnfmybjmqxfrmchfgllbysmxvzjm",
                        "bafyreiewjsfepwak4ft7pe4mi5tdefc7jdl5ssen6ef76ryuwql6n7e52m",
                        "bafyreihamgkr23zldmeiaygil7n56rrxpabsufs6hkqelq5zjdgrfggnwa",
                        "bafyreibzobcao62dmft6higwkhhp6644xoysczn7qy74qkji2f3myn4qim",
                        "bafyreihoixhqp2lvz4cod6gxul7c2cedcmkjrg47h2hnbjhw3umryylb2e",
                        "bafyreidpibbb7vud5a2ue4abom5msklm2x6p334aipxa4fltflegm3qhem",
                        "bafyreif4vy3jofm64ldrw3idedua2wedetzvwx64po5huqdpr3vyxzaejy",
                        "bafyreif3tbcx2hkk5d3wdl5ewx6ufugyliud3smhben2krmo3lrdg646me",
                        "bafyreihkmldv7wzoqjsexnhesgbo2tv72eeqhql4jz37aaf5r5on657maa",
                        "bafyreigiovd74gb6slvc23jctmt5pc7kj23ytmrapuzkdktx3he5zq6sma",
                        "bafyreicj5m2w5ayimybh7zujgsevlkiixs44dcnbw6dhnrxi2vcwisdo6e",
                        "bafyreicgzp25vmi3qdyyuvfpxarrebetkq7mch4v74f3lqdoqluztejq34",
                        "bafyreib25nv4gcthnl7zzrai3tny3g5kubcbhjyt4hntwfm4zmanelgjqa",
                        "bafyreibk7yeqimsuxxg6xx5oovf75po74xa3lssuoobwexfzcugiaw7xfm",
                        "bafyreifof6beaz5jp25bq3xtfouzmv3nji4khuiolh2k4zvelviaxit7bu",
                        "bafyreiaenycmszslvxu335jrdmuwcfl2l554dpxzgsy3ntsjziru7uedzq",
                        "bafyreih3sz33gnwxckgs2n75fyudybvitqufcypac5vrdmdr3hday3w24u",
                        "bafyreia3hamb6qfhmwiswy6gwbu2vfftbv2hbimbpwamrqbux4gx4zdqsq",
                        "bafyreiarbifnpbwe3btdrtkfkkbkh64avgwqhuzbapxlk6xopbywcrwhmm",
                        "bafyreihnppgpmixhcbn5gd6qmg2awnf56uu7amqcbr6xqnn25fntmejgsu",
                        "bafyreichr2s2yyykdki7zm4vzisqblcwprx6ovdv4p3aognbl3izcozylq",
                        "bafyreifg25gxnp4advt4zk2zvsnl4cz55ra25l42w5rpqgdjdqhgmne2ga",
                        "bafyreib5ogvpbdigppclhr2vzl2kj4cj23xxrxf6vl3o2fnvuxztjejsd4",
                        "bafyreigsayj7yhxe6dmjohxon3uivyvbbvgba4c44fgxbe63kco3hjrafy",
                        "bafyreibuxnha4m432txryctn774jokwcpyl5xqkkko522t2bjx3kmcdtvy",
                        "bafyreifstthml25blp7yppydg63llqzdeokk2wqc2ym4o5dsuzdmgfymuy",
                        "bafyreid3ro3luiymembibkrrfhhmaqjitmibvzwxfmx2z22ky34bji4ehq",
                        "bafyreifhwdqeflexo6sqwydszew45masur6wzjqztvny7qweuokiiemele",
                        "bafyreiglp6anezlnhfjao4qhlms74hofsp3av4gid5uqequr3ch6c7ukru",
                        "bafyreiasbud42ceabhtt3o3w36nexpontqwjj7tg4pg3rh32dwhazxyizy",
                        "bafyreiafvkbyyqrud6mrdc72sszjct6v7hnxrgxqkekjzts7nyviznhnpu",
                        "bafyreigwtxpj4eplp7toqphanp5hncsmvvhdwni3hte52gapbgynkv5k7y",
                        "bafyreie4rged3jfbs6cqeqnvdt5osvkhm7qyr5okkhgsvoms34g46jezle",
                        "bafyreiacpbjrhwrixbwpjoxk4ekq6aiywpxmnbn3dhvnlsre5elsvhs76q",
                        "bafyreih2cksz6jixbynwtlsmhyozfpcxxy6yxdnn77urpodupkxodiv43q",
                        "bafyreicmvzwovcptox7ouux3muwoswa77fua3vnf45lrrqfqrp3qdawm7u",
                        "bafyreidpmpgka22nsaridfhbjma6sim6p4idlfopogmz3lvuuajckfxfbq",
                        "bafyreidtwlxjgbvkiahfwjmmn3j3xhn5uk74nrwbprdzujd2et2l26c5sq",
                        "bafyreibbqmxznuft3ipsqtmni2dfgoygn4scx7db3jgzkgzhpjn35fb4ci",
                        "bafyreihdkmlk6djhf5jqp6bnhssykb5jenxih34f2vpmhs5bvwqkeeskqy",
                        "bafyreidy7y3fcoborpylvodseyscmf5mixp65lz3ulq7eqggv6byjxe4oy",
                        "bafyreiaxrj3ewromgsyui7nkhl3d6sfyzb23cmziumsnmr3gicwke5ixg4",
                        "bafyreiecvcq3na6immh4k7pj3hemx5rhzfz23zmap7qi6sjr56iuonqybe",
                        "bafyreidmzimbgattikq6jyen564kun5prw4f3lgc37zaxorvtrmye7l7ei",
                        "bafyreifqpkxq64gkdafnz3f4ho4qdg5ovb4vdn3k5t6b47gcaqmik4yj54",
                        "bafyreibm6i3b4fhlmfxc2z7nd5x5ymyca4o7wdhhrewagnnhx3kpqfjacq",
                        "bafyreie3coibkka6uf25cngpc2ovtdexdiaanlicgxnzterkfxpewah6oq",
                        "bafyreifht6ibzxoiavte6n7jawykptmyiyzcauxi4asfb2wlzxk2s3dru4",
                        "bafyreihift7rpktl5nlkaw6xlc6w6oprsu5eu2epifocr5yw2zrcrlhjma",
                        "bafyreibyqwttnacsgvt4xn3y4e7nrraqnadoy5c5qwvoilccib4jr4f5ji",
                    ]),
                    meta: SlotMeta {
                        parent_slot: 8,
                        blocktime: 0,
                        block_height: None,
                    },
                    rewards: decode_cid("bafkqaaa"),
                },
            ),
            (
                decode_hex(
                    "86021a010114929855820000820101820202820303820404820505820606820707820808820909820a0a820b0b820c0c820d0d820e0e820f2082102082112082122082130f821410821511821612821713821818148218191582181a1682181b1782181c181882181d181982181e181a82181f181b821820181c821821181d821822181e82182320821824181f821825208218262082182720821828208218292082182a2082182b2082182c182282182d2082182e2082182f208218302082183120821832208218332082183418258218352082183620821837208218381827821839182882183a182982183b182a82183c182b82183d182c82183e182d82183f20821840182e821841182f8218421830821843183182184418328218451833821846183482184718358218481836821849183782184a183882184b183982184c183a82184d183b82184e183c82184f183d821850183e821851183f8218521840821853184182185418429855d82a582500017112207a7b4a72399d2581f6032536c74240df2ec00c4c91e25ff2fe22e0ab9c1202e9d82a58250001711220dfc78765b219e268a8d167e58d43d8d0ff632f6d4ddffc64b9baef50dc5f924bd82a5825000171122096fb0da6bf2aa63cc7b559001fdcb0d97b1fa139476858771b4e62dbc3dc3582d82a582500017112205c784aed6488cdcfc1d335045efe9605a78ea43c648653430b6f4c2417f8e0a6d82a582500017112200f3d58aa03ce3fbb396590aa231ce6000106dcc0d34fe7ad74fc0439a4e975dcd82a5825000171122048833c2cd1e47195859148e6a18f3d1979d9f80c43087acfba43de1babd39e73d82a58250001711220064698c9e6d371a9f028a3bec6e0894c65071b5a63a8395fcdff1e8d60cb2c9cd82a582500017112203325a4e9a344b9753d277a9be2be830e5c48916d06f1041ea15893365c6a2b5dd82a582500017112203b62f24751ee8cc2f0ad420896f350e4bffefd4bfde2dc6bfa6cd201f1a4cba1d82a582500017112200d6f2964f96268ed3e839bb3dacbb259f213580e30bf7e34ad24ee077d9439ead82a5825000171122062ce888682b7213eedf10ada8bef06645f6864a18aa4c00d465948a2ed0e13dad82a58250001711220d5b68d21f58d27d7bb2432111a551e92c153b5364972fefb16853f54cad454d0d82a582500017112201a806791c9275b55e036f391cef44e688faeebddbad32bf2ee3d69efd8f7c18ed82a58250001711220520f53e2b2e73c707348e576f3b5e982323c50f88079fe0696a338150c5943b3d82a582500017112207852147e3c150a6ebbc255e968f1ba21fb630af2e4260e702459fb59b0fe3c5fd82a582500017112203285e24bfb1decfad0d826959ead4f77419df9b65c4d6dd304af1416256cfc56d82a582500017112203a22e47562bde2d2def7ba92dea66c55ec7ad8d4c367549b7d2301d9acf1dde1d82a5825000171122065cf68062e4ed07628fa2c506b38d5aea924ce9500fd20d79ce930074ad99c69d82a58250001711220009edb27484508ea58aa0db211a17a4658b1e9158e75cf4b20e954739c896e29d82a58250001711220593727214e9d3fa791442ca53241a036bc5a76b6b4d4fd9822bbd00c3ea0eca5d82a582500017112201daa75d4a8593e8ef11394a56d5660b4aa4823458fababdbc115c15c8b5f8b2bd82a58250001711220e39925b92694aef62708d331198fb7ddcfa9084c34d4e89cbc4584ed2f4164fdd82a582500017112209017f503ec55718f55ba0b3225822db388f8e97b92527f205c24ef5ff2dc2bc3d82a58250001711220606a6924ea01fc5c608dadf73ebac33f009695632f483fe3d453692ffa9558aed82a58250001711220abac5da7e0cfb427d736a6fa7810f4841c093b07d8405e9b6b3a6fa8f9fc37e6d82a582500017112201f8e614991dbcbb1cbb303364e633b9747ad410a50793820f9269c79aea26d74d82a5825000171122033f7bb5a610b4745f6a01b1aec9a3e538b1c7bc2e9a88cb767f4a029cbb66990d82a58250001711220a76a4894a0f50b527548f8593d195501e43f29792896feeca088acdcbc5bd077d82a582500017112208612558ab14372a7f625fea2892fb23d0e33b95c22c755557159876c649d94b5d82a58250001711220c9a308a95e95e53729b2bba225aca4469fc7692e66276b278c7dd49134f97be9d82a58250001711220adedf500c72112a4b88e798a7a3972bad941f307843c3e9398b012a0aad7050fd82a5825000171122095466e67c9ef286410304f720a2a1c2935dbb57439182752bc4eb0de46aed72fd82a582500017112207f632358d54807f531f0b858d7ca53691ff95682b100872dbc45ace23fdf42f7d82a582500017112202658c8332d1a96e5331850710a75cc83751d5398ec2a51b62d7fa0ff049d18b4d82a582500017112203596d9500d74fee97d37f59ead6459584b28f3c473f736d05dbde75440befeccd82a5825000171122087cffb8f7adebf14698528aacc20b70b1c85467efb4ec37edfc0065c930eacbed82a58250001711220a247eb104de6090cb4d8745c438cea14bd7bc98b5f8c9286917dbb5eb2983432d82a58250001711220bbfb02bc473a5a2e0ba567f067266494651ba7aa387f069e7268bc9dfdd853bad82a5825000171122053027b8c164a63362e52a9a363fa05c34da80392103b131b26558a09c37a52ced82a58250001711220d658192a443bf99c99d427c0b677d6e49722b864f295dc1efdbb9dc9bf5e91b7d82a58250001711220e22b957d79580e45c5e1515f8c770e7dfc1240cf3172b1be75a811a03294395dd82a582500017112203c200682e37c61b47fb4c9d9db7a587d8bf3e334a37115a5d4b72986d7d88152d82a5825000171122056938cf4cd99f55dd387e835af70b88a107d285ca501a11c43b02bf2a378da6dd82a582500017112209c4f08746334c246526eeb3b6d20f3118ff5371c37b04dccc71177a1fdaed5bfd82a58250001711220224879a7d335763a4fde0df61a2c2be20eb223fbefd14ab432b1c896f0f8180fd82a58250001711220da9b65588c3eaf9679913f2071f89a57491e35ec1f93cf63b88901249db99d49d82a58250001711220f76b2832efbb9a8d6484c3d8e41ab8ac5151dc799993bf1f0e4e4b12f648b1bed82a582500017112209b5c1336e06efd4f403b282a98120d4ac71a34d5d7cb1ba1163197f9065756d2d82a5825000171122003d07d0046737bf34641e0ea68acfc7d4e0694bb264da83be6ea7b9d57ccf09ed82a5825000171122034102231c6172d872c4f1f0c9e3e81de2a960c1603ab6e11476011159b2eef01d82a58250001711220ced57640f88031b9241797e6025e8c89936c2086e9df6cb7080f40c94782776fd82a582500017112200ff81a4834295bc516a8cf94924fa172dcfafbad675682b3df2afdbb94837ee5d82a58250001711220ed2c6e34a6a7a19312b256a4812e0263717536ee6e421a820c91464a8a42aaa9d82a5825000171122008a8532010d648258902e692c38d895cfc2057736a91060e622afd1d77d356f8d82a58250001711220863dd017ae59be608ddc1fdf41bcfda8937fefe22dd11da19b67ba08191ec1b0d82a582500017112206154c473c5989a98fe0d800c6500e71184e6a5e4f571c02ef637f3e0c7412dacd82a58250001711220000b8ce10bc7f0028e3a720ad44d717faf550ba081fab564ca879ff895db06c2d82a58250001711220acd88daa2a9763c01099afb7b5935239651dee3d70babc368466962ea0704f18d82a582500017112206c641e32b79e14415b11fa7f2f495740d3fcd01dfafe67205dc5b6df716c87cdd82a5825000171122013a004c9a785c55cc61058460681ad49e4c459ddee280878e48aa205a174dc28d82a5825000171122066a81e01b2c6d2eb5c07401007eea100aeaafae621bd61804805d95a32e616b9d82a582500017112201970a6f9118c3add2af2cfb52a2585cb47f2f6a8cc04b200e4faaceb86f86109d82a582500017112200dcbbfe226aec43f781a0ac53fd0c5eb3b1ffea5731b6940e0968a039115610ad82a58250001711220eaf55b480e3d0ac7a807f7ae4a8e49213e2484628fc8a7b53ff4786926229518d82a5825000171122008bce1875fb861d5c3295731e31605f8f94a86458b69819b215377923961ecf5d82a58250001711220183db1ac3746fc4c2f75ed6ee1055381952713360cab4c4471d61521d128afefd82a582500017112206f16473c4821a0a95d2a9e481f0889aa63e17baeaf55c84151afdc3db3b41fe3d82a582500017112206efa3edea547496fd3446000d8c255efd20b7bdfd8e02b54b44eb8f8a256523ed82a5825000171122075afd825378271e8280c1e12a2cf7bf4a0b97b43a95e6e931c145bf7b91a57cdd82a5825000171122065352017c06d6ca6f532ee8251fb1ab9b8cbca5d06bd24f19c460f6b080e561bd82a5825000171122079a91046a62c2f0e66170ff5b31203229f52d96fa821e4548045610f286f4307d82a582500017112203f78a01994b4ceebd93067fd28ed3933e4c4ab5cba310db3fc5d62f9311d1ee7d82a58250001711220c2e408c06e5ae6740bc517882ec9d945cae3de4a12a5a0dca12f4df72503441bd82a582500017112209df42a2c1aa1c2787f1bd3de2ae04743f38ba9f57295c55749f1d6d9323b9311d82a58250001711220f5d833b5e48a7806433005119be49de2b701631bf87aa24a211e4ce5a1975ac7d82a58250001711220086e845646a1166e17f6bc601d0ebe28ca6b97d7bf6fd82a6c1bf4dadcc50dabd82a582500017112209509c996b4bb8f87c682562e2c3353ba47a06231c5654ba9b11b579c73de9704d82a58250001711220f5073258230957e87f6adb603de69883a4fbbe36690afb8ec39073d7e16c6dd8d82a5825000171122043853fbad49824fb5ce8ac6ee6d0ee1b90ffd8f933756ff93cdd93c380865f59d82a58250001711220375c64953680bb4c01ada6aa4bdc98167a9ef40780549eedbb3ab864ced8d6b2d82a58250001711220f3452529d963be8ee22be5cc3ee5d8663dd40c32ad1811dd3633c5607f841eb6d82a582500017112204dac68cd51c4d0e3b41a3c2dccfccd707981a09e32c25e3f7ccfd7f726067a28d82a582500017112207994c891d59c1e279484287d5bc724d8adbba03839c569da582c2634646a70a0d82a58250001711220a52bba176136c5f8f8fb88a33467fcb8adc15707854c78dd3f21e7b5bd4c5861d82a58250001711220cc7d109ebf107a00b35b1fa31ae603519a732032074faf966871bc80f063314f831a0101149100f6d82a450001550000",
                ),
                Block {
                    slot: 16848018,
                    shredding: vec![
                        Shredding {
                            entry_end_idx: 0,
                            shred_end_idx: 0,
                        },
                        Shredding {
                            entry_end_idx: 1,
                            shred_end_idx: 1,
                        },
                        Shredding {
                            entry_end_idx: 2,
                            shred_end_idx: 2,
                        },
                        Shredding {
                            entry_end_idx: 3,
                            shred_end_idx: 3,
                        },
                        Shredding {
                            entry_end_idx: 4,
                            shred_end_idx: 4,
                        },
                        Shredding {
                            entry_end_idx: 5,
                            shred_end_idx: 5,
                        },
                        Shredding {
                            entry_end_idx: 6,
                            shred_end_idx: 6,
                        },
                        Shredding {
                            entry_end_idx: 7,
                            shred_end_idx: 7,
                        },
                        Shredding {
                            entry_end_idx: 8,
                            shred_end_idx: 8,
                        },
                        Shredding {
                            entry_end_idx: 9,
                            shred_end_idx: 9,
                        },
                        Shredding {
                            entry_end_idx: 10,
                            shred_end_idx: 10,
                        },
                        Shredding {
                            entry_end_idx: 11,
                            shred_end_idx: 11,
                        },
                        Shredding {
                            entry_end_idx: 12,
                            shred_end_idx: 12,
                        },
                        Shredding {
                            entry_end_idx: 13,
                            shred_end_idx: 13,
                        },
                        Shredding {
                            entry_end_idx: 14,
                            shred_end_idx: 14,
                        },
                        Shredding {
                            entry_end_idx: 15,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 16,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 17,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 18,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 19,
                            shred_end_idx: 15,
                        },
                        Shredding {
                            entry_end_idx: 20,
                            shred_end_idx: 16,
                        },
                        Shredding {
                            entry_end_idx: 21,
                            shred_end_idx: 17,
                        },
                        Shredding {
                            entry_end_idx: 22,
                            shred_end_idx: 18,
                        },
                        Shredding {
                            entry_end_idx: 23,
                            shred_end_idx: 19,
                        },
                        Shredding {
                            entry_end_idx: 24,
                            shred_end_idx: 20,
                        },
                        Shredding {
                            entry_end_idx: 25,
                            shred_end_idx: 21,
                        },
                        Shredding {
                            entry_end_idx: 26,
                            shred_end_idx: 22,
                        },
                        Shredding {
                            entry_end_idx: 27,
                            shred_end_idx: 23,
                        },
                        Shredding {
                            entry_end_idx: 28,
                            shred_end_idx: 24,
                        },
                        Shredding {
                            entry_end_idx: 29,
                            shred_end_idx: 25,
                        },
                        Shredding {
                            entry_end_idx: 30,
                            shred_end_idx: 26,
                        },
                        Shredding {
                            entry_end_idx: 31,
                            shred_end_idx: 27,
                        },
                        Shredding {
                            entry_end_idx: 32,
                            shred_end_idx: 28,
                        },
                        Shredding {
                            entry_end_idx: 33,
                            shred_end_idx: 29,
                        },
                        Shredding {
                            entry_end_idx: 34,
                            shred_end_idx: 30,
                        },
                        Shredding {
                            entry_end_idx: 35,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 36,
                            shred_end_idx: 31,
                        },
                        Shredding {
                            entry_end_idx: 37,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 38,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 39,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 40,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 41,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 42,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 43,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 44,
                            shred_end_idx: 34,
                        },
                        Shredding {
                            entry_end_idx: 45,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 46,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 47,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 48,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 49,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 50,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 51,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 52,
                            shred_end_idx: 37,
                        },
                        Shredding {
                            entry_end_idx: 53,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 54,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 55,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 56,
                            shred_end_idx: 39,
                        },
                        Shredding {
                            entry_end_idx: 57,
                            shred_end_idx: 40,
                        },
                        Shredding {
                            entry_end_idx: 58,
                            shred_end_idx: 41,
                        },
                        Shredding {
                            entry_end_idx: 59,
                            shred_end_idx: 42,
                        },
                        Shredding {
                            entry_end_idx: 60,
                            shred_end_idx: 43,
                        },
                        Shredding {
                            entry_end_idx: 61,
                            shred_end_idx: 44,
                        },
                        Shredding {
                            entry_end_idx: 62,
                            shred_end_idx: 45,
                        },
                        Shredding {
                            entry_end_idx: 63,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 64,
                            shred_end_idx: 46,
                        },
                        Shredding {
                            entry_end_idx: 65,
                            shred_end_idx: 47,
                        },
                        Shredding {
                            entry_end_idx: 66,
                            shred_end_idx: 48,
                        },
                        Shredding {
                            entry_end_idx: 67,
                            shred_end_idx: 49,
                        },
                        Shredding {
                            entry_end_idx: 68,
                            shred_end_idx: 50,
                        },
                        Shredding {
                            entry_end_idx: 69,
                            shred_end_idx: 51,
                        },
                        Shredding {
                            entry_end_idx: 70,
                            shred_end_idx: 52,
                        },
                        Shredding {
                            entry_end_idx: 71,
                            shred_end_idx: 53,
                        },
                        Shredding {
                            entry_end_idx: 72,
                            shred_end_idx: 54,
                        },
                        Shredding {
                            entry_end_idx: 73,
                            shred_end_idx: 55,
                        },
                        Shredding {
                            entry_end_idx: 74,
                            shred_end_idx: 56,
                        },
                        Shredding {
                            entry_end_idx: 75,
                            shred_end_idx: 57,
                        },
                        Shredding {
                            entry_end_idx: 76,
                            shred_end_idx: 58,
                        },
                        Shredding {
                            entry_end_idx: 77,
                            shred_end_idx: 59,
                        },
                        Shredding {
                            entry_end_idx: 78,
                            shred_end_idx: 60,
                        },
                        Shredding {
                            entry_end_idx: 79,
                            shred_end_idx: 61,
                        },
                        Shredding {
                            entry_end_idx: 80,
                            shred_end_idx: 62,
                        },
                        Shredding {
                            entry_end_idx: 81,
                            shred_end_idx: 63,
                        },
                        Shredding {
                            entry_end_idx: 82,
                            shred_end_idx: 64,
                        },
                        Shredding {
                            entry_end_idx: 83,
                            shred_end_idx: 65,
                        },
                        Shredding {
                            entry_end_idx: 84,
                            shred_end_idx: 66,
                        },
                    ],
                    entries: decode_cids([
                        "bafyreid2pnfheom5ewa7mazfg3dueqg7f3aayter4jp7f7rc4cvzyeqc5e",
                        "bafyreig7y6dwlmqz4jukrulh4wguhwgq75rs63kn376gjon255inyx4sjm",
                        "bafyreiew7mg2npzkuy6mpnkzaap5zmgzpmp2cokhnbmhog2omln4hxbvqi",
                        "bafyreic4pbfo2zeizxh4duzvarpp5fqfu6hkipdeqzjugc3pjqsbp6hauy",
                        "bafyreiaphvmkua6oh65tszmqvirrzzqaaednzqgtj7t225h4aq42j2lv3q",
                        "bafyreiciqm6czupeogkyleki42qy6pizphm7qdcdbb5m7osd3yn2xu46om",
                        "bafyreiagi2mmtzwtogu7akfdx3dobckmmudrwwtdva4v7tp7d2gwbszmtq",
                        "bafyreibtewsoti2exf2t2j32tprl5ayolrejc3ig6ecb5ikysm3fy2rllu",
                        "bafyreib3mlzeouportbpblkcbclpguhex77p2s754logx6tm2ia7djglue",
                        "bafyreiann4uwj6lcndwt5a43wpnmxmsz6ijvqdrqx57djlje5ydx3fbz5i",
                        "bafyreidcz2einavxee7o34ik3kf66btel5ugjimkutaa2rszjcro2dqt3i",
                        "bafyreigvw2gsd5mne7l3wjbscenfkhusyfj3knsjol7pwfufh5kmvvcu2a",
                        "bafyreia2qbtzdsjhlnk6anxtshhpittir6xoxxn22mv7f3r5nhx5r56bry",
                        "bafyreicsb5j6fmxhhryhgshfo3z3l2mcgi6fb6eaph7anfvdhakqywkdwm",
                        "bafyreidykikh4pavbjxlxqsv5fupdorb7nrqv4xeeyhhajcz7nm3b7r4l4",
                        "bafyreibsqxrex6y55t5nbwbgswpk2t3xigo7tns4jvw5gbfpcqlck3h4ky",
                        "bafyreib2elshkyv54ljn5552slpkm3cv5r5nrvgdm5kjw7jdahm2z4o54e",
                        "bafyreidfz5uamlso2b3cr6rmkbvtrvnovesm5fia7uqnphhjgaduvwm4ne",
                        "bafyreiaat3nsoscfbdvfrkqnwii2c6sglcy6sfmooxhuwihjkrzzzclofe",
                        "bafyreiczg4tsctu5h6tzcrbmuuzedibwxrnhnnvu2t6zqiv32agd5ihmuu",
                        "bafyreia5vj25jkczh2hpce4uuvwvmyfuvjecgrmpvov5xqivyfoiwx4lfm",
                        "bafyreihdtes3sjuuv33cocgtgemy7n65z6uqqtbu2tujzpcfqtws6qle7u",
                        "bafyreieqc72qh3cvoghvloqlgisyelntrd4os64skj7saxbe55p7fxblym",
                        "bafyreidanjusj2qb7rogbdnn647lvqz7acljkyzpja76hvctnex7vfkyvy",
                        "bafyreiflvro2pygpwqt5onvg7j4bb5eedqetwb6yibpjw2z2n6upt7bx4y",
                        "bafyreia7rzquteo3zoy4xmydgzhggo4xi6wuccsqpe4cb6jgtr425itnoq",
                        "bafyreibt665vuyili5c7nia3dlwjupstrmohxqxjvcgloz7uuau4xntjsa",
                        "bafyreifhnjejjihvbnjhkshyle6rsvib4q7ss6jis37ozieivtolyw6qo4",
                        "bafyreiegcjkyvmkdokt7mjp6ukes7mr5byz3sxbcy5kvk4kzq5wgjhmuwu",
                        "bafyreigjumeksxuv4u3stmv3uis2zjcgt7dwsltge5vspdd52sitj6l35e",
                        "bafyreifn5x2qbrzbckslrdtzrj5ds4v23fa7gb4ehq7jhgfqckqkvvyfb4",
                        "bafyreievizxgpsppfbsbamcpoifcuhbjgxn3k5bzdatvfpcowdpenlwxf4",
                        "bafyreid7mmrvrvkia72td4fyldl4uu3jd74vnavracds3pcfvtrd7x2c64",
                        "bafyreibgldedgli2s3stggcqoefhltedouovhghmfji3mll7ud7qjhiywq",
                        "bafyreibvs3mvadlu73ux2n7vt2wwiwkyjmuphrdt643naxn545kebpx6zq",
                        "bafyreiehz75y66w6x4kgtbjivlgcbnyldscum7x3j3bx5x6aazojgdvmxy",
                        "bafyreifci7vratpgbegljwdulrbyz2quxv54tc27rsjinel5xnplfgbugi",
                        "bafyreif37mblyrz2lixaxjlh6btsmzeumun2pkryp4dj44tixso73wctxi",
                        "bafyreictaj5yyfskmm3c4uvjunr7ubodjwuaheqqhmjrwjsvrie4g6sszy",
                        "bafyreigwlamsurb37gojtvbhyc3hpvxes4rlqzhssxob57n3txe36xurw4",
                        "bafyreihcfokx26kybzc4lykrl6ghodt57qjebtzroky345nicgqdffbzlu",
                        "bafyreib4eadify34mg2h7ngj3hnxuwd5rpz6gnfdoek2lvfxfgdnpwebki",
                        "bafyreicwsogpjtmz6vo5hb7igwxxboekcb6sqxffagqryq5qfpzkg6g2nu",
                        "bafyreie4j4ehiyzuyjdfe3xlhnwsb4yrr72tohbxwbg4zryro6q73lwvx4",
                        "bafyreibcjb42puzvoy5e7xqn6yncyk7cb2zch67p2fflimvrzclpb6ayb4",
                        "bafyreig2tnsvrdb6v6lhtej7eby7rgsxjepdl3a7sphwhoejaesj3om5je",
                        "bafyreihxnmudf353tkgwjbgd3dsbvofmkfi5y6mzso7r6dsojmjpmsfrxy",
                        "bafyreie3lqjtnydo7vhuaozifkmbedkky4ndjvoxzmn2cfrrs74qmv2w2i",
                        "bafyreiad2b6qarttppzumqpa5jukz7d5jydjjozgjwudxzxkpoovpthqty",
                        "bafyreibucarddrqxfwdsyty7bspd5ao6fklayfqdvnxbcr3acekzwlxpae",
                        "bafyreigo2v3eb6eagg4sif4x4ybf5dejsnwcbbxj35wlocapideupatxn4",
                        "bafyreiap7aneqnbjlpcrnkgpssje7ils3t5pxllhk2blhxzk7w5zja364u",
                        "bafyreihnfrxdjjvhugjrfmswusas4atdof2tn3toiiniederizfiuqvkve",
                        "bafyreiaivbjsaegwjasysaxgslby3ck47qqfo43kseda4yrk7uoxpu2w7a",
                        "bafyreieghxibplszxzqi3xa735a3z7nisn767yrn2eo2dg3hxiebshwbwa",
                        "bafyreidbktchhrmytkmp4dmabrsqbzyrqttklzhvohac55rx6pqmoqjnvq",
                        "bafyreiaabogocc6h6abi4otsblke24l7v5kqxieb7k2wjsuht74jlwygyi",
                        "bafyreifm3cg2ukuxmpabbgnpw62zgurzmuo64plqxk6dnbdgsyxka4cpda",
                        "bafyreidmmqpdfn46cravwep2p4xusv2a2p6nahp27ztsaxofw3pxc3ehzu",
                        "bafyreiatuacmtj4fyvommecyiydidlkj4tcftxpofaehrzekuic2c5g4fa",
                        "bafyreidgvapadmwg2lvvyb2acad65iiav2vpvzrbxvqyasaf3fndfzqwxe",
                        "bafyreiazoctpsemmhlosv4wpwuvclboli7zpnkgmaszabzh2vtvyn6dbbe",
                        "bafyreianzo76ejvoyq7xqgqkyu75brplhmp75jltdnuubyewribzcflbbi",
                        "bafyreihk6vnuqdr5bld2qb7xvzfi4sjbhysiiyupzct3kp7upbusmiuvda",
                        "bafyreiaixtqyox5ymhk4gkkxghrrmbpy7ffimrmlngazwikto6jdsypm6u",
                        "bafyreiayhwy2yn2g7rgc65pnn3qqku4bsutrgnqmvngei4owcuq5ckfp54",
                        "bafyreidpczdtysbbucuv2ku6japqrcnkmpqxxlvpkxeecunp3q63hna74m",
                        "bafyreido7i7n5jkhjfx5grdaadmmevpp2ifxxx6y4avvjncoxd4kevsshy",
                        "bafyreidvv7mckn4cohucqda6ckrm667uuc4xwq5jlzxjghaulp33sgsxzu",
                        "bafyreidfguqbpqdnnstpkmxoqji7wgvzxdf4uxigxuspdhcgb5vqqdswdm",
                        "bafyreidzveienjrmf4hgmfyp6wzreazct5jns35iehsfjacfmehsq32da4",
                        "bafyreib7pcqbtffuz3v5smdh7uuo2ojt4tckwxf2geg3h7c5ml4tchi644",
                        "bafyreigc4qema3s24z2axrixraxmtwkfzlr54sqsuwqnzijpjx3ska2edm",
                        "bafyreie56qvcygvbyj4h6g6t3yvoar2d6of2t5lssxcvospr23mteo4tce",
                        "bafyreihv3az3lzekpadegmafcgn6jhpcw4awgg7ypkreuii6jts2df22y4",
                        "bafyreiain2cfmrvbczxbp5v4maoq5prizjvzpv57n7mcu3a36tnnzrinvm",
                        "bafyreievbheznnf3r6d4naswfywdgu52i6qgemofmvf2tmi3k6ohhxuxaq",
                        "bafyreihva4zfqiyjk7uh62w3ma66ngedut534ntjbl5y5q4qopl6c3dn3a",
                        "bafyreicdqu73vveyet5vz2fmn3tnb3q3sd75r6jtovx7spg5spbybbs7le",
                        "bafyreibxlrsjknuaxngadlngvjf5zgawpkppib4akspo3oz2xbsm5wgwwi",
                        "bafyreihtiusstwldx2hoek7fzq7olwdghxkaymvndai52nrtyvqh7ba6wy",
                        "bafyreicnvrum2uoe2dr3igr4fxgpztlqpga2bhrsyjpd67gp273smbt2fa",
                        "bafyreidzstejdvm4dytzjbbipvn4ojgyvw52aobzyvu5uwbmey2gi2tqua",
                        "bafyreifffo5boyjwyx4pr64ium2gp7fyvxavob4fjr4n2pzb46232tcyme",
                        "bafyreigmpuij5pyqpialgwy7umnoma2rtjzsamqhj6xzm2drxsapayzrj4",
                    ]),
                    meta: SlotMeta {
                        parent_slot: 16848017,
                        blocktime: 0,
                        block_height: None,
                    },
                    rewards: decode_cid("bafkqaaa"),
                },
            ),
            (
                decode_hex(
                    "86021a010114919847820000820101820202820303820404820505820606820707820808820909820a0a820b0b820c0c820d0d820e0e820f0f8210108211118212128213138214148215158216168217178218181818821819181982181a181a82181b181b82181c181c82181d181d82181e181e82181f181f821820182082182118218218221822821823182382182418248218251825821826182682182718278218281828821829182982182a182a82182b2082182c182b82182d182c82182e182d82182f182e821830182f82183118308218321831821833183282183418338218351834821836183582183718368218381837821839183882183a183982183b183a82183c183b82183d183c82183e183d82183f183e821840183f8218411840821842208218432082184418428218452082184618439847d82a582500017112206bfcde9a6f23d03b33f2a4d1135dc3970815719b09a956b883b31d4eb85023e1d82a5825000171122003ab19de28ded2d7d665a0b1896beddfb30ca88aef70a9adcf6b1cc5b314c38ad82a582500017112204be442543d49422e83ee44ec51611359a624fd6e4049d420d69f51bc733ecf9fd82a5825000171122011842c59e6fd486e598a18ed43b239b0b02c0606797aa08473f2a92e052cd339d82a58250001711220a0bfd4b8944fe01269bd3c2ac655e37be871204f7b44010e48a58a572a81aa22d82a58250001711220d27964f4e7d127c422172e4ddf6b082beda585eb04253d4adac0d0e149e3a094d82a582500017112207136492d9269abd5def0a873d6a5ff3f8e2925d3f1c871c10702f4f45514ccead82a582500017112203d802715ae82d143d5e3a6e29710d5827124400d7394c739ff563dc94a3ce4c9d82a5825000171122012c2adc3f0345d1867b02960618dd83043531b0e588dd937626402680dfb0476d82a5825000171122085136f0cef534efb9add7b40c97fd7ceb6c7e9c91fb8545a7458cde3cce8ef42d82a582500017112200698ee0994a17c50b9294b8ec53be81023cb617dbc84f0bc1410181dd4cf0092d82a582500017112205648dece36c600fe0d37325f618e17b8c2f464453f4047b1d0f52770e7a12c9bd82a58250001711220d4ca4f4d9aeea014438cbfdf1dc9c9edd30fcb2657959a2787571c8b33916bedd82a582500017112202a5b69e72190bad322dcf7fa84aa1c34b6519613498a9a31cc17a21bf28bfef3d82a582500017112200f6da15963d6a4655bb331981fda9874f68e3d447b2bb681c7c201288e57e075d82a58250001711220a7a3e0f698897433c5eddf6a1a7d50b7fd00417046fa2ddc5178bf9f2a31b3c7d82a582500017112206670b6daeaf058c670f175df1ce04e34cc9f76215163549f962dc7cab71cd468d82a5825000171122061047a232da59a98262a59b9a8c63b0d6b2ea28f771b0caac9b5a381b527f795d82a582500017112206aeb0b34d3bde27b6b5a3580454d3b28de64d9475dcd2703e66b2e644b33afa1d82a58250001711220d8fd8bdbe7436ff52f87bf64e6d1b4181569850a302b4e620d6b37236c66e334d82a582500017112207fa615d84889b7a6f0e8735ef3b3b5d338dc6b2d91f446cbb633f131034854e4d82a582500017112208bfdabe4decdf69e4483ad4e8f5a3412a915fc6ff49bb6480ec55bb37a9d1097d82a5825000171122012b1f5d0e10d896d7a2af66bcb29ea3a43b7e8c8987b8d1e42acd24925c4f076d82a58250001711220b49e48d5e9c1235747bc4b9cf5579a37dc499a45ddba0ae27c7ea7d6c7b9205cd82a582500017112204188264a5fe69e0b09430967f4c27860af844d67aa118df3b3463e90a3e81d63d82a58250001711220c0375cebcb34559aa7121c280e2964e4085bbb81d091e0808850c237510734c8d82a58250001711220d6d66d5e64f05606a2fbea42dfa0ba2f72e1d091810905c1f2fa5066f5ed76b5d82a5825000171122055f3b7c93951830ce6a2c266bb00a9491d3270f91a9e2efcefe4c1d93b31e04bd82a58250001711220352d36155e25429828f0bc8749c167d60276e11f770ea8a7d41ce489be359848d82a58250001711220f93964a6bacbd0ad3883592bc4c9d23d088afc51ad6b8ca0a5318ca6241bf944d82a5825000171122039bbc0c7b0f8cf709d3f3a7a3130c2bb0ba8dd09686ec5c42b78acb66aaf94c3d82a58250001711220f9471c545dff692d1cc5dd55da6ddec6c2ed38a9383779b3a969ac90a9719692d82a58250001711220b72e60b94a8f06ecd31c4141f47eecc7d36fa3523a79764e40ddeebd11825be0d82a58250001711220a83aecc6bad54d4655cdd3b908839002064a2d52d233d1516cc4b349e9bfb868d82a58250001711220d39c62d67914bff25fb9ba13e006e848a4f3e4ba92ecaae5f11ebe6acf61ad93d82a58250001711220040ae61e537254bc719f1b3822b79fc45d076cf143996c7a8abe01e52e0160d2d82a58250001711220411101afd2848478a42dacd6d340d64d43c3b801aa69f04ed985e9b499a1a018d82a58250001711220bb151b8fa2cd0d70ebd130cbba200ec2cdfce8b4d64cee4dcec669d812d0bd63d82a58250001711220daf52ad48c0012af789fa3b2e42083f030d637a6877a21bd4ad0f1a270094b03d82a58250001711220d928b6b97c978321a80d6833505c4e342e6c348e3477235c8a31cf5b90b756dad82a5825000171122001696f3dd57e7e6b94ac9b9170591af4ff2250516e9f2397b6789c061ad6ff1bd82a58250001711220fff0e3c77c797413d5c28a997eb119570fd176fa2a9cc0d57e7303e755ea2362d82a58250001711220deece89b3ef5b779616e7251eb7cc827e4d675dc0c620c83943abb0dcb086cafd82a582500017112204f95d45cb7b76d88ce55a039eb04836242b57ec2549e32b9b60c946a82691766d82a58250001711220579c3327438139b5ee7dbc569cbc68c13b59b48d2e7b3fe4691124ac8a3ae022d82a58250001711220b37a8a7ced7c7141e9d4deb04f7f4c8f1d8f6abe5f91c20ee096081e53c6fd15d82a58250001711220c002d99e4927216f5ede25a0a5ef1ba550971100682552d48df2fd44faff84bbd82a582500017112202fa698bd2bf345554ee38b2553d0b41af617761bde5faf2b960ddeb28f46a2aad82a58250001711220c97a7431b6ca37d37ba13a916e00a8b26c99d628ac0a50e5b7b618047124f555d82a58250001711220d70b24c64466132d125c3a05bc9f72f27020ef9ff498ec2faac069acb6998395d82a58250001711220fa0eb8847f5b2d0aa54ccc0604e3698dec799817d9600d19731259dd260f6ddad82a58250001711220826a03001e3e657458f14a694cf3f61b445b7da0bb129f89d42badf053148ad8d82a58250001711220b21b0d4f437cd97c066980bb8d279b67dea78231d2fc8435b2ef5ff149b40cbcd82a582500017112202035fa36ba77d4836629df19e1129822ee041be7079b04089431fa8a2dc3bdbfd82a58250001711220695ffec21df2470fe2352ae91270361f9fafcf37d44ba75d0b969523f26ad62cd82a58250001711220eb4687a7697d6752ff500e33ad6338e2ab3b885753a9d2e96af4f3afc216ebded82a582500017112206544bd83b387abbdd333475632415e1dcaf4d0f5b7071176aa26746cc8d0a88bd82a582500017112203a7473a5dee09bc016ce383bd5f3d8dd6f85cb8192005efd5790d48abab2478fd82a58250001711220dbff5685fa01080e5263a8e34658650430b0e209ecb19931ba0c6c66ad75e8ced82a582500017112209f275626e319b1273f3168c72ddc5f50836cff1ebae467749f0bd4f05443c4ced82a582500017112203723e4127daf6dd586c21d3020408d65eddf1a153e8fca5ec5a165c1398957d2d82a58250001711220dad7e6b619b3a3a6f43b3ec1f38052121e56797c9fd09ceeaa10f7fda2653139d82a582500017112202bcb7ad63dd201cb86eb90d58be28ec1e7c23832219018fe27c7df6b71b35f0ad82a5825000171122063bddb0041a532829b91df714ac0b3b27fd6f77f548563eccad1141189866a1cd82a5825000171122030c7b78e9e59bd51113a9a44962165196f056778ac68b33a8490acc169d12b6cd82a582500017112202fe6c9ec20ad535dba0d17370629609733e58b623221c21aadd7f7548ff1fb70d82a58250001711220def3d1d50d708a20e9936d9ae1645fabdab3b133c039535a68318f3477212c94d82a58250001711220b95ec1d63b4bbf4f1da29b59050942b2fcf84a95720052218b7d2573f09103e6d82a582500017112201c2d2370f9f44f0b06dfd13f6b476f0d4ed0112850c1d4b289fe691c516ffee4d82a5825000171122007337abf3c2b03a74bf2f2bdfc9f5b2c8f299cbb02224225c59643798953463cd82a58250001711220b01461cbe4a4add5144a70c8f754e62ecfecc2ac5a0e361ae43e4ce5402a636b831a0101149000f6d82a450001550000",
                ),
                Block {
                    slot: 16848017,
                    shredding: vec![
                        Shredding {
                            entry_end_idx: 0,
                            shred_end_idx: 0,
                        },
                        Shredding {
                            entry_end_idx: 1,
                            shred_end_idx: 1,
                        },
                        Shredding {
                            entry_end_idx: 2,
                            shred_end_idx: 2,
                        },
                        Shredding {
                            entry_end_idx: 3,
                            shred_end_idx: 3,
                        },
                        Shredding {
                            entry_end_idx: 4,
                            shred_end_idx: 4,
                        },
                        Shredding {
                            entry_end_idx: 5,
                            shred_end_idx: 5,
                        },
                        Shredding {
                            entry_end_idx: 6,
                            shred_end_idx: 6,
                        },
                        Shredding {
                            entry_end_idx: 7,
                            shred_end_idx: 7,
                        },
                        Shredding {
                            entry_end_idx: 8,
                            shred_end_idx: 8,
                        },
                        Shredding {
                            entry_end_idx: 9,
                            shred_end_idx: 9,
                        },
                        Shredding {
                            entry_end_idx: 10,
                            shred_end_idx: 10,
                        },
                        Shredding {
                            entry_end_idx: 11,
                            shred_end_idx: 11,
                        },
                        Shredding {
                            entry_end_idx: 12,
                            shred_end_idx: 12,
                        },
                        Shredding {
                            entry_end_idx: 13,
                            shred_end_idx: 13,
                        },
                        Shredding {
                            entry_end_idx: 14,
                            shred_end_idx: 14,
                        },
                        Shredding {
                            entry_end_idx: 15,
                            shred_end_idx: 15,
                        },
                        Shredding {
                            entry_end_idx: 16,
                            shred_end_idx: 16,
                        },
                        Shredding {
                            entry_end_idx: 17,
                            shred_end_idx: 17,
                        },
                        Shredding {
                            entry_end_idx: 18,
                            shred_end_idx: 18,
                        },
                        Shredding {
                            entry_end_idx: 19,
                            shred_end_idx: 19,
                        },
                        Shredding {
                            entry_end_idx: 20,
                            shred_end_idx: 20,
                        },
                        Shredding {
                            entry_end_idx: 21,
                            shred_end_idx: 21,
                        },
                        Shredding {
                            entry_end_idx: 22,
                            shred_end_idx: 22,
                        },
                        Shredding {
                            entry_end_idx: 23,
                            shred_end_idx: 23,
                        },
                        Shredding {
                            entry_end_idx: 24,
                            shred_end_idx: 24,
                        },
                        Shredding {
                            entry_end_idx: 25,
                            shred_end_idx: 25,
                        },
                        Shredding {
                            entry_end_idx: 26,
                            shred_end_idx: 26,
                        },
                        Shredding {
                            entry_end_idx: 27,
                            shred_end_idx: 27,
                        },
                        Shredding {
                            entry_end_idx: 28,
                            shred_end_idx: 28,
                        },
                        Shredding {
                            entry_end_idx: 29,
                            shred_end_idx: 29,
                        },
                        Shredding {
                            entry_end_idx: 30,
                            shred_end_idx: 30,
                        },
                        Shredding {
                            entry_end_idx: 31,
                            shred_end_idx: 31,
                        },
                        Shredding {
                            entry_end_idx: 32,
                            shred_end_idx: 32,
                        },
                        Shredding {
                            entry_end_idx: 33,
                            shred_end_idx: 33,
                        },
                        Shredding {
                            entry_end_idx: 34,
                            shred_end_idx: 34,
                        },
                        Shredding {
                            entry_end_idx: 35,
                            shred_end_idx: 35,
                        },
                        Shredding {
                            entry_end_idx: 36,
                            shred_end_idx: 36,
                        },
                        Shredding {
                            entry_end_idx: 37,
                            shred_end_idx: 37,
                        },
                        Shredding {
                            entry_end_idx: 38,
                            shred_end_idx: 38,
                        },
                        Shredding {
                            entry_end_idx: 39,
                            shred_end_idx: 39,
                        },
                        Shredding {
                            entry_end_idx: 40,
                            shred_end_idx: 40,
                        },
                        Shredding {
                            entry_end_idx: 41,
                            shred_end_idx: 41,
                        },
                        Shredding {
                            entry_end_idx: 42,
                            shred_end_idx: 42,
                        },
                        Shredding {
                            entry_end_idx: 43,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 44,
                            shred_end_idx: 43,
                        },
                        Shredding {
                            entry_end_idx: 45,
                            shred_end_idx: 44,
                        },
                        Shredding {
                            entry_end_idx: 46,
                            shred_end_idx: 45,
                        },
                        Shredding {
                            entry_end_idx: 47,
                            shred_end_idx: 46,
                        },
                        Shredding {
                            entry_end_idx: 48,
                            shred_end_idx: 47,
                        },
                        Shredding {
                            entry_end_idx: 49,
                            shred_end_idx: 48,
                        },
                        Shredding {
                            entry_end_idx: 50,
                            shred_end_idx: 49,
                        },
                        Shredding {
                            entry_end_idx: 51,
                            shred_end_idx: 50,
                        },
                        Shredding {
                            entry_end_idx: 52,
                            shred_end_idx: 51,
                        },
                        Shredding {
                            entry_end_idx: 53,
                            shred_end_idx: 52,
                        },
                        Shredding {
                            entry_end_idx: 54,
                            shred_end_idx: 53,
                        },
                        Shredding {
                            entry_end_idx: 55,
                            shred_end_idx: 54,
                        },
                        Shredding {
                            entry_end_idx: 56,
                            shred_end_idx: 55,
                        },
                        Shredding {
                            entry_end_idx: 57,
                            shred_end_idx: 56,
                        },
                        Shredding {
                            entry_end_idx: 58,
                            shred_end_idx: 57,
                        },
                        Shredding {
                            entry_end_idx: 59,
                            shred_end_idx: 58,
                        },
                        Shredding {
                            entry_end_idx: 60,
                            shred_end_idx: 59,
                        },
                        Shredding {
                            entry_end_idx: 61,
                            shred_end_idx: 60,
                        },
                        Shredding {
                            entry_end_idx: 62,
                            shred_end_idx: 61,
                        },
                        Shredding {
                            entry_end_idx: 63,
                            shred_end_idx: 62,
                        },
                        Shredding {
                            entry_end_idx: 64,
                            shred_end_idx: 63,
                        },
                        Shredding {
                            entry_end_idx: 65,
                            shred_end_idx: 64,
                        },
                        Shredding {
                            entry_end_idx: 66,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 67,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 68,
                            shred_end_idx: 66,
                        },
                        Shredding {
                            entry_end_idx: 69,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 70,
                            shred_end_idx: 67,
                        },
                    ],
                    entries: decode_cids([
                        "bafyreidl7tpju3zd2a5th4ve2ejv3q4xbakxdgyjvfllra5tdvhlqubd4e",
                        "bafyreiadvmm54kg62ll5mznawgewx3o7wmgkrcxpocu23t3ldtc3gfgdri",
                        "bafyreicl4rbfipkjiixih3se5riwce2zuysp23sajhkcbvu7kg6hgpwpt4",
                        "bafyreiarqqwftzx5jbxftcqy5vb3eonqwawambtzpkqii47svexaklgthe",
                        "bafyreifax7klrfcp4ajgtpj4fldfly335bysat33iqaq4sffrjlsvankei",
                        "bafyreigspfspjz6re7ccefzojxpwwcbl5wsyl2yeeu6uvwwa2dquty5asq",
                        "bafyreidrgzes3etjvpk554fioplkl7z7ryuslu7rzby4cbyc6t2fkfgm5i",
                        "bafyreib5qatrlluc2fb5ly5g4klrbvmcoeseadltstdtt72whxeuupheze",
                        "bafyreiasykw4h4bulumgpmbjmbqy3wbqinjrwdsyrxmtoyteajua36yeoy",
                        "bafyreiefcnxqz32tj35zvxl3idex7v6ow3d6tsi7xbkfu5cyzxr4z2hpii",
                        "bafyreiagtdxatffbprilskklr3ctx2aqepfwc7n4qtylyfaqdao5jtyasi",
                        "bafyreicwjdpm4nwgad7a2nzsl5qy4f5yyl2girj7ibd3duhve5yopijmtm",
                        "bafyreiguzjhu3gxouakehdf734o4tspn2mh4wjsxswncpb2xdsfthell5u",
                        "bafyreibklnu6oimqxljsfxhx7kckuhbuwzizme2jrknddtaxuin7fc766m",
                        "bafyreiapnwqvsy6wursvxmzrtap5vgdu62hd2rd3fo3idr6caeui4v7aou",
                        "bafyreifhupqpngejoqz4l3o7ninh2ufx7uaec4cg7iw5yulyx6psumnty4",
                        "bafyreidgoc3nv2xqlddhb4lv34ooatruzspxmikrmnkj7frny7flohguna",
                        "bafyreidbar5cglnftkmcmkszxgummoynnmxkfd3xdmgkvsnvuoa3kj7xsu",
                        "bafyreidk5mftju554j5wwwrvqbcu2ozi3zsnsr25zutqhztlfzsewm5pue",
                        "bafyreigy7wf5xz2dn72s7b57mttndnaycvuykcrqfnhgedllg4rwyzxdgq",
                        "bafyreid7uyk5qsejw6tpb2dtl3z3hnothdogwlmr6rdmxnrt6eyqgscu4q",
                        "bafyreiel7wv6jxwn62peja5nj2hvunasvek7y37uto3eqdwflozxvhiqs4",
                        "bafyreiaswh25byinrfwxukxwnpfst2r2io36rseypogr4qvm2jeslrhqoy",
                        "bafyreifutzenl2obenluppcltt2vpgrx3rezuro5xifoe7d6u7lmpojalq",
                        "bafyreicbrateux7gtyfqsqyjm72me6dav6ce2z5kcgg7hm2gh2ikh2a5mm",
                        "bafyreigag5ooxszukwnkoeq4fahcszhebbn3xaoqshqibccqyi3vcbzuza",
                        "bafyreigw2zwv4zhqkydkf67kilp2borpolq5bembbec4d4x2kbtpl3lwwu",
                        "bafyreicv6o34sokrqmgoniwcm25qbkkjduzhb6i2tyxpz37eyhmtwmpajm",
                        "bafyreibvfu3bkxrfikmcr4f4q5e4cz6waj3och3xb2ukpva44se34nmyja",
                        "bafyreihzhfsknowl2cwtra2zfpcmtur5bcfpyunnnogkbjjrrstcig7ziq",
                        "bafyreibzxpampmhyz5yj2pz2piytbqv3boun2clin3c4ik3yvs3gvl4uym",
                        "bafyreihzi4ofixp7newrzro5kxng3xwgylwtrkjyg543hkljvsiks4mwsi",
                        "bafyreifxfzqlssupa3wnghcbih2h53gh2nx2gur2pf3e4qg5526rdas34a",
                        "bafyreifihlwmnowvjvdfltotxeeiheacazfc2uwsgpivc3gewne6tp5yna",
                        "bafyreigttrrnm6iux7zf7on2cpqan2ciutz6jous5svol4i6xzvm6ynnsm",
                        "bafyreiaebltb4u3sks6hdhy3harlph6eludwz4kdtfwhvcv6ahss4ala2i",
                        "bafyreicbcea27uueqr4kilnm23jubvsnipb3qanknhye5wmf5g2jtinada",
                        "bafyreif3cuny7iwnbvyoxujqzo5cadwczx6orngwjtxe3twgnhmbfuf5mm",
                        "bafyreig26uvnjdaackxxrh5dwlscba7qgdldpjuhpiq32swq6grhacklam",
                        "bafyreigzfc3ls7exqmq2qdlignifytrufzwdjdruo4rvzcrrz5nzbn2w3i",
                        "bafyreiabnfxt3vl6pzvzjle3sfyfsgxu74rfaulot4rzpntytqdbvvx7dm",
                        "bafyreih76dr4o7dzoqj5lquktf7lcgkxb7ixn6rkttank7ttaptvl2rdmi",
                        "bafyreig65tujwpxvw54wc3tskhvxzsbh4tlhlxammigihfb2xmg4wcdmv4",
                        "bafyreicpsxkfzn5xnwem4vnahhvqja3cik2x5qsutyzltnqmsrvie2ixmy",
                        "bafyreicxtqzsoq4bhg2647n4k2oly2gbhnm3jdjopm76i2ireswiuoxaei",
                        "bafyreiftpkfhz3l4ofa6tvg6wbhx6tepdwhwvps7shba5yewbapfhrx5cu",
                        "bafyreigaalmz4sjhefxv5xrfucs66g5fkclrcadievjnjdps7vcpv74exm",
                        "bafyreibpu2ml2k7tivku5y4levj5bna26ylxmg66l6xsxfqn32zi6rvcvi",
                        "bafyreigjpj2ddnwkg7jxxij2sfxabkfsnsm5mkfmbjioln5wdachcjhvku",
                        "bafyreigxbmsmmrdgcmwrexb2aw6j64xsoaqo7h7utdwc7kwangwlngmdsu",
                        "bafyreih2b24ii723fufkktgmaycog2mn5r4zqf6zmagrs4yslhosmd3n3i",
                        "bafyreiecnibqahr6mv2fr4kknfgph5q3irnx3if3ckpytvblvxyfgfek3a",
                        "bafyreifsdmgu6q343f6am2maxogspg3h32tyemos7scdlmxpl7yutnamxq",
                        "bafyreibagx5dnotx2sbwmko7dhqrfgbc5ycbxzyhtmcarfbr7kfc3q55x4",
                        "bafyreidjl77mehpsi4h6enjk5ejhanq7t6x46n6ujotv2c4wsur7e2wwfq",
                        "bafyreihli2d2o2l5m5jp6uaogowwgohcvm5yqv2tvhjos2xu6ox4efxl3y",
                        "bafyreidfis6yhm4hvo65gm2hkyzecxq5zl2nb5nxa4ixnkrgorwmrufirm",
                        "bafyreib2orz2lxxatpabntryhpk7hwg5n6c4xamsabpp2v4q2sflvmshr4",
                        "bafyreig375lil6qbbahfey5i4ndfqziegcyoecpmwgmtdoqmnrtk25pizy",
                        "bafyreie7e5lcnyyzwett6mliy4w5yx2qqnwp6hv24rtxjhyl2tyfiq6ezy",
                        "bafyreibxepsbe7npnxkynqq5gaqebdlf5xprufj6r7ff5rnbmxattckx2i",
                        "bafyreig227tlmgntuotpioz6yhzyauqsdzlhs7e72coo5kqq6762ezjrhe",
                        "bafyreiblzn5nmposahfyn24q2wf6fdwb47bdqmrbsamp4j6h35vxdm27bi",
                        "bafyreiddxxnqaqnfgkbjxeo7offmbm5sp7lpo72uqvr6zswrcqiytbtkdq",
                        "bafyreibqy63y5hszxvircou2islcczizn4cwo6fmncztvbeqvtawtujlnq",
                        "bafyreibp43e6yifnkno3udixg4dcsyexgpsywyrsehbbvlox65ki74p3oa",
                        "bafyreig66pi5kdlqriqote3ntlqwix5l3kz3cm6ahfjvu2brr42hoijmsq",
                        "bafyreifzl3a5mo2lx5hr3iu3lecqsqvs7t4evflsabjcdc35evz7beid4y",
                        "bafyreia4furxb6puj4fqnx6rh5vuo3ynj3ibckcqyhklfcp6neofc3764q",
                        "bafyreiahgn5l6pblaotux4xsxx6j6wzmr4uzzoycejbclrmwin4ysu2ghq",
                        "bafyreifqcrq4xzfevxkristqzd3vjzroz7wmflc2by3bvzb6jtsuaktdnm",
                    ]),
                    meta: SlotMeta {
                        parent_slot: 16848016,
                        blocktime: 0,
                        block_height: None,
                    },
                    rewards: decode_cid("bafkqaaa"),
                },
            ),
            (
                decode_hex(
                    "86021a0101148b987d820000820101820202820303820404820505820606820707820808820909820a0a820b0b820c20820d20820e0d820f2082100e82112082122082130f821420821520821611821720821818208218192082181a1382181b2082181c2082181d2082181e2082181f1582182020821821168218221782182318188218241819821825181a821826181b821827181c821828181d821829181e82182a181f82182b182082182c182182182d182282182e182382182f182482183018258218311826821832182782183318288218341829821835182a821836182b821837182c821838182d821839182e82183a182f82183b183082183c183182183d183282183e183382183f183482184018358218411836821842183782184318388218441839821845183a821846183b821847183c821848183d821849183e82184a183f82184b184082184c2082184d2082184e184182184f208218502082185120821852184382185320821854184482185518458218562082185720821858208218592082185a2082185b2082185c2082185d184882185e2082185f208218602082186120821862208218632082186420821865184b8218662082186720821868208218692082186a2082186b2082186c2082186d184e82186e2082186f20821870208218712082187220821873208218742082187518518218762082187718528218781853821879185482187a185582187b185682187c1857987dd82a58250001711220a85a601ca60c4b4d414dc74418a679c661bef8dfaf87fb02e17ec26a03c2be6bd82a582500017112207d2a75dda2fa09f9227faf331088810a51f3d1a49d24fc85febdc89d4a90879cd82a58250001711220ac036e390ef4cf0df83848da1ed7e4778c20ed281a5d137a1ef59a3932bed090d82a58250001711220d4e2c57c7a1f940aed8eb334a5828675f902543fb145e4690a5dbaae6538e383d82a58250001711220ae48bc0559a103390b1da6d114dc06c63d2de088e4289ef0ffe3b2b90ddfdbe5d82a58250001711220122458bae9d73d8306c653beae5c8af621ae21843aefd53b5c377eb1e1c57441d82a58250001711220bcd171d11173998e2653be48aeb84b372208dc121c6f7add182b6742022d899ed82a58250001711220d12369ce2a4103d1eba6b3652bce04db8fa0e264a3993d2b0f5c7a7e17543e4ed82a58250001711220495082622e2824666c7dd7aae905e380c9fac374beb5b7a88e7d6c8ad4dbb37fd82a582500017112205ff4067cfd8acdcc0504bcaa176d24aebf51e6fc446b79206c51cfbbefb292c0d82a582500017112209044123bdf21ca673863c54d7f17d0456945f769f91347ffcd83a9681c63c5b2d82a5825000171122098ef4e6abbd2a5c33601e4e6fbcc9f749eb8d37aa118b2d0880007bc23c4a429d82a58250001711220a1e811bfc0c8aaae4e2547bc9ffc5fe91f77a7ed3c6e13a5df025fb4118a0509d82a58250001711220ac4d4b53c54b88845826bfdc0df3ab21cbaeb7eaeb0fe21f5bd63195656adf84d82a58250001711220dc2c0415af440b68e5cb06c6c5a630f1d5d643130dc834269588aa149cb4bb1cd82a58250001711220e3c5b09918552bc3146d65a83f9eb835541499f4721f47e3b7e6cad91c1278d3d82a5825000171122024128f6d4d66e2a1b762da3e432117b9871c0136e29460ad1bca78a6e5fb3955d82a582500017112202bd0b59be1fe439ee7c12b15683f60715240e4c10db16ef22a456e3a507c46bcd82a58250001711220ee560717e92216c086395a35251c774286c9b8432ee077006f5cd04c268169d0d82a58250001711220b4a4c8d32d6351293297b6562d0b74ad768cf08a074ba0ae26ea245958e40e64d82a5825000171122053196e14dc5287c9f20f0ab84e4364c77e18c80807b066c28bc42465cfd58039d82a582500017112208845c07448772d2bf4a66327348d204080ca42735e433a37409bda2d6c57be26d82a582500017112209419d299829ac15944176ceb899392ec463dac4b688030fea606a891e8d1d6ebd82a58250001711220ff95bfe16eaa82fddb69ffe92c91c63727c7a1a1273e4c6eae25558378b61910d82a58250001711220a054bc303186055fe1a91c7b976e5bab5c60bd13119246253793cecfc2fb2c74d82a58250001711220529abc76611d50ba827b1cb36d8b50d86958b931d0fd5ddf32d30ee00a252332d82a5825000171122022e2495b00df1785a149aeb6b4fa628f16ecfb8ab3beb3aefe0034a7423fd966d82a5825000171122012501ba765f96662255e27c4c0521f996afc983acdc715590b0ebc83cd26e354d82a58250001711220551d89ffc9c0cc2dc504de25defc81d038b4197d642c4874054282cf0985ca5fd82a582500017112202cdd6e9b4beef41af52ec8853d4c2f4437b357c5ae9d07fb341c9c0c5530ed56d82a58250001711220cb122e2fb09ba6fbdb0a3fbd493ff446b696afda7446b4b50385c067c33d34a7d82a582500017112203e28731445efe1a61ce9a924dbc41dc1210f54381348dc0c7bc2b573ca1bd595d82a58250001711220dbaed7365bbab49349169fc6e84e94921d485ef7df8a6d2c7be45d727fd8ed1bd82a58250001711220e1e3324afc252b2fdd170a42ef78d14a42f09ebb45b5c1d58d90715fceec0c0ed82a58250001711220438dce327ca0c36da4ef3b683778277a7d92cbf36ab70e1706963763105ceb7bd82a582500017112206cbdb506dbbca1e4d95efab5e2694f4bd160c46bb0581132a6a987523db0cd66d82a5825000171122043453a6e1f79bea4d9da541ffabdb5d4604316da1b8b9dfdeff1c11b984781cfd82a582500017112202238ca5fcf8c723030e98cb7513190cb7bde7e8ab0491cb57767f5d091ccf469d82a5825000171122056053c26c48cf029f938506a27931112c86267ba9bab5734701678e58d6f69ded82a5825000171122079c4e7e2a55d3d1cb3c382e45f93b85e46144f51d65ebc7a3515c734dda11129d82a58250001711220923da5e6a6c7bcb9789fb9dbc94f237926cb2404f11a072fcea044acd1da38efd82a58250001711220f4c628b9bcc3e7e5714f9ad7d3eeb048d46bfd1d689b757fc70a6a31c988a00dd82a582500017112209ec948d917d1544a675b788e2b5121d29f4312bac5690a2f25d76456eb8a1ae9d82a58250001711220b341af846d916f7008eecf8e49e677d65c6e520d2cec6866634008ca40cf6287d82a5825000171122079e25c94b7da4944031d4ba70ebd7c2b12c1a4a01bbf4d0000091ca2e212e3c2d82a582500017112201929b7edb951208d533382aa26dda068cdc86ac92f1370d9a0053208fad7714cd82a5825000171122045a770980c21a4e997c174328d0e2defd84ee0b44d37cc83fdb026c13c57a16ad82a582500017112209e086d05dd0dbe579fe43a6d1a505c00f92212e83c3f61d46109576d349ca67dd82a5825000171122011816eef0b36d7dbf9fbe8a1a4e71132c90bf3202a8f83085c0ff5dae96aa139d82a5825000171122016073319c2c88029267ba63730f7e7c295f193879c8fa6300119f5e365fbcf01d82a582500017112209512d0b199718cdb37b6a5087923d8e370879f8eb9d6e9ccd5e4eb8c9b35478dd82a582500017112202025257fafd9651b8f2fc028dfe8838c8af03d96d2c04a7444eb443a3d1699aed82a582500017112206af3e6bd9221f0b7219792e1ea3bb82520d1a850d4b668534b00caac72a9ae39d82a58250001711220a43b29203edab69aad5c041254444f668f6035ab942dd9e9a7e380374fba3429d82a58250001711220a7a3ba0d5131e12b822d9d1fe905adf63280f24d19a52f80328cee9f1de57d0ed82a5825000171122038a1167a06c2a63f57b94f427c70055330126c88a87fb627692942489378f532d82a58250001711220104f742d6ed0722e57e81bac19e1ca700ddf8139e5feb938f24bb1685aaa14a3d82a58250001711220762df63efb0062005e4a529d92321cf65e1b6261acb3c87a7200efe300da558cd82a5825000171122014a336324357744e74638ae8eb78fa3b57312f5c003546756a093a5ea22e49cfd82a582500017112209ddef408c9cbe31cc8346e573b6f49b7db88e77ad962f811375c0dc160214eddd82a582500017112204d697f912b5c333e9231f03cf7468920ea048594f04497d8d65f52e2a62a12e3d82a58250001711220030613708233836e9773efe70c63ead7ba90e771d1e453526e554314a1c77dffd82a58250001711220ea5a95dd640d2243d9482bdcfbe9a06eef9cde6272a8ae40038564df08b98279d82a5825000171122093f7bf25c3a287c60f8a24f85f9bca3a34add55696d90c6072df06eec038995bd82a5825000171122040b3449b7c3929580a79ec406d7edca7c913d1417e853a2236a28ff95e3b0684d82a58250001711220c0e4d8b099518349f93e578ba17d7946acba6db40ada83a52f230903e860c315d82a58250001711220bb18d31c2d52ae331763be485f904f8eaa55879d62cdd7b3a5da0c4cb9988f3bd82a582500017112208a2804d2a1d0b24b2757482b764de93f6c94d9f083424b6d37bf5c2e715e8cced82a582500017112205444ca369f7b15f8b9cb2cc67d739ee7458dec59860f6c93a1b3494f544e5536d82a582500017112208f9e3cd7a7a81a0797b7a6a0a94baa032b5f9f903cb58205bf83a0cffa8685aed82a582500017112203a4b99c56eb2d755f114004eca68c6388196741aa29da0c5ee8bd01eaec89502d82a58250001711220317dd2cdd4d7d4fe75d459f8c85bd0b4943892a740f86005f64100ed7083f648d82a582500017112202ab4fee352f6c0da0e0b682ee44db59fe71129523db13b6747703ffd0a8de2fdd82a5825000171122098fa137ebeb37da7e54e85d4a587839e693ec8bea22009b016d2058f988e352fd82a5825000171122008021cc398346d0f23c30fae8c41641062e01ecdea5a943b85e3202648327f3ad82a58250001711220e3eae13abfa5d597621ce5985eecb1c8b8d293f9957bf1de68d5d2d95c657c44d82a582500017112202eb792c6640e0c8c2916fb86ab01d7bb07b077cb455e388125b2aa0d4757c63bd82a582500017112203cf3830b3f3c74f0277bd7babb0d275ebfc1b43673c30d456bbc92c4d5f2501cd82a58250001711220e83f158e08de222fc2ff221aacdbb8d72032ed42e170301bd7ffd2af3609dbe0d82a58250001711220df685427bab0ff0b93a9bb4e62cc6383c3951f6d237e99227b4e2adf75c16b3cd82a58250001711220865b85b50bbb7d9da17fd44139633689a8d2477f5a94a415478b1e575e23c3f9d82a58250001711220e5e13529e2568f15e3f79150c9f8ce8f4a1e32f0c88b2d193c2a8ae370f23a24d82a5825000171122088a59dbb11a52fcadce00c4d69c2418f6d7541f373cf0f7a7c2dab28298ef3bdd82a58250001711220961b353ab072a0d939c574776f919d5065f9eaca3a1064ccb329c8bdb376d13bd82a5825000171122066ac6a868af71f78f5af171e872853b604faeee1ed076f4a25e4c0fef2d32a40d82a582500017112200d94d31ca4572c821611ce25f90b8f46dd01486de430b1a75a46332303101dd4d82a582500017112200ed9f44044ae67ed3e75b5cfa5a26aac10315704decfe136308d00211802cffed82a58250001711220387b03aa5614903121e2dde929bb38607cead311945c13379b9b6b9cc02f1544d82a58250001711220848d3e84570679d5e33b5ee2380abf4b5851f741b2f1d649272619ba22578f64d82a58250001711220981efd7a06bae17973846c5d829f98a47bbd6c52371658f2288620377accdc2cd82a58250001711220bae9336b71c0e3a67fb647da46499a78fbd9f51573ad73c570bbb1f444eafac0d82a58250001711220ddc743d09f64562b75930324c56750522dc23b51748661b2af54cdcfa9feb227d82a582500017112207c38c641812c0f01267508a9ca44d8312256d9e2e524103a85bafdef12165a81d82a58250001711220447f3a446ecb4ade60445e08c70e81bee8dc750c8e8c3017b01b8b713a79c1d8d82a58250001711220f60394b4c0f393729b35e55c2c59cc1e69e8529bcdb543e83ac359ff18424319d82a5825000171122021bb0176aa664319aa15dfdee3860ce1fbb7808a3fff2c57d80c0ce7a0c1c5ccd82a582500017112204f74248fbe8560d5661e95f5357f5d7f9a58ed69ba42a8b1636eb18ab5f9d6d9d82a5825000171122007de92a77f8a26e9ca617f5c1ee8e93d724bff90a26effb068581b03ffcec75ad82a58250001711220d4cea1101468155aa35dbd9e8c29a9ade853cc247a0216bcd5ca8b07ffc610ddd82a58250001711220ae2018bc1bfbd2de34e9e6bf9763cc4b7b7d1d5c8ee72dbf066194c23e523628d82a58250001711220c07fa620924aa8e8c4ce15719027c6d8a730199bc3670d8d11905d5841b86847d82a5825000171122015af932df144e2db082f692ead7820794c807c8d25059fbfcd0f248201d6ecc4d82a582500017112204cb2367a0c927fc1839f36f5c77f1985e32dccdfeb39c135c46d07cdc572b849d82a58250001711220a3639bc9060a2685d913c87b4142824d5987af8f38d76de01b713654d01aead9d82a58250001711220424762429ed7206d39dcfef0dca9bedc1de509271eea0de5e5a778f92dbe03f2d82a582500017112209c386c92bff1d06a559d2b6c10097283835770d8d3ded56f8d06f14dae994bd6d82a58250001711220422fbb50b93eaeb8c8b75091e7a23030011d80a3ce13e28c71d549b53c453327d82a582500017112204a562d4acb548459c00078a89b92e7c3071b89bb9cdf5dae05cf105dff6a7ecfd82a582500017112207acd42d895b1a3b42bc83bcfc62faedfc4c0d3e715b0ed20fa7f2a0dd50860f2d82a58250001711220bc86f529fcc32f6f046f9d29dddb6188eab39915cb6e0475791797bce37db4f8d82a582500017112200c9243876646a7b18c90bcec0456a8dc46d617b60155d359507fa89de33cb883d82a58250001711220aeaf310d7759e57b262997107cb3c0d3e4f6c75b468b596ff6d837f4ed95be30d82a58250001711220babd05d8114485b65e206226406e135a141559ab9fa15e8b6be4e555f4ea89dfd82a58250001711220ea0b501bdc8b1555d8929969177d13696dbd0eef628b0e012374496992ec37aed82a582500017112203c78c33083d7a4e96de846f7f53eb2ef24ca7a8731579f2c5bef8cf270bdbe88d82a58250001711220458291d782ae9f7034104fc63833231cdb180d388baf4c0437e342832228b995d82a582500017112205f872f7994238b38bd7cba6ffdc87b16322d443ed8a565b8bbbf69b58c90a20cd82a5825000171122056e39bb87080c311020a17e3974fe31a18a108df59da58d4c5732213a831734fd82a58250001711220fca0744cf73d0dfdbfe57ce4cce1c3f9e9b4c4dda9507e497d45fd02c47482abd82a58250001711220e47271e0ae03ef927a946b467054ce43e160247b4c37fe6ea18e79516d967626d82a582500017112208ea44476a7c69ef06c0f8066198deec60cbe37fdf829dd2d0aee45049da58fb1d82a58250001711220a03083127494afc8aed44b4cb73d85e636359cef0e4b029e876d4fbd92826c3ad82a582500017112208d0d81d9e09701c9660ee1969931a95d7288d8e9c3c6403011e218c258607da5d82a58250001711220be6f36a6dfac48dfe14afe4f845d0aa7745987d2c9f7579693b95393b712d642d82a58250001711220d22296aad1059f1b358ea60db4d4c083017b18bfd26673a254546259b528b5dc831a0101148a00f6d82a450001550000",
                ),
                Block {
                    slot: 16848011,
                    shredding: vec![
                        Shredding {
                            entry_end_idx: 0,
                            shred_end_idx: 0,
                        },
                        Shredding {
                            entry_end_idx: 1,
                            shred_end_idx: 1,
                        },
                        Shredding {
                            entry_end_idx: 2,
                            shred_end_idx: 2,
                        },
                        Shredding {
                            entry_end_idx: 3,
                            shred_end_idx: 3,
                        },
                        Shredding {
                            entry_end_idx: 4,
                            shred_end_idx: 4,
                        },
                        Shredding {
                            entry_end_idx: 5,
                            shred_end_idx: 5,
                        },
                        Shredding {
                            entry_end_idx: 6,
                            shred_end_idx: 6,
                        },
                        Shredding {
                            entry_end_idx: 7,
                            shred_end_idx: 7,
                        },
                        Shredding {
                            entry_end_idx: 8,
                            shred_end_idx: 8,
                        },
                        Shredding {
                            entry_end_idx: 9,
                            shred_end_idx: 9,
                        },
                        Shredding {
                            entry_end_idx: 10,
                            shred_end_idx: 10,
                        },
                        Shredding {
                            entry_end_idx: 11,
                            shred_end_idx: 11,
                        },
                        Shredding {
                            entry_end_idx: 12,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 13,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 14,
                            shred_end_idx: 13,
                        },
                        Shredding {
                            entry_end_idx: 15,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 16,
                            shred_end_idx: 14,
                        },
                        Shredding {
                            entry_end_idx: 17,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 18,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 19,
                            shred_end_idx: 15,
                        },
                        Shredding {
                            entry_end_idx: 20,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 21,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 22,
                            shred_end_idx: 17,
                        },
                        Shredding {
                            entry_end_idx: 23,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 24,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 25,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 26,
                            shred_end_idx: 19,
                        },
                        Shredding {
                            entry_end_idx: 27,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 28,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 29,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 30,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 31,
                            shred_end_idx: 21,
                        },
                        Shredding {
                            entry_end_idx: 32,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 33,
                            shred_end_idx: 22,
                        },
                        Shredding {
                            entry_end_idx: 34,
                            shred_end_idx: 23,
                        },
                        Shredding {
                            entry_end_idx: 35,
                            shred_end_idx: 24,
                        },
                        Shredding {
                            entry_end_idx: 36,
                            shred_end_idx: 25,
                        },
                        Shredding {
                            entry_end_idx: 37,
                            shred_end_idx: 26,
                        },
                        Shredding {
                            entry_end_idx: 38,
                            shred_end_idx: 27,
                        },
                        Shredding {
                            entry_end_idx: 39,
                            shred_end_idx: 28,
                        },
                        Shredding {
                            entry_end_idx: 40,
                            shred_end_idx: 29,
                        },
                        Shredding {
                            entry_end_idx: 41,
                            shred_end_idx: 30,
                        },
                        Shredding {
                            entry_end_idx: 42,
                            shred_end_idx: 31,
                        },
                        Shredding {
                            entry_end_idx: 43,
                            shred_end_idx: 32,
                        },
                        Shredding {
                            entry_end_idx: 44,
                            shred_end_idx: 33,
                        },
                        Shredding {
                            entry_end_idx: 45,
                            shred_end_idx: 34,
                        },
                        Shredding {
                            entry_end_idx: 46,
                            shred_end_idx: 35,
                        },
                        Shredding {
                            entry_end_idx: 47,
                            shred_end_idx: 36,
                        },
                        Shredding {
                            entry_end_idx: 48,
                            shred_end_idx: 37,
                        },
                        Shredding {
                            entry_end_idx: 49,
                            shred_end_idx: 38,
                        },
                        Shredding {
                            entry_end_idx: 50,
                            shred_end_idx: 39,
                        },
                        Shredding {
                            entry_end_idx: 51,
                            shred_end_idx: 40,
                        },
                        Shredding {
                            entry_end_idx: 52,
                            shred_end_idx: 41,
                        },
                        Shredding {
                            entry_end_idx: 53,
                            shred_end_idx: 42,
                        },
                        Shredding {
                            entry_end_idx: 54,
                            shred_end_idx: 43,
                        },
                        Shredding {
                            entry_end_idx: 55,
                            shred_end_idx: 44,
                        },
                        Shredding {
                            entry_end_idx: 56,
                            shred_end_idx: 45,
                        },
                        Shredding {
                            entry_end_idx: 57,
                            shred_end_idx: 46,
                        },
                        Shredding {
                            entry_end_idx: 58,
                            shred_end_idx: 47,
                        },
                        Shredding {
                            entry_end_idx: 59,
                            shred_end_idx: 48,
                        },
                        Shredding {
                            entry_end_idx: 60,
                            shred_end_idx: 49,
                        },
                        Shredding {
                            entry_end_idx: 61,
                            shred_end_idx: 50,
                        },
                        Shredding {
                            entry_end_idx: 62,
                            shred_end_idx: 51,
                        },
                        Shredding {
                            entry_end_idx: 63,
                            shred_end_idx: 52,
                        },
                        Shredding {
                            entry_end_idx: 64,
                            shred_end_idx: 53,
                        },
                        Shredding {
                            entry_end_idx: 65,
                            shred_end_idx: 54,
                        },
                        Shredding {
                            entry_end_idx: 66,
                            shred_end_idx: 55,
                        },
                        Shredding {
                            entry_end_idx: 67,
                            shred_end_idx: 56,
                        },
                        Shredding {
                            entry_end_idx: 68,
                            shred_end_idx: 57,
                        },
                        Shredding {
                            entry_end_idx: 69,
                            shred_end_idx: 58,
                        },
                        Shredding {
                            entry_end_idx: 70,
                            shred_end_idx: 59,
                        },
                        Shredding {
                            entry_end_idx: 71,
                            shred_end_idx: 60,
                        },
                        Shredding {
                            entry_end_idx: 72,
                            shred_end_idx: 61,
                        },
                        Shredding {
                            entry_end_idx: 73,
                            shred_end_idx: 62,
                        },
                        Shredding {
                            entry_end_idx: 74,
                            shred_end_idx: 63,
                        },
                        Shredding {
                            entry_end_idx: 75,
                            shred_end_idx: 64,
                        },
                        Shredding {
                            entry_end_idx: 76,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 77,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 78,
                            shred_end_idx: 65,
                        },
                        Shredding {
                            entry_end_idx: 79,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 80,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 81,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 82,
                            shred_end_idx: 67,
                        },
                        Shredding {
                            entry_end_idx: 83,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 84,
                            shred_end_idx: 68,
                        },
                        Shredding {
                            entry_end_idx: 85,
                            shred_end_idx: 69,
                        },
                        Shredding {
                            entry_end_idx: 86,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 87,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 88,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 89,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 90,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 91,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 92,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 93,
                            shred_end_idx: 72,
                        },
                        Shredding {
                            entry_end_idx: 94,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 95,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 96,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 97,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 98,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 99,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 100,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 101,
                            shred_end_idx: 75,
                        },
                        Shredding {
                            entry_end_idx: 102,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 103,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 104,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 105,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 106,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 107,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 108,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 109,
                            shred_end_idx: 78,
                        },
                        Shredding {
                            entry_end_idx: 110,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 111,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 112,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 113,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 114,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 115,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 116,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 117,
                            shred_end_idx: 81,
                        },
                        Shredding {
                            entry_end_idx: 118,
                            shred_end_idx: -1,
                        },
                        Shredding {
                            entry_end_idx: 119,
                            shred_end_idx: 82,
                        },
                        Shredding {
                            entry_end_idx: 120,
                            shred_end_idx: 83,
                        },
                        Shredding {
                            entry_end_idx: 121,
                            shred_end_idx: 84,
                        },
                        Shredding {
                            entry_end_idx: 122,
                            shred_end_idx: 85,
                        },
                        Shredding {
                            entry_end_idx: 123,
                            shred_end_idx: 86,
                        },
                        Shredding {
                            entry_end_idx: 124,
                            shred_end_idx: 87,
                        },
                    ],
                    entries: decode_cids([
                        "bafyreifiljqbzjqmjnguctohiqmkm6ogmg7prx5pq75qfyl6yjvahqv6nm",
                        "bafyreid5fj253ix2bh4se75pgmiiraikkhz5dje5et6il7v5zcouveehtq",
                        "bafyreifmanxdsdxuz4g7qoci3ipnpzdxrqqo2ka2lujxuhxvti4tfpwqsa",
                        "bafyreigu4lcxy6q7sqfo3dvtgssyfbtv7ebfip5rixsgscs5xkxgkohdqm",
                        "bafyreifojc6akwnbam4qwhng2eknybwghuw6bchefcppb77dwk4q3x634u",
                        "bafyreiasermlv2oxhwbqnrstx2xfzcxwegxcdbb257ktwxbxp2y6drluie",
                        "bafyreif42fy5celttghcmu56jcxlqszxeienyeq4n55n2gblm5baelmjty",
                        "bafyreigrenu44ksbapi6xjvtmuv44bg3r6qoezfdte6swd24pj7bovb6jy",
                        "bafyreicjkcbgelriertgy7oxvluqly4azh5mg5f6ww32rdt5nsfnjw5tp4",
                        "bafyreic76qdhz7mkzxgakbf4vilw2jfox5i6n7cenn4sa3crz6567musya",
                        "bafyreieqiqjdxxzbzjttqy6fjv7rpucfnfc7o2pzcnd77tmdvfubyy6fwi",
                        "bafyreiey55hgvo6suxbtmape4354zh3ut24ng6vbdcznbcaaa66chrfefe",
                        "bafyreifb5ai37qgivkxe4jkhxsp7yx7jd532p3j4nyj2lxycl62bdcqfbe",
                        "bafyreifmjvfvhrklrccfqjv73qg7hkzbzoxlp2xlb7rb6w6wggkwk2w7qq",
                        "bafyreig4fqcbll2ebnuolsygy3c2mmhr2xlegeynza2cnfmivikjznf3dq",
                        "bafyreihdywyjsgcvfpbri3lfva7z5obvkqkjt5dsd5d6hn7gzlmryety2m",
                        "bafyreibeckhw2tlg4kq3oyw2hzbscf5zq4oacnxcsrqk2g6kpctol6zzku",
                        "bafyreibl2c2zxyp6iopopqjlcvud6ydrkjaojqinwfxpeksfny5fa7cgxq",
                        "bafyreihokydrp2jcc3aimok2gusry52cq3e3qqzo4b3qa3242bgcnalj2a",
                        "bafyreifuutenglldkeutff5wkywqw5fno2gpbcqhjoqk4jxkermvrzaomq",
                        "bafyreictdfxbjxcsq7e7edykxbhegzghpymmqcahwbtmfc6eers47vmahe",
                        "bafyreieiixahisdxfuv7jjtde42i2icaqdfee426im5doqe33iwwyv56ey",
                        "bafyreieudhjjtau2yfmuif3m5oezhexmiy62ys3iqayp5jqgvci6ruow5m",
                        "bafyreih7sw76c3vkql65w2p75ewjdrrxe7d2dijhhzgg5lrfkwbxrnqzca",
                        "bafyreifaks6dammgavp6dki4polw4w5llrql2eyrsjdckn4tz3h4f6zmoq",
                        "bafyreicstk6hmyi5kc5ie6y4wnwywugynfmlsmoq7vo56mwtb3qaujjdgi",
                        "bafyreibc4jevwag7c6c2csnow22puyupc3wpxcvtx2z257qagstuep6zmy",
                        "bafyreiaskan2ozpzmzrckxrhytafeh4znl6jqowny4kvscyoxsb42jxdkq",
                        "bafyreicvdwe77soazqw4kbg6exppzaoqhc2bs7lefrehibkcqlhqtbokl4",
                        "bafyreibm3vxjws7o6qnpklwiqu6uyl2eg6zvprnotud7wna4tqgfkmhnky",
                        "bafyreiglcixc7me3u355wcr7xvet75cgw2lk7wtui22lka4fybt4gpjuu4",
                        "bafyreib6fbzrirpp4gtbz2njetn4ihobeehvioatjdoay66cwvz4ug6vsu",
                        "bafyreig3v3ltmw52wsjusfu7y3ue5fesdvef5567rjwsy67elvzh7whndm",
                        "bafyreihb4mzev7bffmx52fykilxxrukkilyj5o2fwxa5ldmqofp453amby",
                        "bafyreicdrxhde7faynw2j3z3na3xqj32pwjmx43kw4hbobuwg5rraxhlpm",
                        "bafyreidmxw2qnw54uhsnsxx2wxrgst2l2fqmi25qlaitfjvjq5jd3mgnmy",
                        "bafyreicdiu5g4h3zx2sntwsud75l3noumbbrnwq3roo7337ryenzqr4bz4",
                        "bafyreibchdff7t4moiydb2mmw5itdeglppph5cvqjeolk53h6xijdthune",
                        "bafyreicwau6cnrem6au7socqnitzgeiszbrgpou3vnlti4awpdsy233j3y",
                        "bafyreidzytt6fjk5huolhq4c4rpzhoc6iyke6uowl26hunivy42n3iirfe",
                        "bafyreieshws6njwhxs4xrh5z3peu6i3ze3fsibhrdids7tvaiswndwry54",
                        "bafyreihuyyultpgd47sxct4227j65mci2rv72hlitn2x7rykniy4tcfabu",
                        "bafyreie6zfensf6rkrfgow3yryvvciost5brfowfnefc6joxmrloxcq25e",
                        "bafyreiftigxyi3mrn5yar3wprze6m56wlrxfedjm5rugmy2abdfebt3cq4",
                        "bafyreidz4jojjn62jfcaghklu4hl27blcla2jia3x5gqaaajdsroeexdyi",
                        "bafyreiazfg363okrecgvgm4cvitn3idizxegvsjpcnyntiafgiepvv3rjq",
                        "bafyreicfu5yjqdbbutuzpqlugkgq4lpp3bhobncng7gih7nqe3atyv5bni",
                        "bafyreie6bbwqlxinxzlz7zb2nunfaxaa7erbf2b4h5q5iyijk5wtjhfgpu",
                        "bafyreiarqfxo6czw27n7t67iugsooejszef7gibkr6bqqxap6xnos2vbhe",
                        "bafyreiawa4zrtqwiqausm65gg4yppz6csxyzhb44r6tdaaiz6xrwl66pae",
                        "bafyreievclildglrrtntpnvfbb4shwhdocdz7dvz23u4zvpe5ogjwnkhru",
                        "bafyreibaeusx7l6zmuny6l6afdp6ra4mrlyd3fwsybfhirhliq5d2fuzvy",
                        "bafyreidk6ptl3erb6c3sdf4s4hvdxobfedi2quguwzufgsyazkwhfknohe",
                        "bafyreifehmusapw2w2nk2xaecjkeit3gr5qdlk4ufxm6tj7dqa3u7orufe",
                        "bafyreifhuo5a2ujr4evyelm5d7uqllpwgkapetizuuxyamum52pr3zl5by",
                        "bafyreibyuelhubwcuy7vpokpij6habktgajgzcfip63co2jjijejg6hvgi",
                        "bafyreiaqj52c23wqoixfp2a3vqm6dstqbxpycopf724tr4slwfufvkquum",
                        "bafyreidwfx3d56yamiaf4ssstwjdehhwlynweynmwpehu4qa57rqbwsvrq",
                        "bafyreiauum3deq2xorhhiy4k5dvxr6r3k4ys6xaagvdhk2qjhjpkelsjz4",
                        "bafyreie5332arsol4momqndok45w6snx3oeoo6wzml4bcn24bxawaiko3u",
                        "bafyreicnnf7zck24gm7jempqht3uncja5icilfhqisl5rvs7klrkmkqs4m",
                        "bafyreiadayjxbartqnxjo47p44ggh2wxxkioo4or4rjve3svimkkdr3574",
                        "bafyreihklkk52zanejb5ssbl3t56tido56on4ytsvcxeaa4fmtpqromcpe",
                        "bafyreiet667slq5cq7da7cre7bpzxsr2gsw5kvuw3egga4w7a3xmaoezlm",
                        "bafyreicawncjw7bzffmau6pmibwx5xfhzej5cql6qu5cenvcr74v4oygqq",
                        "bafyreiga4tmlbgkrqne7spsxroqx26kgvs5g3nak3kb2klzdbeb6qygdcu",
                        "bafyreif3ddjrylksvyzroy56jbpzat4ovjkyphlczxl3hjo2brgltgephm",
                        "bafyreiekfacnfioqwjfsov2ifn3e32j7nsknt4edijfw2n57lqxhcxumzy",
                        "bafyreicuitfdnh33cx4ltszmyz6xhhxhiwg6ywmgb5wjhintjfhvitsvgy",
                        "bafyreiepty6npj5ididzpn5gucuuxkqdfnpz7eb4wwbalp4dudh7vbufvy",
                        "bafyreib2jom4k3vs25k7cfaaj3fgrrryqglhigvctwqml3ul2apk5sevai",
                        "bafyreibrpxjm3vgx2t7hlvcz7defxufusq4jfj2a7bqal5sbadwxba7wja",
                        "bafyreibkwt7oguxwydna4c3if3se3nm744issur5we5wor3qh76qvdpc7u",
                        "bafyreiey7ijx5pvtpwt6ktuf2ssypa46ne7mrpvceae3afwsawhzrdrvf4",
                        "bafyreiaiaiomhgbunuhshqypv2geczaqmlqb5tpklkkdxbpdeateqmt7hi",
                        "bafyreihd5lqtvp5f2wlwehhftbpozmoixdjjh6mvppy542gv2lmvyzl4iq",
                        "bafyreibow6jmmzaobsgcsfx3q2vqdv53a6yhps2fly4icjnsviguov6ghm",
                        "bafyreib46obqwpz4otyco66xxk5q2j26x7a3inttymguk254slcnl4sqdq",
                        "bafyreihih4ky4cg6eix4f7zcdkwnxogxeazo2qxboaybxv772kxtmco34a",
                        "bafyreig7nbkcpovq74fzhkn3jzrmyy4dyokr63jdp2mse62oflpxlqllhq",
                        "bafyreiegloc3kc53pwo2c76uie4wgnujvdjeo722sssbkr4ldzlv4i6d7e",
                        "bafyreihf4e2styswr4k6h54rkde7rtupjipdf4girmwrspbkrlrxb4r2eq",
                        "bafyreieiuwo3wenff7fnzyamjvu4eqmpnv2ud43tz4hxu7bnvmuctdxtxu",
                        "bafyreiewdm2tvmdsudmttrluo5xzdhkqmx46vsr2cbsmzmzjzc63g5wrhm",
                        "bafyreidgvrvincxxd54pllyxd2dsqu5wat5o5ypna5xuujpeyd7pfuzkia",
                        "bafyreianstjrzjcxfsbbmeooex4qxd2g3uauq3pegcy2owsggmrqgea52q",
                        "bafyreiao3h2earfom7wt45nvz6s2e2vmcayvobg6z7qtmmenaaqrqawp7y",
                        "bafyreibypmb2uvqusaysdyw55eu3wodaptvngemulqjtpg43noomalyviq",
                        "bafyreieeru7iivygphk6go264i4avp2llbi7oqns6hlesjzgdg5cev4pmq",
                        "bafyreieyd36xubv24f4xhbdmlwbj7gfepo6wyurxczmpekegea3xvtg4fq",
                        "bafyreif25ezww4oa4oth7nsh3jdetgty7pm7kfltvvz4k4f3wh2ej2x2ya",
                        "bafyreig5y5b5bh3ekyvxleydetcwoucsfxbdwuluqzq3fl2uzxh2t7vse4",
                        "bafyreid4hddedajmb4asm5iivhfejwbrejlntyxfeqidvbn27xxrefs2qe",
                        "bafyreicep45ei3wljlpgarc6bddq5an65dohkdeorqybpma3rnytu6ob3a",
                        "bafyreihwaokljqhtsnzjwnpflqwftta6nhuffg6nwvb6qowdlh7rqqsdde",
                        "bafyreibbxmaxnktgimm2ufo733rymdhb7o3ybcr774wfpwambtt2bqofzq",
                        "bafyreicpoqsi7pufmdkwmhuv6u2x6xl7tjmo22n2ikulcy3owgfll6ow3e",
                        "bafyreiah32jko74ke3u4uyl7lqpor2j5ojf77efcn373a2cydmb77twhli",
                        "bafyreiguz2qrafdicvnkgxn5t2gctknn5bj4yjd2aillzvokrmd77rqq3u",
                        "bafyreifoeamlyg732lpdj2pgx6lwhtclpn6r2xeo44w36btbstbd4urwfa",
                        "bafyreigap6tcbeskvdumjtqvogicprwyu4ybtg6dm4gy2emqlvmedodii4",
                        "bafyreiavv6js34ke4lnqql3jf2wxqidzjsahzdjfawp37tipesbadvxmyq",
                        "bafyreicmwi3hudesp7ayhhzw6xdx6gmf4mw4zx7lhhatlrdna7g4k4vyje",
                        "bafyreifdmon4sbqke2c5se6ipnaufasnlgd27dzy25w6ag3rgzknagxk3e",
                        "bafyreicci5refhwxebwttxh66doktpw4dxsqsjy65ig6lznhpd4s3pqd6i",
                        "bafyreie4hbwjfp7r2bvflhjlnqias4udqnlxbwgt33kw7dig6fg25gkl2y",
                        "bafyreiccf65vboj6v24mrn2qsht2embqaeoybi6ocpriy4ovjg2tyrjte4",
                        "bafyreickkywuvs2uqrm4aadyvcnzfz6da4nyto4435o24bopcbo762t6z4",
                        "bafyreid2zvbnrfnruo2cxsb3z7dc7lw7ytanhzyvwdwsb6t7fig5kcda6i",
                        "bafyreif4q32st7gdf5xqi345fho5wymi5kzzsfolnychk6ixs66og7nu7a",
                        "bafyreiamsjbyozsgu6yyzef45qcfnkg4i3lbpnqbkxjvsud7vco6gpfyqm",
                        "bafyreifov4yq252z4v5smkmxcb6lhqgt4t3mow2grnmw75wyg72o3fn6ga",
                        "bafyreif2xuc5qekeqw3f4idcezag4e22cqkvtk47ufpiw27e4vk7j2uj34",
                        "bafyreihkbnibxxelcvk5reuznelx2e3jnw6q533crmhaci3ujfuzf3bxvy",
                        "bafyreib4pdbtba6xutuw32cg672t5mxpetfhvbzrk6psyw7prtzhbpn6ra",
                        "bafyreicfqki5pavot5ydiecpyy4dgiy43mma2oelv5gain7dikbsekfzsu",
                        "bafyreic7q4xxtfbdrm4l27f2n764q6ywgiwuipwyuvs3ro57ng2yzefcbq",
                        "bafyreicw4on3q4eaymiqecqx4olu7yy2dcqqrx2z3jmnjrlteij2qmltj4",
                        "bafyreih4ub2ez5z5bx637zl44tgodq7z5g2mjxnjkb7es7kf7ubmi5ecvm",
                        "bafyreiheojy6blqd56jhvfdlizyfjtsd4fqci62mg77g5imopfiw3ftwey",
                        "bafyreieourchnj6gt3ygyd4amymy33wgbs7dp7pyfhos2cxoiucj3jmpwe",
                        "bafyreifagcbre5euv7ek5vcljs3t3bpggy2zz3yojmbj5b3nj66zfatmhi",
                        "bafyreienbwa5tyexahewmdxbs2mtdkk5okenr2odyzadaepcddbfqyd5uu",
                        "bafyreif6n43knx5mjdp6csx6j6cf2cvhormypuwj65lzne5zkoj3oewwii",
                        "bafyreigseklkvuift4ntldvgbw2njqedaf5rrp6smzz2evcumjm3kkfv3q",
                    ]),
                    meta: SlotMeta {
                        parent_slot: 16848010,
                        blocktime: 0,
                        block_height: None,
                    },
                    rewards: decode_cid("bafkqaaa"),
                },
            ),
        ] {
            let node = Block::try_from(bytes.as_ref()).expect("valid node");
            assert_eq!(node, frame);
        }
    }
}
