```bash
cargo run \
  --features=counter \
  --bin pumpfun_dump \
  --release \
  -- \
  --car ~/Desktop/of1/869/epoch-869.car \
  --parse \
  --decode \
  > ~/Desktop/pumpfun-epoch-869.full.ndjson
```