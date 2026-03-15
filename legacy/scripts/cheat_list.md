# Pipeline Commands Cheat Sheet

## 0) Full pipeline (default)

```bash
python scripts/run_pipeline.py --from-epoch 916 --to-epoch 920
```

## 1) Full pipeline + force overwrite

```bash
python scripts/run_pipeline.py --from-epoch 916 --to-epoch 920 --force
```

## 2) Run only decode

```bash
python scripts/run_pipeline.py --from-epoch 916 --to-epoch 920 --steps decode
```

## 3) Run only prepare + flatten

```bash
python scripts/run_pipeline.py --from-epoch 916 --to-epoch 920 --steps prepare,flatten
```

## 4) Full pipeline with tuned decode params

```bash
python scripts/run_pipeline.py \
  --from-epoch 916 --to-epoch 920 \
  --workers 15 \
  --batch-items 5000 \
  --write-chunk 300000 \
  --inflight-mult 2
```

## 5) Full pipeline with custom paths

```bash
python scripts/run_pipeline.py \
  --from-epoch 916 --to-epoch 920 \
  --raw-dir data/raw \
  --processed-dir data/processed \
  --flat-dir data/flat \
  --decoded-dir data/decoded \
  --idl-path decoder/6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P.json \
  --worker-path decoder/decode_worker.mjs
```

## 6) Dry-run (print commands, do not execute)

```bash
python scripts/run_pipeline.py --from-epoch 916 --to-epoch 920 --dry-run
```

## 7) Continue even if step failed

```bash
python scripts/run_pipeline.py --from-epoch 916 --to-epoch 920 --no-stop-on-error
```

## 8) Use specific Python binary

```bash
python scripts/run_pipeline.py \
  --python-bin /Users/ivanagafoshin/Code/jupyter-lab/solana_car/venv/bin/python \
  --from-epoch 916 --to-epoch 920
```

## 9) Decode + save accounts mapping

```bash
python scripts/run_pipeline.py \
  --from-epoch 916 --to-epoch 920 \
  --steps decode \
  --save-accounts-mapping
```

⸻

# Individual scripts (if needed)

## A) Prepare only

```bash
python scripts/00_prepare_epochs.py --epoch-start 916 --epoch-end 920
```

## B) Prepare only + force

```
python scripts/00_prepare_epochs.py --epoch-start 916 --epoch-end 920 --force
```

## C) Flatten only

```bash
python scripts/01_flatten_pump_ix.py
```

## D) Flatten only + force

```bash
python scripts/01_flatten_pump_ix.py --force
```

## E) Decode only

```bash
python scripts/02_decode_fast.py
```

## F) Decode only + force

```bash
python scripts/02_decode_fast.py --force
```

## G) Decode only with tuning

```bash
python scripts/02_decode_fast.py \
  --workers 15 \
  --batch-items 5000 \
  --write-chunk 300000 \
  --inflight-mult 2
```
