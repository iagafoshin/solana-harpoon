"""
Экспорт данных из parquet-файлов в bot/data/ для paper trading бота.

Запуск из корня проекта:
    .venv/bin/python scripts/export_bot_data.py

Генерирует:
    bot/config.json
    bot/data/wallet_scores.json
    bot/data/creator_scores.json
    bot/data/cabal_wallets.json
    bot/data/conviction_wallets.json
    bot/data/daily_market.json
"""

import json
from pathlib import Path

import catboost as cb
import numpy as np
import polars as pl

ROOT = Path(__file__).parent.parent
BOT_DIR = ROOT / 'bot'
BOT_DATA_DIR = BOT_DIR / 'data'
DATA_DIR = ROOT / 'data' / 'alpha'

BOT_DATA_DIR.mkdir(parents=True, exist_ok=True)

# --- Параметры стратегии (должны совпадать с notebook 16) ---
TRAIL_STOP = 0.20
TAKE_PROFIT = 2.0
TIMEOUT_MIN = 15
DELAY_SEC = 60
ENTRY_SLIP = 0.02
EXIT_FEE = 0.01
POS_SIZE_SOL = 0.05

# Берём список фич прямо из модели — источник истины
_model = cb.CatBoostClassifier()
_model.load_model(str(BOT_DIR / 'model.cbm'))
FEATURE_COLS = list(_model.feature_names_)
print(f'Feature columns from model: {len(FEATURE_COLS)}')
print(FEATURE_COLS)

# --- 1. Wallet scores из conviction_wallets.parquet ---
print('\n[1] Wallet scores...')
wallets_df = pl.read_parquet(DATA_DIR / 'conviction_wallets.parquet')
wallet_scores = (
    wallets_df
    .select(['user', 'win_rate', 'total_pnl_sol', 'median_roi',
             'median_hold_sec', 'n_tokens_traded'])
    .to_pandas()
    .set_index('user')
    .to_dict('index')
)
with open(BOT_DATA_DIR / 'wallet_scores.json', 'w') as f:
    json.dump(wallet_scores, f)
print(f'  Wallet scores: {len(wallet_scores):,} wallets')

# --- 2. Creator scores из early_signal_dataset ---
print('[2] Creator scores...')
creator_export_cols = ['creator', 'creator_avg_ath_mcap', 'creator_success_rate',
                       'creator_n_tokens_before', 'bot_score',
                       'creator_is_first_time', 'creator_is_serial']
dataset = pl.read_parquet(DATA_DIR / 'features' / 'early_signal_dataset.parquet',
                          columns=['block_time'] + creator_export_cols)
creator_scores = (
    dataset
    .sort('block_time')
    .group_by('creator')
    .last()
    .drop('block_time')
    .to_pandas()
    .set_index('creator')
    .to_dict('index')
)
with open(BOT_DATA_DIR / 'creator_scores.json', 'w') as f:
    json.dump(creator_scores, f)
print(f'  Creator scores: {len(creator_scores):,} creators')

# --- 3. Cabal wallets ---
print('[3] Cabal wallets...')
cabal_wallets = (
    pl.read_parquet(DATA_DIR / 'wallet_cabal_score.parquet')
    .filter(pl.col('is_in_cabal'))['user']
    .to_list()
)
with open(BOT_DATA_DIR / 'cabal_wallets.json', 'w') as f:
    json.dump(cabal_wallets, f)
print(f'  Cabal wallets: {len(cabal_wallets):,}')

# --- 4. Conviction wallets (список user) ---
print('[4] Conviction wallets...')
conviction_wallets = wallets_df['user'].to_list()
with open(BOT_DATA_DIR / 'conviction_wallets.json', 'w') as f:
    json.dump(conviction_wallets, f)
print(f'  Conviction wallets: {len(conviction_wallets):,}')

# --- 5. Daily market baseline ---
print('[5] Daily market metrics...')
daily_market = pl.read_parquet(DATA_DIR / 'daily_market_metrics.parquet')
# Переименовываем колонки в имена, ожидаемые feature_collector.py
col_rename = {
    'n_trades': 'daily_n_trades',
    'total_volume_sol': 'daily_volume_sol',
    'n_active_users': 'daily_n_active_users',
    'buy_ratio': 'daily_buy_ratio',
    'n_active_mints': 'daily_n_new_tokens',
}
daily_market = daily_market.rename({k: v for k, v in col_rename.items() if k in daily_market.columns})
daily_cols = [c for c in ['daily_n_trades', 'daily_volume_sol', 'daily_n_active_users',
                           'daily_buy_ratio', 'daily_n_new_tokens'] if c in daily_market.columns]
last_row = daily_market.sort('date').tail(1).select(daily_cols).to_pandas()
last_day = {k: float(v) for k, v in last_row.iloc[0].to_dict().items()}
with open(BOT_DATA_DIR / 'daily_market.json', 'w') as f:
    json.dump(last_day, f, indent=2)
print(f'  Daily market baseline: {last_day}')

# --- 6. Threshold из predictions Window 3 ---
print('[6] Computing threshold (top 1% from Window 3)...')
preds = pl.read_parquet(DATA_DIR / 'early_signal_predictions.parquet')
proba_w3 = preds.filter(pl.col('window') == 'Window 3')['proba'].to_numpy()
threshold_1pct = float(np.percentile(proba_w3, 99))
print(f'  Threshold (top 1%): {threshold_1pct:.6f}  (n={len(proba_w3):,})')

# --- 7. Config ---
print('[7] Saving config.json...')
config = {
    'feature_cols': FEATURE_COLS,
    'threshold_proba': threshold_1pct,
    'trail_stop': TRAIL_STOP,
    'take_profit': TAKE_PROFIT,
    'timeout_sec': TIMEOUT_MIN * 60,
    'entry_delay_sec': DELAY_SEC,
    'entry_slippage': ENTRY_SLIP,
    'exit_fee': EXIT_FEE,
    'position_size_sol': POS_SIZE_SOL,
    'max_open_positions': 5,
}
with open(BOT_DIR / 'config.json', 'w') as f:
    json.dump(config, f, indent=2)
print(f'  Feature columns: {len(FEATURE_COLS)}')
print(f'  Threshold: {threshold_1pct:.6f}')

print('\nDone! All bot data exported to bot/')
