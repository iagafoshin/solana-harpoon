//! Парсер транзакций Solana через Old Faithful gRPC.
//!
//! Подключается к локальному faithful-cli, стримит транзакции в диапазоне слотов,
//! находит транзакции, где участвуют кошельки из Parquet-файла (целевые "боты"),
//! и для каждого притока SOL к боту записывает донора (MASTER_DONOR = fee payer, SIDE_DONOR = остальные).
//! Результат — CSV: epoch, slot, role, donor, bot, amount (SOL), programs.

use std::collections::HashSet;
use std::fs::File;
use std::io::{Write, BufWriter};
use std::time::Instant;

use bs58;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use prost::Message;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::VersionedTransaction;
use tonic::Request;

pub mod old_faithful {
    tonic::include_proto!("old_faithful");
}

use old_faithful::old_faithful_client::OldFaithfulClient;
use old_faithful::{StreamTransactionsFilter, StreamTransactionsRequest, TransactionStatusMeta};

/// Кошелёк в коде представлен 32 байтами (как в Solana). HashSet по ним даёт O(1) поиск "этот ключ в списке целей?".
type PubkeyBytes = [u8; 32];

/// Порог изменения баланса в лампортах, чтобы считать движение значимым (шум от комиссий отфильтровываем).
const LAMPORTS_THRESHOLD: i64 = 1_000_000;
/// В одном SOL ровно 10^9 лампортов.
const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

/// Обходит аккаунты транзакции: если аккаунт — один из целевых кошельков (бот) и его баланс
/// вырос больше чем на LAMPORTS_THRESHOLD, ищем контрагента (кого списали) и пишем строку в CSV.
fn process_balances(
    epoch: u64,
    slot: u64,
    meta_bytes: &[u8],
    account_keys: &[Pubkey],
    target_wallets: &HashSet<PubkeyBytes>,
    programs_str: &str,
    writer: &mut BufWriter<File>,
) -> std::io::Result<()> {
    let meta = match TransactionStatusMeta::decode(meta_bytes) {
        Ok(m) => m,
        Err(_) => return Ok(()),
    };
    for (i, key) in account_keys.iter().enumerate() {
        if !target_wallets.contains(&key.to_bytes()) {
            continue;
        }
        if i >= meta.pre_balances.len() || i >= meta.post_balances.len() {
            continue;
        }

        let pre = meta.pre_balances[i] as i64;
        let post = meta.post_balances[i] as i64;
        let delta = post - pre;

        if delta.abs() > LAMPORTS_THRESHOLD {
            let sol_delta = delta as f64 / LAMPORTS_PER_SOL;
            find_counterparty(
                epoch,
                slot,
                &meta,
                account_keys,
                i,
                sol_delta,
                key,
                programs_str,
                writer,
            )?;
        }
    }
    Ok(())
}

/// Для бота, получившего SOL (bot_delta > 0), ищем аккаунт с противоположным изменением баланса
/// (списание примерно на ту же сумму) — это донор. Индекс 0 в транзакции — fee payer (MASTER_DONOR).
fn find_counterparty(
    epoch: u64,
    slot: u64,
    meta: &TransactionStatusMeta,
    keys: &[Pubkey],
    bot_index: usize,
    bot_delta: f64,
    bot_key: &Pubkey,
    programs_str: &str,
    writer: &mut BufWriter<File>,
) -> std::io::Result<()> {
    for (i, key) in keys.iter().enumerate() {
        if i == bot_index {
            continue;
        }

        let delta = (meta.post_balances[i] as i64) - (meta.pre_balances[i] as i64);
        let sol_delta = delta as f64 / LAMPORTS_PER_SOL;

        if bot_delta > 0.0 && sol_delta < -bot_delta {
            let role = if i == 0 {
                "MASTER_DONOR"
            } else {
                "SIDE_DONOR"
            };

            writeln!(
                writer,
                "{},{},{},{},{},{:.6},{}",
                epoch,
                slot,
                role,
                key,
                bot_key,
                bot_delta.abs(),
                programs_str
            )?;
        }
    }
    Ok(())
}

/// Загружает из Parquet первый столбец как base58-публичные ключи и возвращает HashSet для быстрого поиска.
/// Ожидается один столбец с адресами кошельков (наши "целевые боты").
fn load_targets(path: &str) -> Result<HashSet<PubkeyBytes>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let mut set = HashSet::with_capacity(8_500_000);

    println!("Загрузка кошельков в память...");
    for row_result in reader.get_row_iter(None)? {
        let row = row_result?;
        if let Ok(user_str) = row.get_string(0) {
            if let Ok(decoded) = bs58::decode(user_str).into_vec() {
                if let Ok(bytes) = decoded.try_into() {
                    set.insert(bytes);
                }
            }
        }
    }
    println!("Загружено {} кошельков", set.len());
    Ok(set)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!(
            "Использование: {} <start_slot> <end_slot> [epoch] [parquet_path] [output_csv]",
            args.get(0).map(|s| s.as_str()).unwrap_or("old-faithful-rpc")
        );
        eprintln!("  epoch по умолчанию: 0");
        eprintln!("  parquet_path по умолчанию: trade_unique_wallets.parquet");
        eprintln!("  output_csv по умолчанию: found_relations.csv");
        std::process::exit(1);
    }

    let start_slot: u64 = args[1].parse()?;
    let end_slot: u64 = args[2].parse()?;
    let epoch = args.get(3).map(|s| s.parse()).transpose()?.unwrap_or(0);
    let parquet_path = args
        .get(4)
        .map(String::as_str)
        .unwrap_or("trade_unique_wallets.parquet");
    let output_path = args
        .get(5)
        .map(String::as_str)
        .unwrap_or("found_relations.csv");

    let total_slots = end_slot.saturating_sub(start_slot);
    if total_slots == 0 {
        eprintln!("❌ start_slot должен быть меньше end_slot");
        std::process::exit(1);
    }

    let target_wallets = load_targets(parquet_path)?;

    // Подключение к faithful-cli по gRPC (должен быть уже запущен пайплайном).
    let mut client = OldFaithfulClient::connect("http://127.0.0.1:8888").await?;

    // Фильтр: только транзакции, где участвует System Program (11111...).
    // Так отсекаем чисто токенные переводы и оставляем движения нативного SOL.
    let filter = StreamTransactionsFilter {
        account_include: vec!["11111111111111111111111111111111".to_string()],
        ..Default::default()
    };

    let request = Request::new(StreamTransactionsRequest {
        start_slot,
        end_slot: Some(end_slot),
        filter: Some(filter),
    });

    println!(
        "🚀 Эпоха {}: слоты {}..{}, вывод в {}",
        epoch, start_slot, end_slot, output_path
    );

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(output_path)?;

    if file.metadata()?.len() == 0 {
        writeln!(file, "epoch,slot,role,donor,bot,amount,programs")?;
    }

    let mut csv_writer = BufWriter::new(file);

    let mut stream = client.stream_transactions(request).await?.into_inner();

    let mut processed_count = 0u64;
    let mut last_print = Instant::now();

    while let Some(tx_response) = stream.message().await? {
        processed_count += 1;
        let current_slot = tx_response.slot;

        if last_print.elapsed().as_secs() >= 10 {
            let processed_slots = current_slot.saturating_sub(start_slot);
            let progress = (processed_slots as f64 / total_slots as f64) * 100.0;
            println!(
                "⏳ Прогресс: {:.2}% | Слот: {} | Обработано TX: {}",
                progress, current_slot, processed_count
            );
            csv_writer.flush()?;
            last_print = Instant::now();
        }

        if let Some(raw_tx) = tx_response.transaction {
            if let Ok(tx) = bincode::deserialize::<VersionedTransaction>(&raw_tx.transaction) {
                let account_keys = tx.message.static_account_keys();

                let has_bot = account_keys
                    .iter()
                    .any(|key| target_wallets.contains(&key.to_bytes()));

                if has_bot {
                    // Собираем список программ из инструкций (для колонки programs в CSV).
                    let mut programs = Vec::new();
                    for ix in tx.message.instructions() {
                        let prog_idx = ix.program_id_index as usize;
                        if let Some(prog_key) = account_keys.get(prog_idx) {
                            programs.push(prog_key.to_string());
                        }
                    }
                    programs.sort();
                    programs.dedup();
                    let programs_str = programs.join("|");

                    process_balances(
                        epoch,
                        current_slot,
                        &raw_tx.meta,
                        account_keys,
                        &target_wallets,
                        &programs_str,
                        &mut csv_writer,
                    )?;
                }
            }
        }
    }

    csv_writer.flush()?;
    println!(
        "✅ Стрим завершен. Обработано транзакций: {}",
        processed_count
    );
    Ok(())
}
