# WORKFLOW.md — как работать с Claude Code над этим проектом

## Структура рабочей директории

```
solana-harpoon/
├── CLAUDE.md               # ← Claude Code читает автоматически
├── ARCHITECTURE.md          # ← ссылка из CLAUDE.md
├── legacy/                  # ← старый код как референс
│   ├── solana-harpoon/      # содержимое solana-harpoon.zip
│   ├── scripts/             # содержимое scripts.zip
│   └── solana_parser/       # содержимое solana_parser.zip
├── idls/
│   └── pumpfun.json         # IDL файл Pump.fun (скачай с chain или Anchor)
├── crates/                  # ← новый код, создается по шагам
│   ├── harpoon-car/
│   ├── harpoon-solana/
│   ├── harpoon-decode/
│   ├── harpoon-export/
│   └── harpoon-cli/
├── Cargo.toml               # workspace
└── tests/
```

## Начальная настройка

```bash
# 1. Создай директорию
mkdir solana-harpoon && cd solana-harpoon

# 2. Положи файлы
#    - CLAUDE.md и ARCHITECTURE.md в корень
#    - Старый код в legacy/
mkdir -p legacy
# распакуй три zip в legacy/

# 3. Создай workspace Cargo.toml
#    (попроси Claude Code сделать это первым делом)

# 4. Запусти Claude Code
claude
```

## Порядок работы (один крейт за сессию)

### Сессия 1: harpoon-car

```
> Создай крейт harpoon-car. Мигрируй из legacy/solana-harpoon/src/ файлы:
  node.rs, node/*.rs, varint.rs, util.rs. Адаптируй под отдельный крейт
  (pub exports, собственный Cargo.toml с минимальными зависимостями).
  Добавь поддержку mmap через memmap2.
  Все существующие тесты должны проходить.
```

Проверь: `cargo test -p harpoon-car`
Закоммить.

### Сессия 2: harpoon-solana

```
> Создай крейт harpoon-solana. Извлеки логику десериализации транзакций
  и метаданных из legacy/solana-harpoon/src/bin/solana_extractor.rs.
  Нужны функции: decode_transaction, decode_metadata, resolve_full_account_keys,
  matches_programs, extract_instructions. Смотри ARCHITECTURE.md секцию
  "Migration checklist" для маппинга старый код → новый.
```

Проверь: `cargo test -p harpoon-solana`
Закоммить.

### Сессия 3: harpoon-decode

```
> Создай крейт harpoon-decode. Это generic Anchor IDL decoder.
  Загружает IDL JSON, строит HashMap дискриминаторов, десериализует borsh
  в serde_json::Value. Смотри ARCHITECTURE.md секцию "Anchor IDL decoder design".
  Для тестирования используй IDL из idls/pumpfun.json.
  Сравни выход с результатами старого Node.js декодера.
```

Проверь: `cargo test -p harpoon-decode`
Закоммить.

### Сессия 4: harpoon-export

```
> Создай крейт harpoon-export. Мигрируй Arrow/Parquet логику из
  solana_extractor.rs (build_schema, flush_batch_sync). Добавь поддержку
  CSV и JSON Lines. Поддержи партиционирование по event_name.
```

Проверь: `cargo test -p harpoon-export`
Закоммить.

### Сессия 5: harpoon-cli

```
> Создай крейт harpoon-cli. Собери всё вместе. Subcommands:
  ingest, decode, inspect. Пайплайн через bounded channels:
  reader → rayon → writer. Мигрируй download логику (aria2c),
  progress bars (indicatif), timing stats. Смотри CLAUDE.md секцию
  "CLI subcommands" для полного описания интерфейса.
```

Проверь: `cargo build --release -p harpoon-cli`
Тест на реальном .car файле.
Закоммить.

### Сессия 6: Integration + polish

```
> Добавь integration test: маленький .car файл → полный пайплайн → проверь
  содержимое Parquet. Добавь criterion бенчмарки для CAR parsing и IDL decode.
  Напиши README.md с описанием, примерами использования, бенчмарками.
  Настрой GitHub Actions CI.
```

## Советы по работе с Claude Code

1. **Один крейт за сессию.** Не пытайся сделать всё за раз. Контекст ограничен.

2. **Коммить после каждого крейта.** Claude Code видит git history и понимает что уже сделано.

3. **Тестируй сразу.** После каждого шага: `cargo test`, `cargo clippy`. Фикси сразу, не копи.

4. **Используй legacy/ как референс, не как copy-paste.** Говори Claude:
   "посмотри как это сделано в legacy/..., и адаптируй для нового крейта".

5. **Для harpoon-decode попроси сначала план.** Это самый сложный крейт. Скажи:
   "Опиши план реализации IDL decoder, покажи ключевые типы и API, потом пиши код."

6. **Если контекст кончается** — начни новую сессию. CLAUDE.md подгрузится автоматически,
   а git log покажет что уже сделано.

## Что делать если Claude Code ошибается

- Указывай конкретный файл: "посмотри legacy/solana-harpoon/src/bin/solana_extractor.rs строки 600-700"
- Давай конкретный тест-кейс: "вот hex данные инструкции, ожидаемый decode такой-то"
- Если генерит bloat — скажи: "упрости, убери лишние абстракции"
- Если не компилится — скопируй ошибку компилятора как есть, Claude Code хорошо их чинит

## После завершения (перед open source)

1. Пройди `cargo deny check` — аудит лицензий
2. Убедись что нигде нет упоминаний trading, bot, alpha, strategy
3. Добавь LICENSE (AGPL-3.0 как в текущем Cargo.toml, или Apache-2.0/MIT для большей adoption)
4. Добавь CONTRIBUTING.md
5. Проверь что .gitignore исключает legacy/, data/, *.car
