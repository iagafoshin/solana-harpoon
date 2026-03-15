import fs from "node:fs";
import bs58 from "bs58";
import * as anchor from "@coral-xyz/anchor";

/**
 * Robust Decoder for Solana Anchor Events.
 * Features:
 * 1. Handles CPI prefixes automatically.
 * 2. Auto-fixes "Version Mismatch" (RangeError) by trying to decode legacy schemas.
 */

function loadIdl(idlPath) {
  return JSON.parse(fs.readFileSync(idlPath, "utf8"));
}

// Глубокое копирование, чтобы не ломать исходный объект при ретраях
function deepClone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

function normalizeIdl(idlRaw) {
  const idl = deepClone(idlRaw);
  const typesByName = new Map((idl.types || []).map((t) => [t.name, t.type]));

  // Fill account types
  for (const acc of idl.accounts || []) {
    if (!("type" in acc)) {
      const accType = typesByName.get(acc.name);
      if (accType) acc.type = accType;
    }
  }

  // Fill event fields from types if missing
  for (const ev of idl.events || []) {
    if (!("fields" in ev)) {
      const evType = typesByName.get(ev.name);
      if (evType && evType.fields) {
        ev.fields = evType.fields;
      }
    }
  }
  return idl;
}

function toHex(buf) {
  return Buffer.from(buf).toString("hex");
}

function isBN(x) {
  return (
    x &&
    typeof x === "object" &&
    typeof x.toString === "function" &&
    x.constructor &&
    x.constructor.name === "BN"
  );
}

function normalize(x) {
  if (x == null) return x;

  // 1. Добавили проверку на PublicKey (чтобы адреса были красивыми строками)
  if (x.toBase58 && typeof x.toBase58 === "function") {
    return x.toBase58();
  }

  // 2. BN -> строка
  if (isBN(x)) return x.toString(10);

  // 3. Buffer -> hex
  if (Buffer.isBuffer(x)) return x.toString("hex");
  if (x instanceof Uint8Array) return Buffer.from(x).toString("hex");

  // 4. Массивы
  if (Array.isArray(x)) return x.map(normalize);

  // 5. Объекты
  if (typeof x === "object") {
    const out = {};
    for (const [k, v] of Object.entries(x)) out[k] = normalize(v);
    const keys = Object.keys(out);
    if (keys.length === 1 && out[keys[0]] === true) {
      return { variant: keys[0] };
    }
    return out;
  }

  return x;
}

function mapAccountsToNames(idl, ixName, accounts) {
  if (!accounts?.length) return null;
  const ix = (idl.instructions || []).find((i) => i.name === ixName);
  if (!ix?.accounts?.length) return null;
  const out = {};
  for (let i = 0; i < Math.min(ix.accounts.length, accounts.length); i++) {
    out[ix.accounts[i].name] = accounts[i];
  }
  return out;
}

// --- CORE LOGIC ---

function findEventDefinition(idl, discBuffer) {
  if (!idl.events) return null;
  const searchDisc = Array.from(discBuffer);
  for (const ev of idl.events) {
    if (ev.discriminator && ev.discriminator.length === 8) {
      if (ev.discriminator.every((byte, i) => byte === searchDisc[i])) {
        return ev;
      }
    }
  }
  return null;
}

/**
 * Пытается декодировать тело ивента.
 * Если падает с ошибкой длины (RangeError), пробует удалить последнее поле из определения
 * (эмуляция старой версии программы) и пробует снова.
 */
function decodeBodyWithRetry(idlRaw, eventName, bodyBuffer) {
  let currentIdl = normalizeIdl(idlRaw);
  
  // Максимум попыток (сколько полей готовы отрезать с конца)
  const MAX_RETRIES = 5; 

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      // Каждый раз создаем новый кодер с (возможно) измененным IDL
      const coder = new anchor.BorshCoder(currentIdl);
      const decodedBody = coder.types.decode(eventName, bodyBuffer);
      
      return {
        success: true,
        data: decodedBody,
        fieldsRemoved: attempt
      };
    } catch (e) {
      const isRangeError = String(e).includes("RangeError") || String(e).includes("ERR_OUT_OF_RANGE");
      
      // Если это не ошибка длины или мы исчерпали попытки -> сдаемся
      if (!isRangeError || attempt === MAX_RETRIES) {
        return { success: false, error: String(e) };
      }

      // --- BACKWARD COMPATIBILITY MAGIC ---
      // Ищем определение типа в IDL и удаляем последнее поле
      const typeDef = currentIdl.types.find(t => t.name === eventName);
      if (typeDef && typeDef.type && Array.isArray(typeDef.type.fields) && typeDef.type.fields.length > 0) {
        // Удаляем последнее поле (например, mayhem_mode)
        typeDef.type.fields.pop();
      } else {
        // Если полей не осталось, стоп
        return { success: false, error: String(e) };
      }
    }
  }
}

function decodeEventManually(idl, rawBuffer) {
  // 1. CPI Check
  const CPI_DISC_HEX = "e445a52e51cb9a1d";
  let isCpi = false;
  let eventBuffer = rawBuffer;

  if (rawBuffer.length >= 16 && toHex(rawBuffer.slice(0, 8)) === CPI_DISC_HEX) {
    isCpi = true;
    eventBuffer = rawBuffer.slice(8);
  }

  // 2. Discriminator
  if (eventBuffer.length < 8) return null;
  const eventDisc = eventBuffer.slice(0, 8);
  
  // 3. Find Definition
  const eventDef = findEventDefinition(idl, eventDisc);
  if (!eventDef) return null;

  // 4. Decode with Retry Logic (Version Mismatch Handler)
  const bodyBuffer = eventBuffer.slice(8);
  const result = decodeBodyWithRetry(idl, eventDef.name, bodyBuffer);

  if (result.success) {
    return {
      kind: isCpi ? "event_cpi" : "event",
      name: eventDef.name,
      data: normalize(result.data),
      discHex: toHex(eventDisc),
      versionMismatchFixed: result.fieldsRemoved > 0 ? `Removed ${result.fieldsRemoved} trailing fields (legacy data)` : false
    };
  } else {
    return {
      kind: "event_decode_error",
      name: eventDef.name,
      error: result.error
    };
  }
}

// --- MAIN ---

function decodeOne(idl, b58data, accounts = null) {
  const raw = Buffer.from(bs58.decode(b58data));
  
  // Для инструкций используем стандартный кодер (там редко меняются хвосты так критично)
  const standardCoder = new anchor.BorshCoder(normalizeIdl(idl));

  // 1. Try Instruction
  try {
    const ix = standardCoder.instruction.decode(raw);
    if (ix) {
      return {
        ok: true,
        kind: "instruction",
        name: ix.name,
        data: normalize(ix.data),
        discHex: toHex(raw.slice(0, 8)),
        accounts: mapAccountsToNames(idl, ix.name, accounts),
      };
    }
  } catch (e) {}

  // 2. Try Event (Smart)
  const ev = decodeEventManually(idl, raw);
  if (ev) {
    return {
      ok: true,
      ...ev
    };
  }

  return {
    ok: false,
    kind: "unknown",
    discHex: toHex(raw.slice(0, 8)),
    totalLen: raw.length,
  };
}

// CLI
if (process.argv.length < 4) {
  console.log("Usage: node decode.mjs <IDL.json> <base58_data> [accounts]");
  process.exit(1);
}

const idlRaw = loadIdl(process.argv[2]);
const b58data = process.argv[3];
const accounts = process.argv[4] ? JSON.parse(process.argv[4]) : null;

const res = decodeOne(idlRaw, b58data, accounts);
console.log(JSON.stringify(res, null, 2));