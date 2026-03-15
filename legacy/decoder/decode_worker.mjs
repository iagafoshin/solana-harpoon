// decoder/decode_worker.mjs
import fs from "node:fs";
import readline from "node:readline";
import bs58 from "bs58";
import * as anchor from "@coral-xyz/anchor";

// ---------------- helpers ----------------
function loadIdl(idlPath) {
  return JSON.parse(fs.readFileSync(idlPath, "utf8"));
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
  if (x.toBase58 && typeof x.toBase58 === "function") return x.toBase58();
  if (isBN(x)) return x.toString(10);
  if (Buffer.isBuffer(x)) return x.toString("hex");
  if (x instanceof Uint8Array) return Buffer.from(x).toString("hex");
  if (Array.isArray(x)) return x.map(normalize);
  if (typeof x === "object") {
    const out = {};
    for (const [k, v] of Object.entries(x)) out[k] = normalize(v);
    const keys = Object.keys(out);
    if (keys.length === 1 && out[keys[0]] === true) return { variant: keys[0] };
    return out;
  }
  return x;
}

// ---------------- normalize IDL once ----------------
// (оставим твою нормализацию типов -> accounts/events)
function deepClone(obj) {
  return JSON.parse(JSON.stringify(obj));
}
function normalizeIdl(idlRaw) {
  const idl = deepClone(idlRaw);
  const typesByName = new Map((idl.types || []).map((t) => [t.name, t.type]));

  for (const acc of idl.accounts || []) {
    if (!("type" in acc)) {
      const accType = typesByName.get(acc.name);
      if (accType) acc.type = accType;
    }
  }
  for (const ev of idl.events || []) {
    if (!("fields" in ev)) {
      const evType = typesByName.get(ev.name);
      if (evType && evType.fields) ev.fields = evType.fields;
    }
  }
  return idl;
}

// ---------------- precomputed maps ----------------
function buildEventDiscMap(idlNorm) {
  const m = new Map();
  for (const ev of idlNorm.events || []) {
    if (ev.discriminator && ev.discriminator.length === 8) {
      const discHex = Buffer.from(ev.discriminator).toString("hex");
      m.set(discHex, ev.name);
    }
  }
  return m;
}
function buildIxDiscMap(idlNorm) {
  const m = new Map();
  for (const ix of idlNorm.instructions || []) {
    if (ix.discriminator && ix.discriminator.length === 8) {
      const discHex = Buffer.from(ix.discriminator).toString("hex");
      m.set(discHex, ix.name);
    }
  }
  return m;
}
function buildIxArgsMap(idlNorm) {
  const m = new Map(); // ixName -> [{name,type}...]
  for (const ix of idlNorm.instructions || []) {
    m.set(
      ix.name,
      Array.isArray(ix.args) ? ix.args.map((a) => ({ ...a })) : [],
    );
  }
  return m;
}
function buildIxAccountsMap(idlNorm) {
  const m = new Map(); // ixName -> [accName...]
  for (const ix of idlNorm.instructions || []) {
    const names = (ix.accounts || []).map((a) => a.name);
    m.set(ix.name, names);
  }
  return m;
}
function buildEventFieldsMap(idlNorm) {
  const m = new Map(); // eventName -> [{name,type}...]
  for (const t of idlNorm.types || []) {
    if (t?.type?.fields && Array.isArray(t.type.fields)) {
      m.set(
        t.name,
        t.type.fields.map((f) => ({ ...f })),
      );
    }
  }
  return m;
}

function mapAccountsToNamesFast(ixAccountsMap, ixName, accounts) {
  if (!accounts?.length) return null;
  const names = ixAccountsMap.get(ixName);
  if (!names?.length) return null;
  const out = {};
  const n = Math.min(names.length, accounts.length);
  for (let i = 0; i < n; i++) out[names[i]] = accounts[i];
  return out;
}

function isRangeErrorMsg(msg) {
  return msg.includes("RangeError") || msg.includes("ERR_OUT_OF_RANGE");
}

// ---------------- coder caches ----------------
// base coder for normal decode
let baseCoder = null;

// for retry: key -> coder
const ixCoderCache = new Map(); // `${ixName}:${removedArgs}` -> coder
const evCoderCache = new Map(); // `${eventName}:${removedFields}` -> coder

function patchIdlForIx(idlNorm, ixName, removedArgs) {
  // копируем только instructions массив (легче, чем deepClone всего)
  const idl = {
    ...idlNorm,
    instructions: (idlNorm.instructions || []).map((ix) => {
      if (ix.name !== ixName) return ix;
      const args = Array.isArray(ix.args)
        ? ix.args.slice(0, Math.max(0, ix.args.length - removedArgs))
        : [];
      return { ...ix, args };
    }),
  };
  return idl;
}

function patchIdlForEvent(idlNorm, eventName, removedFields) {
  // копируем только types массив
  const idl = {
    ...idlNorm,
    types: (idlNorm.types || []).map((t) => {
      if (t.name !== eventName) return t;
      const fields = t?.type?.fields
        ? t.type.fields.slice(
            0,
            Math.max(0, t.type.fields.length - removedFields),
          )
        : [];
      return { ...t, type: { ...t.type, fields } };
    }),
  };
  return idl;
}

function getIxRetryCoder(idlNorm, ixName, removedArgs) {
  const key = `${ixName}:${removedArgs}`;
  const cached = ixCoderCache.get(key);
  if (cached) return cached;
  const patched = patchIdlForIx(idlNorm, ixName, removedArgs);
  const coder = new anchor.BorshCoder(patched);
  ixCoderCache.set(key, coder);
  return coder;
}

function getEvRetryCoder(idlNorm, eventName, removedFields) {
  const key = `${eventName}:${removedFields}`;
  const cached = evCoderCache.get(key);
  if (cached) return cached;
  const patched = patchIdlForEvent(idlNorm, eventName, removedFields);
  const coder = new anchor.BorshCoder(patched);
  evCoderCache.set(key, coder);
  return coder;
}

// ---------------- decode logic ----------------
function decodeInstructionWithRetry(
  idlNorm,
  ixDiscMap,
  ixArgsMap,
  rawBuffer,
  maxRetries = 5,
) {
  // 0) base fast try
  try {
    const ix = baseCoder.instruction.decode(rawBuffer);
    if (ix) return { success: true, data: ix, argsRemoved: 0 };
  } catch (e) {
    const msg = String(e);
    if (!isRangeErrorMsg(msg)) return { success: false, error: msg };
  }

  if (rawBuffer.length < 8) return { success: false, error: "BufferTooShort" };
  const discHex = toHex(rawBuffer.slice(0, 8));
  const ixName = ixDiscMap.get(discHex);
  if (!ixName) return { success: false, error: "UnknownIxDisc" };

  const originalArgs = ixArgsMap.get(ixName) || [];

  for (let removed = 1; removed <= maxRetries; removed++) {
    try {
      const coder = getIxRetryCoder(idlNorm, ixName, removed);
      const decoded = coder.instruction.decode(rawBuffer);
      if (decoded) {
        // backfill missing args with null
        const missingArgs = originalArgs.slice(-removed);
        for (const a of missingArgs) decoded.data[a.name] = null;
        return { success: true, data: decoded, argsRemoved: removed };
      }
    } catch (e2) {
      const msg2 = String(e2);
      if (!isRangeErrorMsg(msg2) || removed === maxRetries) {
        return { success: false, error: msg2 };
      }
    }
  }
  return { success: false, error: "IxRetryFailed" };
}

function decodeEventBodyWithRetry(
  idlNorm,
  eventFieldsMap,
  eventName,
  bodyBuffer,
  maxRetries = 5,
) {
  const originalFields = eventFieldsMap.get(eventName) || [];

  try {
    const decoded = baseCoder.types.decode(eventName, bodyBuffer);
    return { success: true, data: decoded, fieldsRemoved: 0 };
  } catch (e) {
    const msg = String(e);
    if (!isRangeErrorMsg(msg)) return { success: false, error: msg };
  }

  for (let removed = 1; removed <= maxRetries; removed++) {
    try {
      const coder = getEvRetryCoder(idlNorm, eventName, removed);
      const decoded = coder.types.decode(eventName, bodyBuffer);
      const missing = originalFields.slice(-removed);
      for (const f of missing) decoded[f.name] = null;
      return { success: true, data: decoded, fieldsRemoved: removed };
    } catch (e2) {
      const msg2 = String(e2);
      if (!isRangeErrorMsg(msg2) || removed === maxRetries) {
        return { success: false, error: msg2 };
      }
    }
  }
  return { success: false, error: "EvRetryFailed" };
}

function decodeEventManually(idlNorm, eventDiscMap, eventFieldsMap, rawBuffer) {
  const CPI_DISC_HEX = "e445a52e51cb9a1d";
  let isCpi = false;
  let eventBuffer = rawBuffer;

  if (rawBuffer.length >= 16 && toHex(rawBuffer.slice(0, 8)) === CPI_DISC_HEX) {
    isCpi = true;
    eventBuffer = rawBuffer.slice(8);
  }
  if (eventBuffer.length < 8) return null;

  const discHex = toHex(eventBuffer.slice(0, 8));
  const eventName = eventDiscMap.get(discHex);
  if (!eventName) return null;

  const body = eventBuffer.slice(8);
  const res = decodeEventBodyWithRetry(
    idlNorm,
    eventFieldsMap,
    eventName,
    body,
    5,
  );

  if (res.success) {
    return {
      ok: true,
      kind: isCpi ? "event_cpi" : "event",
      name: eventName,
      data: normalize(res.data),
      discHex,
      versionMismatchFixed:
        res.fieldsRemoved > 0 ? `Removed ${res.fieldsRemoved} fields` : false,
    };
  }
  return {
    ok: false,
    kind: "event_decode_error",
    name: eventName,
    error: res.error,
    discHex,
  };
}

function decodeOne(
  idlNorm,
  ixDiscMap,
  eventDiscMap,
  ixArgsMap,
  ixAccountsMap,
  eventFieldsMap,
  b58data,
  accounts = null,
) {
  const raw = Buffer.from(bs58.decode(b58data));

  // 1) instruction
  const ixRes = decodeInstructionWithRetry(
    idlNorm,
    ixDiscMap,
    ixArgsMap,
    raw,
    5,
  );
  if (ixRes.success) {
    return {
      ok: true,
      kind: "instruction",
      name: ixRes.data.name,
      data: normalize(ixRes.data.data),
      discHex: toHex(raw.slice(0, 8)),
      accounts: mapAccountsToNamesFast(
        ixAccountsMap,
        ixRes.data.name,
        accounts,
      ),
      versionMismatchFixed:
        ixRes.argsRemoved > 0
          ? `Removed ${ixRes.argsRemoved} args (legacy instruction)`
          : false,
    };
  }

  // 2) event
  const ev = decodeEventManually(idlNorm, eventDiscMap, eventFieldsMap, raw);
  if (ev) return ev.ok ? ev : { ok: false, ...ev };

  return {
    ok: false,
    kind: "unknown",
    discHex: toHex(raw.slice(0, 8)),
    totalLen: raw.length,
    error: ixRes.error || "Unknown",
  };
}

// ---------------- main ----------------
if (process.argv.length < 3) {
  console.error("Usage: node decode_worker.mjs <IDL.json>");
  process.exit(1);
}

const idlRaw = loadIdl(process.argv[2]);
const idlNorm = normalizeIdl(idlRaw);

baseCoder = new anchor.BorshCoder(idlNorm);

const eventDiscMap = buildEventDiscMap(idlNorm);
const ixDiscMap = buildIxDiscMap(idlNorm);
const ixArgsMap = buildIxArgsMap(idlNorm);
const ixAccountsMap = buildIxAccountsMap(idlNorm);
const eventFieldsMap = buildEventFieldsMap(idlNorm);

const rl = readline.createInterface({
  input: process.stdin,
  crlfDelay: Infinity,
});

for await (const line of rl) {
  const s = line.trim();
  if (!s) continue;

  let msg;
  try {
    msg = JSON.parse(s);
  } catch {
    process.stdout.write(
      JSON.stringify({ id: null, ok: false, error: "bad_json" }) + "\n",
    );
    continue;
  }

  const id = msg.id ?? null;
  const b58 = msg.b58;
  const accounts = msg.accounts ?? null;

  try {
    const res = decodeOne(
      idlNorm,
      ixDiscMap,
      eventDiscMap,
      ixArgsMap,
      ixAccountsMap,
      eventFieldsMap,
      b58,
      accounts,
    );
    process.stdout.write(JSON.stringify({ id, ok: true, res }) + "\n");
  } catch (e) {
    process.stdout.write(
      JSON.stringify({ id, ok: false, error: String(e) }) + "\n",
    );
  }
}
