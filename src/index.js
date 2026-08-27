const COLLECTOR_ID = 2;
const SOURCE_NAME = "YahooD1Worker";
const MAX_RETRIES = 3;

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runCollector(env));
  },
  async fetch(request, env) {
    const result = await runCollector(env);
    return new Response(JSON.stringify(result, null, 2), {
      headers: { "content-type": "application/json" },
    });
  },
};

async function runCollector(env) {
  const securities = await fetchSecurities(env);
  const nowIso = new Date().toISOString().replace("T", " ").slice(0, 19);
  let added = 0, skipped = 0, errors = 0;
  const errorDetails = [];

  for (const { security_id, ticker } of securities) {
    try {
      const price = await fetchPriceWithRetry(ticker);
      const exists = await priceAlreadyExists(env, security_id, nowIso);
      if (exists) { skipped++; continue; }
      await insertPrice(env, security_id, price, nowIso);
      added++;
    } catch (e) {
      errors++;
      errorDetails.push({ ticker, security_id, error: String(e) });
    }
  }

  return { added, skipped, errors, errorDetails, timestamp: nowIso };
}

// --- Neon via Hyperdrive ---
async function fetchSecurities(env) {
  const postgres = (await import("postgres")).default;
  const sql = postgres(env.HYPERDRIVE.connectionString, { max: 1 });

  try {
    const rows = await sql`
      SELECT security_id, ticker FROM security_master
      WHERE collector = ${COLLECTOR_ID} AND ticker IS NOT NULL
    `;
    return rows.map((r) => ({ security_id: r.security_id, ticker: r.ticker }));
  } finally {
    try { await sql.end({ timeout: 5 }); } catch (_) {}
  }
}

// --- Yahoo Finance ---
async function fetchPriceWithRetry(ticker) {
  let lastError;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try { return await fetchYahooPrice(ticker); }
    catch (e) { lastError = e; if (attempt < MAX_RETRIES) await sleep(1000 * attempt); }
  }
  throw lastError;
}

async function fetchYahooPrice(ticker) {
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}?interval=1d&range=1d`;
  const res = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0 (compatible; MunotstadtCollector/1.0)" } });
  if (!res.ok) throw new Error(`Yahoo HTTP ${res.status}`);
  const data = await res.json();
  const price = data?.chart?.result?.[0]?.meta?.regularMarketPrice;
  if (price == null) throw new Error("regularMarketPrice ist null/undefined");
  return Number(price);
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// --- D1 ---
async function priceAlreadyExists(env, securityId, priceDate) {
  const row = await env.DB
    .prepare(`SELECT 1 FROM security_prices WHERE security_id = ? AND price_date = ? AND source = ? LIMIT 1`)
    .bind(securityId, priceDate, SOURCE_NAME)
    .first();
  return !!row;
}

async function insertPrice(env, securityId, price, priceDate) {
  await env.DB
    .prepare(
      `INSERT INTO security_prices (security_id, price, price_adjusted, price_date, source, created_at, modified_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`
    )
    .bind(securityId, price, price, priceDate, SOURCE_NAME, priceDate, priceDate)
    .run();
}
