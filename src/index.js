const COLLECTOR_ID = 2;
const SOURCE_NAME = "YahooD1Worker";

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

  const tickers = securities.map((s) => s.ticker);
  const quotes = await fetchYahooQuotesBatch(tickers);

  const toInsert = [];
  const errorDetails = [];

  for (const { security_id, ticker } of securities) {
    const price = quotes.get(ticker);
    if (price == null) {
      errorDetails.push({ ticker, security_id, error: "Kein Kurs von Yahoo erhalten" });
      continue;
    }
    toInsert.push({ security_id, price, priceDate: nowIso });
  }

  const inserted = await insertPricesBatch(env, toInsert);

  return {
    added: inserted,
    skipped: toInsert.length - inserted,
    errors: errorDetails.length,
    errorDetails,
    totalSecurities: securities.length,
    timestamp: nowIso,
  };
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

// --- Yahoo Finance: alle Ticker in EINEM Request ---
async function fetchYahooQuotesBatch(tickers) {
  const symbols = tickers.join(",");
  const url = `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${encodeURIComponent(symbols)}`;
  const res = await fetch(url, {
    headers: { "User-Agent": "Mozilla/5.0 (compatible; MunotstadtCollector/1.0)" },
  });
  if (!res.ok) throw new Error(`Yahoo HTTP ${res.status}`);
  const data = await res.json();
  const results = data?.quoteResponse?.result || [];

  const priceMap = new Map();
  for (const r of results) {
    if (r.symbol && r.regularMarketPrice != null) {
      priceMap.set(r.symbol, Number(r.regularMarketPrice));
    }
  }
  return priceMap;
}

// --- D1: ein einziges Batch-INSERT für alle Preise ---
async function insertPricesBatch(env, priceResults) {
  if (priceResults.length === 0) return 0;

  const placeholders = priceResults.map(() => "(?, ?, ?, ?, ?, ?, ?)").join(", ");
  const values = priceResults.flatMap((r) => [
    r.security_id, r.price, r.price, r.priceDate, SOURCE_NAME, r.priceDate, r.priceDate,
  ]);

  const result = await env.DB
    .prepare(
      `INSERT OR IGNORE INTO security_prices
        (security_id, price, price_adjusted, price_date, source, created_at, modified_at)
       VALUES ${placeholders}`
    )
    .bind(...values)
    .run();

  return result.meta.changes;
}
