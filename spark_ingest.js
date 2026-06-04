/* ============================================================
 *  spark_ingest.js — Weekly market snapshot from LIVE MLS listings
 *  ------------------------------------------------------------
 *  This is the Redfin pipeline's successor. Instead of downloading
 *  Redfin's monthly pre-aggregated file, it pulls ACTIVE listings
 *  from your MLS via the Spark API and computes the market numbers
 *  itself — the same approach Altos uses, at the freshness you want.
 *
 *  WHAT YOU CONFIRM WHEN YOUR SPARK KEY ARRIVES (two things, marked
 *  with  >>> CONFIRM  below):
 *    1. SPARK_TOKEN  — the access token from your Datamart welcome email.
 *    2. The PropertyType / status codes your MLS uses (most use
 *       'A' for residential and 'Active' for status, but confirm).
 *
 *  Everything else — the math, the upsert, the trailing logic — is
 *  done and tested. Set the token, set your regions, deploy.
 * ============================================================ */

const https      = require("https");
const { Client } = require("pg");
const fs         = require("fs");
const path       = require("path");

/* ============================================================
 *  CONFIG — the only part you edit
 * ============================================================ */
const CONFIG = {
  databaseUrl: process.env.DATABASE_URL,

  // >>> CONFIRM (1): your Spark access token. Put it in Railway as a
  // variable named SPARK_TOKEN (do NOT paste it into this file).
  sparkToken: process.env.SPARK_TOKEN,

  // Spark production API host (from the docs). Leave as-is unless your
  // welcome email gives a different host.
  apiHost: "sparkapi.com",

  // The cities you farm. Spark matches on the City field exactly,
  // so use the MLS's spelling (usually plain city name, no state).
  trackedCities: ["Santa Clarita", "Calabasas", "Malibu"],

  // The zip codes you farm (Spark field: PostalCode).
  trackedZips: ["90232", "91355", "91301", "90265"],

  // >>> CONFIRM (2): property type + active status codes for your MLS.
  // 'A' = residential and 'Active' are the common defaults.
  propertyType: "A",
  activeStatus: "Active",

  // Don't pull obviously-bad rows (placeholder $1 listings, etc.)
  minListPrice: 10000,

  // Spark page size (max 1000). We paginate automatically.
  pageSize: 1000,
};

// The StandardFields we need for the math. Keeping the list short
// makes each request faster and lighter.
const SELECT_FIELDS = [
  "ListPrice", "OriginalListPrice", "City", "PostalCode",
  "BuildingAreaTotal", "DaysOnMarket", "ListingContractDate",
  "StandardStatus", "PropertyType",
].join(",");

/* ============================================================
 *  Spark API helpers
 * ============================================================ */

// One GET against the Spark API, returns parsed JSON.
function sparkGet(pathAndQuery) {
  return new Promise((resolve, reject) => {
    const req = https.request(
      { host: CONFIG.apiHost, path: pathAndQuery, method: "GET",
        headers: { "Authorization": "OAuth " + CONFIG.sparkToken,
                   "Accept": "application/json" } },
      (res) => {
        let body = "";
        res.on("data", (c) => (body += c));
        res.on("end", () => {
          if (res.statusCode !== 200) {
            return reject(new Error(`Spark HTTP ${res.statusCode}: ${body.slice(0, 300)}`));
          }
          try { resolve(JSON.parse(body)); }
          catch (e) { reject(new Error("Bad JSON from Spark: " + e.message)); }
        });
      }
    );
    req.on("error", reject);
    req.end();
  });
}

// Pull EVERY active listing matching a filter, following pagination.
async function fetchActive(filter) {
  const out = [];
  let page = 1, totalPages = 1;
  do {
    const q =
      `/v1/listings?_filter=${encodeURIComponent(filter)}` +
      `&_select=${encodeURIComponent(SELECT_FIELDS)}` +
      `&_pagination=1&_limit=${CONFIG.pageSize}&_page=${page}`;
    const json = await sparkGet(q);
    const results = (json.D && json.D.Results) || [];
    for (const r of results) out.push(r.StandardFields || {});
    const pg = json.D && json.D.Pagination;
    totalPages = pg ? pg.TotalPages : 1;
    page++;
  } while (page <= totalPages);
  return out;
}

// Build the SparkQL _filter string for one region.
function buildFilter(field, value) {
  return `${field} Eq '${value}' ` +
         `And PropertyType Eq '${CONFIG.propertyType}' ` +
         `And StandardStatus Eq '${CONFIG.activeStatus}' ` +
         `And ListPrice Ge ${CONFIG.minListPrice}`;
}

/* ============================================================
 *  The analytical heart — turn raw listings into market numbers.
 *  (Pure function: fully unit-testable, no network or DB.)
 * ============================================================ */
function median(nums) {
  const a = nums.filter((n) => Number.isFinite(n)).sort((x, y) => x - y);
  if (!a.length) return null;
  const m = Math.floor(a.length / 2);
  return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2;
}
function avg(nums) {
  const a = nums.filter((n) => Number.isFinite(n));
  return a.length ? a.reduce((s, n) => s + n, 0) / a.length : null;
}

function computeSnapshot(listings, asOf) {
  const n = listings.length;
  const prices = listings.map((l) => l.ListPrice);
  const doms   = listings.map((l) => l.DaysOnMarket);
  const ppsf   = listings
    .map((l) => (l.BuildingAreaTotal > 0 ? l.ListPrice / l.BuildingAreaTotal : null))
    .filter(Number.isFinite);

  const weekAgo = new Date(asOf);
  weekAgo.setDate(weekAgo.getDate() - 7);
  const newCnt = listings.filter(
    (l) => l.ListingContractDate && new Date(l.ListingContractDate) >= weekAgo
  ).length;

  const dropCnt = listings.filter(
    (l) => Number.isFinite(l.OriginalListPrice) && l.ListPrice < l.OriginalListPrice
  ).length;

  return {
    active_inventory:    n,
    median_list_price:   median(prices),
    median_ppsf:         ppsf.length ? +median(ppsf).toFixed(2) : null,
    median_dom:          median(doms),
    avg_dom:             avg(doms) === null ? null : +avg(doms).toFixed(1),
    new_listings_7d:     newCnt,
    price_decreased_cnt: dropCnt,
    price_decreased_pct: n ? +((100 * dropCnt) / n).toFixed(1) : null,
  };
}

/* ============================================================
 *  Database
 * ============================================================ */
async function ensureTable(client) {
  const p = path.join(__dirname, "schema_spark.sql");
  if (fs.existsSync(p)) await client.query(fs.readFileSync(p, "utf8"));
}

async function upsertSnapshot(client, row) {
  const cols = [
    "region_type", "region_id", "property_type", "snapshot_date",
    "active_inventory", "median_list_price", "median_ppsf", "median_dom",
    "avg_dom", "new_listings_7d", "price_decreased_cnt", "price_decreased_pct",
  ];
  const vals = cols.map((c) => row[c] ?? null);
  const ph = cols.map((_, i) => `$${i + 1}`).join(",");
  const upd = cols
    .filter((c) => !["region_type","region_id","property_type","snapshot_date"].includes(c))
    .map((c) => `${c}=EXCLUDED.${c}`).concat("ingested_at=now()").join(",");
  await client.query(
    `INSERT INTO market_snapshot (${cols.join(",")}) VALUES (${ph})
     ON CONFLICT (region_type, region_id, property_type, snapshot_date)
     DO UPDATE SET ${upd}`, vals);
}

/* ============================================================
 *  Main
 * ============================================================ */
async function main() {
  if (!CONFIG.databaseUrl) throw new Error("DATABASE_URL is not set.");
  if (!CONFIG.sparkToken)  throw new Error("SPARK_TOKEN is not set. Add it as a Railway variable once your key arrives.");

  const asOf = new Date().toISOString().slice(0, 10);
  const client = new Client({
    connectionString: CONFIG.databaseUrl,
    ssl: { rejectUnauthorized: false },
  });
  await client.connect();

  try {
    await ensureTable(client);
    let wrote = 0;

    for (const city of CONFIG.trackedCities) {
      const listings = await fetchActive(buildFilter("City", city));
      const snap = computeSnapshot(listings, asOf);
      await upsertSnapshot(client, {
        region_type: "city", region_id: `${city}`,
        property_type: CONFIG.propertyType, snapshot_date: asOf, ...snap,
      });
      console.log(`[city] ${city}: ${snap.active_inventory} active, median $${snap.median_list_price}`);
      wrote++;
    }

    for (const zip of CONFIG.trackedZips) {
      const listings = await fetchActive(buildFilter("PostalCode", zip));
      const snap = computeSnapshot(listings, asOf);
      await upsertSnapshot(client, {
        region_type: "zip", region_id: zip,
        property_type: CONFIG.propertyType, snapshot_date: asOf, ...snap,
      });
      console.log(`[zip] ${zip}: ${snap.active_inventory} active, median $${snap.median_list_price}`);
      wrote++;
    }

    console.log(`\n✓ Wrote ${wrote} regional snapshots for ${asOf}.`);
  } finally {
    await client.end();
  }
}

// Only run main() when executed directly — so tests can import the math.
if (require.main === module) {
  main().catch((e) => { console.error("\n✗ Spark ingestion failed:", e.message); process.exit(1); });
}

module.exports = { computeSnapshot, median, avg };
