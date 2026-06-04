/* ============================================================
 *  ingest.js  —  Weekly Redfin market-data pipeline
 *  ------------------------------------------------------------
 *  WHAT IT DOES, in plain English:
 *    1. Downloads Redfin's zip-code and city data files (streamed,
 *       so it never loads the whole multi-GB file into memory).
 *    2. Keeps ONLY the zips and cities you list in CONFIG below.
 *    3. Writes them into your Postgres `market_data` table.
 *    4. Re-running it overwrites recently-revised weeks automatically
 *       (Redfin revises the last ~4 weeks), so the data stays accurate.
 *
 *  Run it weekly with Railway's cron (see the setup notes I gave you).
 *
 *  YOU EDIT ONLY THE CONFIG BLOCK. Everything below it can be left alone.
 * ============================================================ */

const https     = require("https");
const zlib      = require("zlib");
const readline  = require("readline");
const { Client } = require("pg");
const fs        = require("fs");
const path      = require("path");

/* ============================================================
 *  CONFIG  —  this is the only part you need to touch
 * ============================================================ */
const CONFIG = {
  // Your Postgres connection. On Railway this comes from the
  // DATABASE_URL variable automatically — leave this as-is.
  databaseUrl: process.env.DATABASE_URL,

  // Redfin's downloadable region files (gzipped tab-separated).
  // IMPORTANT: confirm these two URLs on Redfin's Downloads page
  // (redfin.com/news/data-center/downloads/) and paste the exact
  // links here if they differ. These are the long-standing locations.
  files: {
    zip:  "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz",
    city: "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz",
  },

  // The zips you farm. Replace these LA-area examples with yours.
  trackedZips: ["90232", "91355", "91301", "90265"],

  // The cities you farm. Format MUST be "City Name, ST".
  trackedCities: ["Santa Clarita, CA", "Calabasas, CA", "Malibu, CA"],

  // Which property type(s) to keep. "All Residential" is the headline
  // number most agents quote. Add others if you want, e.g.
  // "Single Family Residential", "Condo/Co-op", "Townhouse".
  propertyTypes: ["All Residential"],

  // Keep non-seasonally-adjusted rows (the raw numbers). Set to true
  // only if you specifically want seasonally adjusted values.
  seasonallyAdjusted: false,

  // Insert rows to the DB in batches of this size.
  batchSize: 500,
};

/* ============================================================
 *  Column-name helpers
 *  Redfin's headers are lowercase_with_underscores. We map by NAME,
 *  not position, so a column re-order in their file won't break us.
 *  Each metric lists a few possible header spellings just in case.
 * ============================================================ */
const COLS = {
  period_begin:            ["period_begin"],
  period_end:              ["period_end"],
  period_duration:         ["period_duration"],
  region:                  ["region"],
  city:                    ["city"],
  state_code:              ["state_code", "state"],
  property_type:           ["property_type"],
  is_seasonally_adjusted:  ["is_seasonally_adjusted"],
  last_updated:            ["last_updated"],
  median_sale_price:       ["median_sale_price"],
  median_list_price:       ["median_list_price"],
  homes_sold:              ["homes_sold"],
  pending_sales:           ["pending_sales"],
  new_listings:            ["new_listings"],
  inventory:               ["inventory"],
  months_of_supply:        ["months_of_supply"],
  median_dom:              ["median_dom"],
  avg_sale_to_list:        ["avg_sale_to_list"],
  sold_above_list:         ["sold_above_list"],
  price_drops:             ["price_drops"],
  off_market_in_two_weeks: ["off_market_in_two_weeks"],
};

/* ---------- small parsing helpers ---------- */

// Turn a header row into { canonicalName: columnIndex }
function buildHeaderMap(headerFields) {
  const lower = headerFields.map((h) => h.trim().toLowerCase());
  const map = {};
  for (const [canonical, aliases] of Object.entries(COLS)) {
    for (const alias of aliases) {
      const idx = lower.indexOf(alias);
      if (idx !== -1) { map[canonical] = idx; break; }
    }
  }
  return map;
}

// Parse a numeric cell -> Number or null. Strips $ and commas defensively.
function num(v) {
  if (v === undefined || v === null) return null;
  const s = String(v).replace(/[$,]/g, "").trim();
  if (s === "" || s === "NA" || s === "NULL") return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

// Parse a date cell -> 'YYYY-MM-DD' or null.
function dateOrNull(v) {
  if (!v) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

// Pull a 5-digit zip out of Redfin's region label, e.g. "Zip Code: 90232".
function extractZip(regionStr) {
  if (!regionStr) return null;
  const m = String(regionStr).match(/\b(\d{5})\b/);
  return m ? m[1] : null;
}

// Build a "City Name, ST" key from a city row.
function cityKey(get, fields) {
  const city  = get(fields, "city");
  const state = get(fields, "state_code");
  if (city && state) return `${city.trim()}, ${state.trim()}`;
  // Fallback: parse from a region label like "City: Santa Clarita, CA"
  const region = get(fields, "region");
  if (region) {
    const m = String(region).match(/City:\s*(.+?,\s*[A-Z]{2})/i);
    if (m) return m[1].trim();
  }
  return null;
}

/* ============================================================
 *  Core: stream one file, filter to tracked regions, collect rows
 * ============================================================ */
function processFile(regionType, url, isTracked, diag) {
  return new Promise((resolve, reject) => {
    const rows = [];

    https.get(url, (res) => {
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`${regionType}: HTTP ${res.statusCode} from ${url}`));
      }

      const rl = readline.createInterface({
        input: res.pipe(zlib.createGunzip()),
        crlfDelay: Infinity,
      });

      let headerMap = null;
      const get = (fields, name) =>
        headerMap[name] === undefined ? null : fields[headerMap[name]];

      rl.on("line", (line) => {
        const fields = line.split("\t");

        // First line = header. Build the name->index map and print it
        // once so you can eyeball the real column list on first run.
        if (!headerMap) {
          headerMap = buildHeaderMap(fields);
          if (!diag.headerPrinted) {
            console.log(`\n[${regionType}] file header columns:`);
            console.log("  " + fields.map((f) => f.trim()).join(", "));
            diag.headerPrinted = true;
          }
          return;
        }

        // ---- filter: property type ----
        const ptype = (get(fields, "property_type") || "").trim();
        if (!CONFIG.propertyTypes.includes(ptype)) return;

        // ---- filter: seasonal adjustment ----
        const sa = (get(fields, "is_seasonally_adjusted") || "").trim().toLowerCase();
        if (sa === "t" || sa === "true") {
          if (!CONFIG.seasonallyAdjusted) return;
        } else {
          if (CONFIG.seasonallyAdjusted) return;
        }

        // ---- filter: is this one of MY regions? ----
        let regionId, regionName;
        if (regionType === "zip") {
          regionId = extractZip(get(fields, "region"));
          regionName = get(fields, "region");
          if (!regionId || !isTracked(regionId)) return;
        } else {
          regionId = cityKey(get, fields);
          regionName = get(fields, "region");
          if (!regionId || !isTracked(regionId)) return;
        }

        // record which period durations we actually saw (diagnostic)
        const dur = num(get(fields, "period_duration"));
        if (dur !== null) diag.durations.add(dur);

        rows.push({
          region_type:             regionType,
          region_id:               regionId,
          region_name:             regionName,
          state_code:              get(fields, "state_code"),
          property_type:           ptype,
          period_begin:            dateOrNull(get(fields, "period_begin")),
          period_end:              dateOrNull(get(fields, "period_end")),
          period_duration:         dur,
          median_sale_price:       num(get(fields, "median_sale_price")),
          median_list_price:       num(get(fields, "median_list_price")),
          homes_sold:              num(get(fields, "homes_sold")),
          pending_sales:           num(get(fields, "pending_sales")),
          new_listings:            num(get(fields, "new_listings")),
          inventory:               num(get(fields, "inventory")),
          months_of_supply:        num(get(fields, "months_of_supply")),
          median_dom:              num(get(fields, "median_dom")),
          avg_sale_to_list:        num(get(fields, "avg_sale_to_list")),
          sold_above_list:         num(get(fields, "sold_above_list")),
          price_drops:             num(get(fields, "price_drops")),
          off_market_in_two_weeks: num(get(fields, "off_market_in_two_weeks")),
          last_updated:            dateOrNull(get(fields, "last_updated")),
        });
      });

      rl.on("close", () => {
        console.log(`[${regionType}] kept ${rows.length} matching rows`);
        resolve(rows);
      });
      rl.on("error", reject);
    }).on("error", reject);
  });
}

/* ============================================================
 *  Database: ensure table, then upsert rows
 * ============================================================ */
const UPSERT_COLUMNS = [
  "region_type", "region_id", "region_name", "state_code", "property_type",
  "period_begin", "period_end", "period_duration",
  "median_sale_price", "median_list_price", "homes_sold", "pending_sales",
  "new_listings", "inventory", "months_of_supply", "median_dom",
  "avg_sale_to_list", "sold_above_list", "price_drops",
  "off_market_in_two_weeks", "last_updated",
];

async function ensureTable(client) {
  const schemaPath = path.join(__dirname, "schema.sql");
  if (fs.existsSync(schemaPath)) {
    await client.query(fs.readFileSync(schemaPath, "utf8"));
  }
}

async function upsertBatch(client, batch) {
  const cols = UPSERT_COLUMNS;
  const values = [];
  const tuples = batch.map((row, r) => {
    const placeholders = cols.map((_, c) => `$${r * cols.length + c + 1}`);
    cols.forEach((c) => values.push(row[c] ?? null));
    return `(${placeholders.join(",")})`;
  });

  // On conflict (same region + property type + exact period), overwrite
  // the metrics — this is what makes Redfin's late revisions land correctly.
  const updates = cols
    .filter((c) => !["region_type", "region_id", "property_type",
                     "period_begin", "period_end", "period_duration"].includes(c))
    .map((c) => `${c} = EXCLUDED.${c}`)
    .concat("ingested_at = now()")
    .join(", ");

  const sql = `
    INSERT INTO market_data (${cols.join(",")})
    VALUES ${tuples.join(",")}
    ON CONFLICT (region_type, region_id, property_type, period_begin, period_end, period_duration)
    DO UPDATE SET ${updates};
  `;
  await client.query(sql, values);
}

/* ============================================================
 *  Main
 * ============================================================ */
async function main() {
  if (!CONFIG.databaseUrl) {
    throw new Error("DATABASE_URL is not set. On Railway, add it as a variable.");
  }

  const zipSet  = new Set(CONFIG.trackedZips.map((z) => z.trim()));
  const citySet = new Set(CONFIG.trackedCities.map((c) => c.trim()));
  const diag = { headerPrinted: false, durations: new Set() };

  const client = new Client({
    connectionString: CONFIG.databaseUrl,
    ssl: { rejectUnauthorized: false }, // Railway-managed Postgres uses SSL
  });
  await client.connect();

  try {
    await ensureTable(client);

    const zipRows  = await processFile("zip",  CONFIG.files.zip,  (id) => zipSet.has(id),  diag);
    const cityRows = await processFile("city", CONFIG.files.city, (id) => citySet.has(id), diag);
    const all = [...zipRows, ...cityRows];

    if (all.length === 0) {
      console.warn("\n⚠  No rows matched. Check that your zips/cities match Redfin's labels exactly.");
    }

    for (let i = 0; i < all.length; i += CONFIG.batchSize) {
      await upsertBatch(client, all.slice(i, i + CONFIG.batchSize));
    }

    console.log(`\n✓ Upserted ${all.length} rows.`);
    console.log(`  Period durations seen (days): ${[...diag.durations].sort((a, b) => a - b).join(", ") || "none"}`);
    console.log("  (28 ≈ weekly window; ~90 ≈ small-geo monthly. This tells you the real cadence in the file.)");
  } finally {
    await client.end();
  }
}

main().catch((err) => {
  console.error("\n✗ Ingestion failed:", err.message);
  process.exit(1);
});
