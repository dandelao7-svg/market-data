-- ============================================================
--  market_snapshot — weekly snapshots computed from live MLS listings
--  Unlike the Redfin table (which stored Redfin's pre-aggregated
--  periods), here we pull ACTIVE listings from Spark each week and
--  compute the numbers ourselves. One row per region per run = a
--  time series you can trend and feed the gauge ("now vs last month").
-- ============================================================

CREATE TABLE IF NOT EXISTS market_snapshot (
  id                    BIGSERIAL PRIMARY KEY,

  region_type           TEXT NOT NULL,                 -- 'city' or 'zip'
  region_id             TEXT NOT NULL,                 -- 'Santa Clarita, CA' or '90232'
  property_type         TEXT NOT NULL DEFAULT 'A',     -- Spark code: A = residential
  snapshot_date         DATE NOT NULL,                 -- the run date (weekly)

  active_inventory      INTEGER,                       -- # of active listings
  median_list_price     NUMERIC,
  median_ppsf           NUMERIC,                       -- $/sqft
  median_dom            NUMERIC,                       -- days on market
  avg_dom               NUMERIC,
  new_listings_7d       INTEGER,                       -- listed in the last 7 days
  price_decreased_cnt   INTEGER,                       -- listings now below original price
  price_decreased_pct   NUMERIC,                       -- % of active inventory

  ingested_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (region_type, region_id, property_type, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_snapshot_lookup
  ON market_snapshot (region_type, region_id, snapshot_date DESC);
