-- ============================================================
--  Redfin market-data table
--  Run this ONCE against your Postgres database to create the table.
--  ingest.js also runs it automatically on startup, so this file
--  is mostly here for reference / manual setup.
-- ============================================================

CREATE TABLE IF NOT EXISTS market_data (
  id                      BIGSERIAL PRIMARY KEY,

  region_type             TEXT        NOT NULL,   -- 'zip' or 'city'
  region_id               TEXT        NOT NULL,   -- '90210'  or  'Los Angeles, CA'
  region_name             TEXT,                   -- human-friendly label from the file
  state_code              TEXT,
  property_type           TEXT        NOT NULL,   -- e.g. 'All Residential'

  period_begin            DATE        NOT NULL,
  period_end              DATE        NOT NULL,
  period_duration         INTEGER,                -- days covered (28 = weekly window, ~90 = small-geo monthly)

  -- the numbers agents actually talk about
  median_sale_price       NUMERIC,
  median_list_price       NUMERIC,
  homes_sold              NUMERIC,
  pending_sales           NUMERIC,
  new_listings            NUMERIC,
  inventory               NUMERIC,
  months_of_supply        NUMERIC,
  median_dom              NUMERIC,                -- median days on market
  avg_sale_to_list        NUMERIC,                -- ratio, e.g. 0.99
  sold_above_list         NUMERIC,                -- share 0..1
  price_drops             NUMERIC,                -- share 0..1
  off_market_in_two_weeks NUMERIC,                -- share 0..1

  last_updated            TIMESTAMPTZ,            -- Redfin's own "as of" stamp
  ingested_at             TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- One row per region + property type + exact period.
  -- Re-pulling a recently-revised period overwrites the old values
  -- (see the ON CONFLICT clause in ingest.js).
  UNIQUE (region_type, region_id, property_type, period_begin, period_end, period_duration)
);

-- Fast "latest numbers for this region" lookups for the report layer.
CREATE INDEX IF NOT EXISTS idx_market_data_lookup
  ON market_data (region_type, region_id, period_end DESC);
