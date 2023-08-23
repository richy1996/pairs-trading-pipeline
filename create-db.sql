-- Creating Views and Tables
-- =========================

-- This script creates the table and view database objects you need to run the feature transformation.

-- Create Load Views
-- =================
 
-- We want all our transformations and data processing happening in the `equity_statarb` dataset
-- We don't want a situation where we have processing dependent on stuff in other datasets / projects 
-- without it being obvious. 
-- So in each dataset, we want database views prefiexed with `v_load` - these are the only things that "look out"
-- (so we can know what all our dependencies are)
-- Everything else is dependent only on things in the equity_statarb dataset
 
-- Create views and save them in bigquery
 
-- If you're implementing this yourself, you'll want this stuff to point at your data, not ours.
-- You'll also need to search/replacee the project, dataset to whatever yours is called.
-- For example, change `rw-algotrader.equity_statarb` to `bigbadbob.shitco-spreads` or whatever... 
 
CREATE OR REPLACE VIEW `rw-algotrader.equity_statarb.v_load_dailyprices` AS
SELECT
    ticker, 
    date,
    open,
    high,
    low,
    close,
    unadjusted_close,
    volume
FROM `rw-algotrader.master_quandl.safe_dividend_adjustment`;
 
CREATE OR REPLACE VIEW `rw-algotrader.equity_statarb.v_load_dailysnapshot` AS
SELECT
    ticker,
    date,
    marketcap,
    pb,
    pe,
    ps
FROM `rw-algotrader.master_quandl.DAILY` ;

-- Create a view which has: year and first trading day of year
CREATE OR REPLACE VIEW `rw-algotrader.equity_statarb.v_start_of_year` AS
 
WITH data AS (
  SELECT EXTRACT(year from date) as year, date FROM `rw-algotrader.equity_statarb.v_load_dailysnapshot` 
)
 
SELECT
  year,
  MIN(date) as start_of_year
FROM data
GROUP BY year
ORDER BY year;

-- First create a v_load view within the dataset
CREATE OR REPLACE VIEW `rw-algotrader.equity_statarb.v_load_tickers` AS
SELECT
    ticker,
    name
    category,
    isdelisted,
    sector,
    industry,
    currency,
    location
FROM `rw-algotrader.master_quandl.TICKERS`
WHERE table = 'SEP';


-- Create tables to upsert insert into
-- ===================================

-- liquid_universe
-- ---------------
CREATE OR REPLACE TABLE `rw-algotrader.equity_statarb.liquid_universe` (
  universeyear INTEGER NOT NULL,
  ticker STRING NOT NULL,
  marketcap FLOAT64,
  avg_price FLOAT64,
  avg_volume FLOAT64,
  insert_process_uuid STRING
)
CLUSTER BY universeyear, ticker;


-- pairs_universe
-- --------------
CREATE OR REPLACE TABLE `rw-algotrader.equity_statarb.pairs_universe` (
  universeyear INTEGER NOT NULL,
  stock1 STRING NOT NULL,
  stock2 STRING NOT NULL,
  isSameSector INTEGER,
  isSameIndustry INTEGER,
  isDelisted INTEGER,
  insert_process_uuid STRING
)
CLUSTER BY universeyear, stock1, stock2;


-- spreads
-- --------
CREATE OR REPLACE TABLE `rw-algotrader.equity_statarb.spreads` (
  date DATE NOT NULL,
  stock1 STRING NOT NULL,
  stock2 STRING NOT NULL,
  close1 FLOAT64,
  close2 FLOAT64,
  ratiospread FLOAT64,
  spreadclose FLOAT64,
  sma20 FLOAT64,
  sd20 FLOAT64,
  zscore FLOAT64,
  insert_process_uuid STRING
)
PARTITION BY date
CLUSTER BY date, stock1, stock2;

-- lsr_feature
-- -----------
CREATE OR REPLACE TABLE `rw-algotrader.equity_statarb.lsr_feature` (
  startofmonth DATE NOT NULL,
  stock1 STRING NOT NULL,
  stock2 STRING NOT NULL,
  lsr FLOAT64,
  insert_process_uuid STRING
)
PARTITION BY startofmonth
CLUSTER BY startofmonth, stock1, stock2;


-- ed_feature
-- ----------
CREATE OR REPLACE TABLE `rw-algotrader.equity_statarb.ed_feature` (
  startofmonth DATE NOT NULL,
  stock1 STRING NOT NULL,
  stock2 STRING NOT NULL,
  ed FLOAT64,
  insert_process_uuid STRING
)
PARTITION BY startofmonth
CLUSTER BY startofmonth, stock1, stock2;

-- combined yearly features
CREATE OR REPLACE TABLE `rw-algotrader.equity_statarb.yearly_scores` (
  year INTEGER NOT NULL,
  stock1 STRING NOT NULL,
  stock2 STRING NOT NULL,
  lsr_sum FLOAT64,
  lsr_rank INTEGER,
  lsr_bucket STRING,
  ed_sum FLOAT64,
  ed_rank INTEGER,
  ed_bucket STRING,
  combo_score INTEGER,
  isSameIndustry INTEGER,
  insert_process_uuid STRING
)
CLUSTER BY Year, isSameIndustry;


-- Table to hold processing metadata
-- ---------------------------------
CREATE OR REPLACE TABLE `rw-algotrader.equity_statarb.load_process_meta` (
    process_uuid STRING NOT NULL,
    run_timestamp TIMESTAMP NOT NULL,
    parent_process_uuid STRING,
    process_name STRING,
    year_processed INTEGER,
    records_processed INTEGER
);
