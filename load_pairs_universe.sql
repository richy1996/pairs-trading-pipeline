-- Run this to create the stored procedure to populate the `pairs_universe` table
-- NOTE: change the project, dataset to that of your project. 

CREATE OR REPLACE PROCEDURE `rw-algotrader.equity_statarb.load_pairs_universe`(_process_year INT64, _parent_uuid STRING)
BEGIN 

  -- Set process uuid
  DECLARE _process_uuid STRING;
  SET _process_uuid = (SELECT GENERATE_UUID());

  -- Write the details of the attempt to process to load_process_meta
  INSERT INTO `rw-algotrader.equity_statarb.load_process_meta` (
    process_uuid,
    run_timestamp,
    parent_process_uuid,
    process_name,
    year_processed,
    records_processed
  )
  VALUES(
    _process_uuid,
    CURRENT_TIMESTAMP(),
    _parent_uuid,
    'load_pairs_universe',
    _process_year,
    NULL
  );

  -- Delete any records we've already processed for that year
  DELETE FROM `rw-algotrader.equity_statarb.pairs_universe` WHERE universeyear = _process_year;
 
  -- Populate new records
  INSERT INTO `rw-algotrader.equity_statarb.pairs_universe` (
    universeyear,
    stock1,
    stock2,
    isSameSector,
    isSameIndustry,
    isDelisted,
    insert_process_uuid
  )
  WITH pairs AS (
    SELECT DISTINCT
        -- order them alphabetically so stock1 comes at the start
        CASE WHEN a.ticker < b.ticker THEN a.ticker ELSE b.ticker END as stock1,
        CASE WHEN a.ticker < b.ticker THEN b.ticker ELSE a.ticker END as stock2,
    FROM `rw-algotrader.equity_statarb.liquid_universe` a 
    CROSS JOIN `rw-algotrader.equity_statarb.liquid_universe` b
    WHERE a.ticker <> b.ticker
    AND a.universeyear = b.universeyear AND a.universeyear = _process_year
  )
 
  SELECT
      _process_year as universeyear,
      p.stock1,
      p.stock2,
      CASE WHEN t1.sector = t2.sector THEN 1 ELSE 0 END as isSameSector,
      CASE WHEN t1.industry = t2.industry THEN 1 ELSE 0 END as isSameIndustry,
      CASE WHEN t1.isdelisted OR t2.isdelisted THEN 1 ELSE 0 END isDelisted,
      _process_uuid as insert_process_uuid
  FROM pairs p
  INNER JOIN `rw-algotrader.equity_statarb.v_load_tickers` t1 ON t1.ticker = p.stock1
  INNER JOIN `rw-algotrader.equity_statarb.v_load_tickers` t2 ON t2.ticker = p.stock2;

  
  -- Insert metadata of the stuff we inserted
  DELETE FROM `rw-algotrader.equity_statarb.load_process_meta` WHERE process_uuid = _process_uuid;

  INSERT INTO `rw-algotrader.equity_statarb.load_process_meta` (
    process_uuid,
    run_timestamp,
    parent_process_uuid,
    process_name,
    year_processed,
    records_processed
  )
  SELECT 
    _process_uuid as process_id,
    CURRENT_TIMESTAMP() as run_timestamp,
    _parent_uuid as parent_process_id,
    'load_pairs_universe' as process_name,
    _process_year as year_processed,
    COUNT(1) as records_processsed
  FROM `rw-algotrader.equity_statarb.pairs_universe`
  WHERE insert_process_uuid = _process_uuid;
 
END;