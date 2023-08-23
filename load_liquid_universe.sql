-- Run this to create the stored procedure to populate the `liquid_universe`
-- NOTE: change the project, dataset to that of your project. 

CREATE OR REPLACE PROCEDURE `rw-algotrader.equity_statarb.load_liquid_universe`(_process_year INT64, _parent_uuid STRING)
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
    'load_liquid_universe',
    _process_year,
    NULL
  );

  -- Delete any records we've already processed for that year
  DELETE FROM `rw-algotrader.equity_statarb.liquid_universe` WHERE universeyear = _process_year;
 
  -- Populate new records
  INSERT INTO `rw-algotrader.equity_statarb.liquid_universe` (
    universeyear,
    ticker,
    marketcap,
    avg_price,
    avg_volume,
    insert_process_uuid
  )
    SELECT
      _process_year as universeyear,
      d.ticker,
      marketcap,
      avg_price,
      avg_volume,
      _process_uuid
    FROM `rw-algotrader.equity_statarb.v_load_dailysnapshot` d
    INNER JOIN `rw-algotrader.equity_statarb.v_start_of_year` y ON y.start_of_year = d.date
    INNER JOIN (
      SELECT
        ticker,
        AVG(unadjusted_close) as avg_price,
        AVG(volume) as avg_volume
      FROM  `rw-algotrader.equity_statarb.v_load_dailyprices`
      WHERE EXTRACT(year from date) = _process_year
      GROUP BY ticker
    ) a ON d.ticker = a.ticker
    WHERE
      year = _process_year 
      AND marketcap > 1000 -- marketcap is in millions
      AND avg_price > 5
      AND avg_volume > 300000;

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
    'load_liquid_universe' as process_name,
    _process_year as year_processed,
    COUNT(1) as records_processsed
  FROM `rw-algotrader.equity_statarb.liquid_universe`
  WHERE insert_process_uuid = _process_uuid;
 
END;