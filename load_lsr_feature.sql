-- Run this to create the stored procedure to populate the `lsr_feature` table
-- NOTE: change the project, dataset to that of your project. 

CREATE OR REPLACE PROCEDURE `rw-algotrader.equity_statarb.load_lsr_feature`(_process_year INT64, _parent_uuid STRING)
BEGIN 

  -- Set process uuid
  DECLARE _process_uuid STRING;
  DECLARE _process_date DATE;
  SET _process_uuid = (SELECT GENERATE_UUID());
  SET _process_date = (SELECT(DATE(_process_year,1,1)));

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
    'load_lsr_feature',
    _process_year,
    NULL
  );

  -- Delete any records we've already processed for that year
  DELETE FROM `rw-algotrader.equity_statarb.lsr_feature` WHERE EXTRACT(YEAR FROM startofmonth) = _process_year;
 
  -- Populate new records
  INSERT INTO `rw-algotrader.equity_statarb.lsr_feature` (
    startofmonth,
    stock1,
    stock2,
    lsr,
    insert_process_uuid
  )

  WITH dailyreturns AS (
      SELECT
          DATE_TRUNC(date, MONTH) as startofmonth,
          date,
          stock1,
          stock2,
          -LAG(zscore) OVER (PARTITION BY stock1, stock2 ORDER BY date ASC) * IFNULL(LN(ratiospread) - LN( LAG(ratiospread) OVER (PARTITION BY stock1, stock2 ORDER BY date ASC)), 0) as lsr
      FROM `rw-algotrader.equity_statarb.spreads`
      WHERE date >= DATE_SUB(_process_date, INTERVAL 1 DAY) and date < DATE_ADD(_process_date, INTERVAL 1 YEAR)
  )
  
  SELECT
      startofmonth,
      stock1,
      stock2,
      SUM(lsr) as lsr,
      _process_uuid as insert_process_uuid
  FROM dailyreturns
  WHERE startofmonth >= _process_date
  GROUP BY
      startofmonth,
      stock1,
      stock2;

  
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
    'load_lsr_feature' as process_name,
    _process_year as year_processed,
    COUNT(1) as records_processsed
  FROM `rw-algotrader.equity_statarb.lsr_feature`
  WHERE insert_process_uuid = _process_uuid;
 
END;