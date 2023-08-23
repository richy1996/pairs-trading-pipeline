-- Run this to create the stored procedure to populate the `spreads` table
-- NOTE: change the project, dataset to that of your project. 

CREATE OR REPLACE PROCEDURE `rw-algotrader.equity_statarb.load_spreads`(_process_year INT64, _parent_uuid STRING)
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
    'load_spreads',
    _process_year,
    NULL
  );

  -- Delete any records we've already processed for that year
  DELETE FROM `rw-algotrader.equity_statarb.spreads` WHERE EXTRACT(YEAR FROM date) = _process_year;
 
  -- Populate new records
  INSERT INTO `rw-algotrader.equity_statarb.spreads` (
    date,
    stock1,
    stock2,
    close1,
    close2,
    ratiospread,
    spreadclose,
    sma20,
    sd20,
    zscore,
    insert_process_uuid
  )

  WITH longwindow AS (
      SELECT 
          d1.date,
          d1.ticker as stock1, 
          d2.ticker as stock2,
          d1.close as close1,
          d2.close as close2,
          d1.close / d2.close as ratiospread,
          LN(d1.close) - LN(d2.close) as spreadclose,
          AVG(LN(d1.close) - LN(d2.close)) OVER (PARTITION BY d1.ticker, d2.ticker ORDER BY d1.date ASC ROWS 19 PRECEDING) as sma20,
          STDDEV(LN(d1.close) - LN(d2.close)) OVER (PARTITION BY d1.ticker, d2.ticker ORDER BY d1.date ASC ROWS 19 PRECEDING) as sd20,
      FROM `rw-algotrader.equity_statarb.pairs_universe` p
      INNER JOIN `rw-algotrader.equity_statarb.v_load_dailyprices` d1 ON p.stock1 = d1.ticker
      INNER JOIN `rw-algotrader.equity_statarb.v_load_dailyprices` d2 ON p.stock2 = d2.ticker AND d1.date = d2.date
      WHERE p.universeyear = _process_year 
        AND d1.date >= DATE_SUB(_process_date, INTERVAL 1 MONTH) and d1.date < DATE_ADD(_process_date, INTERVAL 1 YEAR)
  )
  
  SELECT 
      date,
      stock1,
      stock2,
      close1,
      close2,
      ratiospread,
      spreadclose,
      sma20,
      sd20,
      SAFE_DIVIDE((spreadclose - sma20), sd20) as zscore,
      _process_uuid as insert_process_id
  FROM longwindow WHERE date >= _process_date;

  
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
    'load_spreads' as process_name,
    _process_year as year_processed,
    COUNT(1) as records_processsed
  FROM `rw-algotrader.equity_statarb.spreads`
  WHERE insert_process_uuid = _process_uuid;
 
END;