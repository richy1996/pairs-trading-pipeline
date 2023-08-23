-- Run this to create the stored procedure to populate the `ed_feaure`
-- NOTE: change the project, dataset to that of your project. 

CREATE OR REPLACE PROCEDURE `rw-algotrader.equity_statarb.load_ed_feature`(_process_year INT64, _parent_uuid STRING)
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
    'load_ed_feature',
    _process_year,
    NULL
  );

  -- Delete any records we've already processed for that year
  DELETE FROM `rw-algotrader.equity_statarb.ed_feature` WHERE EXTRACT(YEAR FROM startofmonth) = _process_year;
 
  -- Populate new records
  INSERT INTO `rw-algotrader.equity_statarb.ed_feature` (
    startofmonth,
    stock1,
    stock2,
    ed,
    insert_process_uuid
  )

  WITH first_prices AS (
    -- Now get the first prices of the stocks for the month.
    SELECT 
        stock1, stock2,
        DATE_TRUNC(date, MONTH) as startofmonth,
        date as first_trading_day,
        close1 as firstclose1,
        close2 as firstclose2
    FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY stock1, stock2, DATE_TRUNC(date, MONTH) ORDER BY date ASC) as row_number
        FROM `rw-algotrader.equity_statarb.spreads`
    )
    WHERE row_number = 1
  )
  
  SELECT 
      f.startofmonth,
      s.stock1, s.stock2,
      SUM(POW(SAFE_DIVIDE(close1, firstclose1) - SAFE_DIVIDE(close2, firstclose2),2)) as distance,
      _process_uuid as insert_process_id
  FROM `rw-algotrader.equity_statarb.spreads` s
  INNER JOIN first_prices f ON s.stock1 = f.stock1 AND s.stock2 = f.stock2 AND f.startofmonth = DATE_TRUNC(s.date, MONTH)
  WHERE s.date >= _process_date AND s.date < DATE_ADD(_process_date, INTERVAL 1 YEAR)
  GROUP BY   
      f.startofmonth,
      s.stock1, s.stock2;

  
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
    'load_ed_feature' as process_name,
    _process_year as year_processed,
    COUNT(1) as records_processsed
  FROM `rw-algotrader.equity_statarb.ed_feature`
  WHERE insert_process_uuid = _process_uuid;
 
END;