-- Run this to create the stored procedure to populate the `yearly_scores` table
-- NOTE: change the project, dataset to that of your project. 


CREATE OR REPLACE PROCEDURE `rw-algotrader.equity_statarb.load_yearly_scores`(_process_year INT64, _parent_uuid STRING)
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
    'load_yearly_scores',
    _process_year,
    NULL
  );

  -- Delete any records we've already processed for that year
  DELETE FROM `rw-algotrader.equity_statarb.yearly_scores` WHERE year = _process_year;

  INSERT INTO `rw-algotrader.equity_statarb.yearly_scores` (
      year,
      stock1,
      stock2,
      lsr_sum,
      lsr_rank,
      lsr_bucket,
      ed_sum,
      ed_rank,
      ed_bucket,
      combo_score,
      isSameIndustry,
      insert_process_uuid
  )


   WITH lsr_yearly AS (
        
        -- Aggregate LSR score to annual for each year and pair
        
        SELECT 
            _process_year as year,
            stock1,
            stock2,
            SUM(lsr) as lsr_sum
        FROM `rw-algotrader.equity_statarb.lsr_feature`
        WHERE EXTRACT(YEAR from startofmonth) = _process_year
        GROUP BY 
            EXTRACT(YEAR FROM startofmonth),
            stock1,
            stock2
    ),

    lsr_ranks AS (
        SELECT
            year,
            stock1,
            stock2,
            lsr_sum,
            ROW_NUMBER() OVER (PARTITION BY year ORDER BY lsr_sum DESC) as lsr_rank,
            ml.quantile_bucketize(lsr_sum, 100) OVER () as lsr_bucket
        FROM lsr_yearly
    ),

    ed_yearly AS (

        -- Aggregate ED score to annual for each year and pair

        SELECT 
            _process_year as year,
            stock1,
            stock2,
            SUM(ed) as ed_sum
        FROM `rw-algotrader.equity_statarb.ed_feature`
        WHERE EXTRACT(YEAR from startofmonth) = _process_year
        GROUP BY 
            EXTRACT(YEAR FROM startofmonth),
            stock1,
            stock2
    ),

    ed_ranks AS (
        SELECT
            year,
            stock1,
            stock2,
            ed_sum,
            ROW_NUMBER() OVER (PARTITION BY year ORDER BY ed_sum ASC) as ed_rank, -- smaller better for ed ranks
            ml.quantile_bucketize(-ed_sum, 100) OVER () as ed_bucket -- smaller better for ed ranks
        FROM ed_yearly
        WHERE year = 2021 -- TODO param
    )

    SELECT
        lsr.year,
        lsr.stock1,
        lsr.stock2,
        lsr.lsr_sum,
        lsr.lsr_rank,
        lsr.lsr_bucket,
        ed.ed_sum,
        ed.ed_rank,
        ed.ed_bucket,
        lsr.lsr_rank + ed.ed_rank as combo_score,
        p.isSameIndustry as isSameIndustry,
        _process_uuid as insert_process_uuid
    FROM lsr_ranks lsr 
    INNER JOIN ed_ranks ed ON lsr.year = ed.year AND lsr.stock1 = ed.stock1 AND lsr.stock2 = ed.stock2
    INNER JOIN `rw-algotrader.equity_statarb.pairs_universe` p ON lsr.year = p.universeyear and lsr.stock1 = p.stock1 AND lsr.stock2 = p.stock2;

    -- Add Metadata to stuff we inserted
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
    'load_yearly_scores' as process_name,
    _process_year as year_processed,
    COUNT(1) as records_processsed
  FROM `rw-algotrader.equity_statarb.yearly_scores`
  WHERE insert_process_uuid = _process_uuid;

END;