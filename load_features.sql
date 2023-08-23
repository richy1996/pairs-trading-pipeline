-- Run this to create the stored procedure to run the feature load process
-- NOTE: change the project, dataset to that of your project. 


CREATE OR REPLACE PROCEDURE `rw-algotrader.equity_statarb.load_features`(_process_year INT64)
BEGIN
   
    -- Load a year of analytical data 
    -- ==============================

    -- Set process id
    DECLARE _process_uuid STRING;
    SET _process_uuid = (SELECT GENERATE_UUID());

    
    -- Write job metadata
    -- ===================
    
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
        NULL,
        'load_features',
        _process_year,
        NULL
    );

    -- Load Process
    -- ============

    -- Process Liquid Universe
    CALL `rw-algotrader.equity_statarb.load_liquid_universe`(_process_year, _process_uuid);
    
    -- Process Pairs Universe
    CALL `rw-algotrader.equity_statarb.load_pairs_universe`(_process_year, _process_uuid);
    
    -- Load Spreads
    CALL `rw-algotrader.equity_statarb.load_spreads`(_process_year, _process_uuid);

    -- Load LSR Feature
    CALL `rw-algotrader.equity_statarb.load_lsr_feature`(_process_year, _process_uuid);

    -- Load ED Feature
    CALL `rw-algotrader.equity_statarb.load_ed_feature`(_process_year, _process_uuid);

    -- Aggregate yearly scores
    CALL `rw-algotrader.equity_statarb.load_yearly_scores`(_process_year, _process_uuid);
    
    -- Log Meta Data
    -- =============
    
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
        _process_uuid,
        CURRENT_TIMESTAMP() as run_timestamp, -- todo use job time perhaps?
        NULL as parent_proces_uuid,
        'load_iterative' as process_name,
        _process_year as year_processed,
        SUM(records_processed)
    FROM `rw-algotrader.equity_statarb.load_process_meta`
    WHERE parent_process_uuid = _process_uuid AND process_name <> 'load_features';

END;