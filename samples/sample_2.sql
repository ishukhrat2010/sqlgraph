-- gold_olaf_agg_month_growth.sql
--
-- SQL file to create derived aggregates for OLAF
--
-- Arguments:
--   processing_time (timestamp): This is the date that the ETL will use as the current date
--   months_to_sync (int): non-negative integer number of months to process looking back from the processing date
-- -------------------------------------------------------------------------------------------------------

  ---------------------------------------------------------------------------------
  -- PROCESS : CREATE TABLE temp_parameters
  -- CONTEXT : Create parameters table to hold reusable variables in processing
  ---------------------------------------------------------------------------------
  DROP TABLE IF EXISTS temp_parameters;
  CREATE TEMP TABLE temp_parameters AS
  SELECT 
      '{{ processing_time }}'::TIMESTAMP                                      AS processing_time
    , {{ months_to_sync }}                                                    AS months_to_sync
    , DATE_TRUNC('DAY', DATEADD(DAY, -1, processing_time))                    AS end_date
    , DATE_TRUNC('MONTH', DATEADD(MONTH, (-1 * months_to_sync) +1, end_date)) AS start_date
    , TO_CHAR(end_date, 'YYYYMMDD')::INTEGER                                  AS end_date_key
    , TO_CHAR(start_date, 'YYYYMMDD')::INTEGER                                AS start_date_key
;
