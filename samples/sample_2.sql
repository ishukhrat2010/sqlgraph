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
-- --------------------------------------------------------------------------------------------------------------------
-- BEGIN DISTRICT DATES/PROPERTIES
-- --------------------------------------------------------------------------------------------------------------------
-- District dates of interest within school years overlapping the processing range.
  DROP TABLE IF EXISTS temp_district_dates;
  CREATE TEMP TABLE temp_district_dates DISTSTYLE ALL
                                        SORTKEY(district_id) AS
  SELECT
      ddd.district_id
      , ddd.is_screener
      , ddd.start_year
      , ddd.school_year_start_date
      , ddd.school_year_end_date
      , dd.day_key                                                                AS max_processed_day_key
      , date(dd.calendar_date)                                                    AS max_calendar_date
      , dateadd(ms, -1, dateadd(day, 1, dd.calendar_date))                        AS max_calendar_date_end_time
      , date(dd.month_end_date)                                                   AS last_day_of_month
      , date(dd.month_begin_date)                                                 AS first_day_of_month
      , DECODE(dd.calendar_date, last_day_of_month, 1, 0)                         AS is_month_end
      , to_char(date_trunc('month', dd.calendar_date), 'YYYYMMDD')::integer       AS month_start_day_key
      , CASE WHEN dd.calendar_date BETWEEN tp.start_date AND tp.end_date
          THEN 1
          ELSE 0
        END                                                                       AS is_within_processing_dates
  FROM gold.dim_district_dates AS ddd
  INNER JOIN temp_parameters AS tp
      ON ddd.school_year_start_date <= tp.end_date
      AND ddd.school_year_end_date >= tp.start_date
  INNER JOIN gold.dim_day AS dd
      ON dd.calendar_date BETWEEN ddd.school_year_start_date AND LEAST(tp.end_date, ddd.school_year_end_date)
  WHERE
      (is_month_end OR date(dd.calendar_date) >= LEAST(tp.end_date, ddd.school_year_end_date))
  ;
