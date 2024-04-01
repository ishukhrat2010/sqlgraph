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
  ---------------------------------------------------------------------------------
  -- PROCESS : CREATE TABLE temp_district_roster
  -- CONTEXT : Create table containing district_id
  ---------------------------------------------------------------------------------
  DROP TABLE IF EXISTS temp_district_roster;
  CREATE TEMP TABLE temp_district_roster DISTKEY(user_id)
                                         SORTKEY(district_id, day_key) AS
  SELECT 
        TDD.month_start_day_key AS day_key
      , TDD.max_calendar_date
      , TDD.max_processed_day_key
      , TDD.is_screener
      , TDD.start_year
      , ROS.district_id
      , ROS.user_id
      , ROS.student_grade
      , STAN.academic_mapping_id
      , STAN.subject_id
      , CASE
            WHEN ADSOM.full_year_lessons_week_all_grades >= 5 THEN '5_or_more'
            WHEN ADSOM.full_year_lessons_week_all_grades >= 2 THEN '2_to_5'
            ELSE '0_to_2'
        END usage_level
  FROM temp_district_dates AS TDD
  INNER JOIN {{ schema }}.olaf_fact_district_student_day_roster AS ROS
          ON TDD.district_id = ROS.district_id
         AND TDD.max_processed_day_key = ROS.day_key
  LEFT JOIN {{ schema }}.agg_day_student_olaf_metrics AS ADSOM
         ON ROS.user_id = ADSOM.user_id
        AND TDD.max_processed_day_key = ADSOM.day_key
        AND ROS.district_id = ADSOM.district_id
  LEFT JOIN {{ schema }}.dim_district_academic_standard AS STAN
         ON ROS.district_id = STAN.district_id 
        AND TDD.max_calendar_date BETWEEN STAN.version_effective_date AND STAN.version_expiration_date
        AND STAN.subject_id = 1
  WHERE TDD.is_within_processing_dates
    AND ROS.application_id = 1
;
  ---------------------------------------------------------------------------------
  -- PROCESS : CREATE TABLE temp_school_roster
  -- CONTEXT : Create table containing school_id
  ---------------------------------------------------------------------------------
  DROP TABLE IF EXISTS temp_school_roster;
  CREATE TEMP TABLE temp_school_roster DISTKEY(user_id)
                                       SORTKEY(district_id, day_key) AS     
  SELECT 
        TDD.month_start_day_key AS day_key
      , TDD.max_calendar_date
      , TDD.max_processed_day_key
      , TDD.is_screener
      , TDD.start_year
      , ROS.district_id
      , ROS.school_id
      , ROS.user_id
      , ROS.student_grade
      , STAN.academic_mapping_id
      , STAN.subject_id
      , CASE
            WHEN ADSOM.full_year_lessons_week_all_grades >= 5 THEN '5_or_more'
            WHEN ADSOM.full_year_lessons_week_all_grades >= 2 THEN '2_to_5'
            ELSE '0_to_2'
        END usage_level
  FROM temp_district_dates AS TDD
  INNER JOIN {{ schema }}.olaf_fact_school_student_day_roster AS ROS
          ON TDD.district_id = ROS.district_id
         AND TDD.max_processed_day_key = ROS.day_key
  LEFT JOIN {{ schema }}.agg_day_student_olaf_metrics AS ADSOM
         ON ROS.user_id = ADSOM.user_id
        AND TDD.max_processed_day_key = ADSOM.day_key
        AND ROS.district_id = ADSOM.district_id
  LEFT JOIN {{ schema }}.dim_district_academic_standard AS STAN
         ON ROS.district_id = STAN.district_id 
        AND TDD.max_calendar_date BETWEEN STAN.version_effective_date AND STAN.version_expiration_date
        AND STAN.subject_id = 1
  WHERE TDD.is_within_processing_dates
    AND ROS.application_id = 1
;
  ---------------------------------------------------------------------------------
  -- PROCESS : CREATE TABLE temp_school_teacher_roster
  -- CONTEXT : Create table containing school_id and customer_id
  ---------------------------------------------------------------------------------
  DROP TABLE IF EXISTS temp_school_teacher_roster;
  CREATE TEMP TABLE temp_school_teacher_roster DISTKEY(user_id)
                                               SORTKEY(district_id, day_key) AS     
  SELECT 
        TDD.month_start_day_key AS day_key
      , TDD.max_calendar_date
      , TDD.max_processed_day_key
      , TDD.is_screener
      , TDD.start_year
      , ROS.district_id
      , ROS.school_id
      , ROS.customer_id
      , ROS.user_id
      , ROS.student_grade
      , STAN.academic_mapping_id
      , STAN.subject_id
      , CASE
            WHEN ADSOM.full_year_lessons_week_all_grades >= 5 THEN '5_or_more'
            WHEN ADSOM.full_year_lessons_week_all_grades >= 2 THEN '2_to_5'
            ELSE '0_to_2'
        END usage_level
  FROM temp_district_dates AS TDD
  INNER JOIN {{ schema }}.olaf_fact_school_teacher_student_day_roster AS ROS
          ON TDD.district_id = ROS.district_id
         AND TDD.max_processed_day_key = ROS.day_key
  LEFT JOIN {{ schema }}.agg_day_student_olaf_metrics AS ADSOM
         ON ROS.user_id = ADSOM.user_id
        AND TDD.max_processed_day_key = ADSOM.day_key
        AND ROS.district_id = ADSOM.district_id
  LEFT JOIN {{ schema }}.dim_district_academic_standard AS STAN
         ON ROS.district_id = STAN.district_id 
        AND TDD.max_calendar_date BETWEEN STAN.version_effective_date AND STAN.version_expiration_date
        AND STAN.subject_id = 1
  WHERE TDD.is_within_processing_dates
    AND ROS.application_id = 1
;
  ---------------------------------------------------------------------------------
  -- PROCESS : CREATE TABLE temp_class_roster
  -- CONTEXT : Create table containing after_school_program_id
  ---------------------------------------------------------------------------------
  DROP TABLE IF EXISTS temp_class_roster;
  CREATE TEMP TABLE temp_class_roster DISTKEY(user_id)
                                      SORTKEY(district_id, day_key) AS     
  SELECT 
        TDD.month_start_day_key AS day_key
      , TDD.max_calendar_date
      , TDD.max_processed_day_key
      , TDD.is_screener
      , TDD.start_year
      , ROS.district_id
      , ROS.user_id
      , ROS.student_grade
      , ROS.after_school_program_id
      , STAN.academic_mapping_id
      , STAN.subject_id
      , CASE
            WHEN ADSOM.full_year_lessons_week_all_grades >= 5 THEN '5_or_more'
            WHEN ADSOM.full_year_lessons_week_all_grades >= 2 THEN '2_to_5'
            ELSE '0_to_2'
        END usage_level
  FROM temp_district_dates as TDD
  INNER JOIN {{ schema }}.olaf_fact_class_student_day_roster AS ROS
          ON TDD.district_id = ROS.district_id
         AND TDD.max_processed_day_key = ROS.day_key
  LEFT JOIN {{ schema }}.agg_day_student_olaf_metrics AS ADSOM
         ON ROS.user_id = ADSOM.user_id
        AND TDD.max_processed_day_key = ADSOM.day_key
        AND ROS.district_id = ADSOM.district_id
  LEFT JOIN {{ schema }}.dim_district_academic_standard AS STAN
         ON ROS.district_id = STAN.district_id 
        AND TDD.max_calendar_date BETWEEN STAN.version_effective_date AND STAN.version_expiration_date
        AND STAN.subject_id = 1
  WHERE TDD.is_within_processing_dates
    AND ROS.application_id = 1
;
----------------------------------------------------------------------
  -- PROCESS : DELETE {{ schema }}.olaf_cagg___metrics
  -- CONTEXT : Clean up previously written data to prepare for writing new data.
   ---------------------------------------------------------------------------------

BEGIN TRANSACTION;

  DELETE FROM {{ schema }}.olaf_cagg_district_month_growth WHERE day_key BETWEEN (SELECT PAR.start_date_key FROM temp_parameters AS PAR) AND (SELECT PAR.end_date_key FROM temp_parameters AS PAR);
  DELETE FROM {{ schema }}.olaf_cagg_class_month_growth WHERE day_key BETWEEN (SELECT PAR.start_date_key FROM temp_parameters AS PAR) AND (SELECT PAR.end_date_key FROM temp_parameters AS PAR);
  DELETE FROM {{ schema }}.olaf_cagg_class_grade_month_growth WHERE day_key BETWEEN (SELECT PAR.start_date_key FROM temp_parameters AS PAR) AND (SELECT PAR.end_date_key FROM temp_parameters AS PAR);
  DELETE FROM {{ schema }}.olaf_cagg_grade_month_growth WHERE day_key BETWEEN (SELECT PAR.start_date_key FROM temp_parameters AS PAR) AND (SELECT PAR.end_date_key FROM temp_parameters AS PAR);
  DELETE FROM {{ schema }}.olaf_cagg_school_month_growth WHERE day_key BETWEEN (SELECT PAR.start_date_key FROM temp_parameters AS PAR) AND (SELECT PAR.end_date_key FROM temp_parameters AS PAR);
  DELETE FROM {{ schema }}.olaf_cagg_school_grade_month_growth WHERE day_key BETWEEN (SELECT PAR.start_date_key FROM temp_parameters AS PAR) AND (SELECT PAR.end_date_key FROM temp_parameters AS PAR);
  DELETE FROM {{ schema }}.olaf_cagg_school_teacher_month_growth WHERE day_key BETWEEN (SELECT PAR.start_date_key FROM temp_parameters AS PAR) AND (SELECT PAR.end_date_key FROM temp_parameters AS PAR);
  DELETE FROM {{ schema }}.olaf_cagg_school_teacher_grade_month_growth WHERE day_key BETWEEN (SELECT PAR.start_date_key FROM temp_parameters AS PAR) AND (SELECT PAR.end_date_key FROM temp_parameters AS PAR);


  ---------------------------------------------------------------------------------
  -- PROCESS : INSERT INTO {{ schema }}.olaf_cagg_district_month_growth
  -- CONTEXT : As specified by OLAF requirements, write each metric for district
   ---------------------------------------------------------------------------------
  INSERT INTO {{ schema }}.olaf_cagg_district_month_growth
  WITH roster AS (
    SELECT DISTINCT
          day_key
        , max_processed_day_key
        , is_screener
        , start_year
        , subject_id
        , user_id
        , district_id
        , student_grade
        , academic_mapping_id
        , usage_level
    FROM temp_district_roster
  )
  SELECT     
        roster.day_key
      , TO_DATE(roster.day_key,'yyyymmdd') AS calendar_date
      , roster.max_processed_day_key
      , roster.is_screener
      , roster.start_year
      , roster.academic_mapping_id
      , roster.subject_id
      , roster.district_id
      , COUNT(DISTINCT roster.user_id) AS count_of_rostered_students
      , COUNT(DISTINCT CASE WHEN growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students
      , AVG(growth.initial_level) AS initial_avg_level
      , AVG(growth.current_level) AS current_avg_level   
      , AVG(CASE WHEN (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level
      , AVG(CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) AS initial_avg_level_near_level
      , AVG(CASE WHEN (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level
      , AVG(CASE WHEN (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level
      , AVG(CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level
      , AVG(CASE WHEN (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level       
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) >= 0 then growth.user_id END) AS current_student_count_at_above_level
      --- USAGE_LEVEL = '0_to_2'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' THEN roster.user_id END) AS count_of_rostered_students_0_to_2
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '0_to_2' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' THEN growth.initial_level END) AS initial_avg_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' THEN growth.current_level END) AS current_avg_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_0_to_2
      , AVG(CASE when roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_0_to_2
      --- USAGE_LEVEL = '2_to_5'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' THEN roster.user_id END) AS count_of_rostered_students_2_to_5
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '2_to_5' and growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' THEN growth.initial_level END) AS initial_avg_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' THEN growth.current_level END) AS current_avg_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_2_to_5
      , AVG(CASE when roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_2_to_5
      --- USAGE_LEVEL = '5_or_more'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' THEN roster.user_id END) AS count_of_rostered_students_5_or_more
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '5_or_more' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' THEN growth.initial_level END) AS initial_avg_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' THEN growth.current_level END) AS current_avg_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_5_or_more
      , AVG(CASE when roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_5_or_more
  FROM roster
  LEFT JOIN {{ schema }}.olaf_cagg_student_month_growth growth
  USING (day_key, user_id, district_id, student_grade, academic_mapping_id, start_year)
  GROUP BY
        roster.day_key
      , roster.is_screener
      , roster.start_year
      , roster.academic_mapping_id
      , roster.subject_id
      , roster.district_id
      , roster.max_processed_day_key
;

  ---------------------------------------------------------------------------------
  -- PROCESS : INSERT INTO {{ schema }}.olaf_cagg_class_month_growth
  -- CONTEXT : As specified by OLAF requirements, write each metric for district and after_school_program
   ---------------------------------------------------------------------------------
  INSERT INTO {{ schema }}.olaf_cagg_class_month_growth
  WITH roster AS (
    SELECT DISTINCT
          day_key
        , max_processed_day_key
        , is_screener
        , start_year
        , subject_id
        , user_id
        , district_id
        , student_grade
        , academic_mapping_id
        , usage_level
        , after_school_program_id
    FROM temp_class_roster
  )
  SELECT
        roster.day_key
      , TO_DATE(roster.day_key,'yyyymmdd') AS calendar_date
      , roster.max_processed_day_key
      , roster.is_screener
      , roster.start_year
      , roster.academic_mapping_id
      , roster.subject_id
      , roster.district_id
      , roster.after_school_program_id
      , COUNT(DISTINCT roster.user_id) AS count_of_rostered_students
      , COUNT(DISTINCT CASE WHEN growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students
      , AVG(growth.initial_level) AS initial_avg_level
      , AVG(growth.current_level) AS current_avg_level
      , AVG(CASE WHEN (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level
      , AVG(CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) AS initial_avg_level_near_level
      , AVG(CASE WHEN (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level
      , AVG(CASE WHEN (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level
      , AVG(CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level
      , AVG(CASE WHEN (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) >= 0 then growth.user_id END) AS current_student_count_at_above_level
      --- USAGE_LEVEL = '0_to_2'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' THEN roster.user_id END) AS count_of_rostered_students_0_to_2
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '0_to_2' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' THEN growth.initial_level END) AS initial_avg_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' THEN growth.current_level END) AS current_avg_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_0_to_2
      , AVG(CASE when roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_0_to_2
      --- USAGE_LEVEL = '2_to_5'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' THEN roster.user_id END) AS count_of_rostered_students_2_to_5
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '2_to_5' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' THEN growth.initial_level END) AS initial_avg_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' THEN growth.current_level END) AS current_avg_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_2_to_5
      , AVG(CASE when roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_2_to_5
      --- USAGE_LEVEL = '5_or_more'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' THEN roster.user_id END) AS count_of_rostered_students_5_or_more
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '5_or_more' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' THEN growth.initial_level END) AS initial_avg_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' THEN growth.current_level END) AS current_avg_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_5_or_more
      , AVG(CASE when roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_5_or_more
  FROM roster
  LEFT JOIN {{ schema }}.olaf_cagg_student_month_growth growth
  USING (day_key, user_id, district_id, student_grade, academic_mapping_id, start_year)
  GROUP BY
        roster.day_key
      , roster.is_screener
      , roster.start_year
      , roster.academic_mapping_id
      , roster.subject_id
      , roster.district_id
      , roster.after_school_program_id
      , roster.max_processed_day_key
;

  ---------------------------------------------------------------------------------
  -- PROCESS : INSERT INTO {{ schema }}.olaf_cagg_class_grade_month_growth
  -- CONTEXT : As specified by OLAF requirements, write each metric for district, after_school_program and student_grade
   ---------------------------------------------------------------------------------
  INSERT INTO {{ schema }}.olaf_cagg_class_grade_month_growth
  WITH roster AS (
    SELECT DISTINCT
          day_key
        , max_processed_day_key
        , is_screener
        , start_year
        , subject_id
        , user_id
        , district_id
        , student_grade
        , academic_mapping_id
        , usage_level
        , after_school_program_id
    FROM temp_class_roster
  )
  SELECT
        roster.day_key
      , TO_DATE(roster.day_key,'yyyymmdd') AS calendar_date
      , roster.max_processed_day_key
      , roster.is_screener
      , roster.start_year
      , roster.academic_mapping_id
      , roster.subject_id
      , roster.district_id
      , roster.after_school_program_id
      , roster.student_grade
      , COUNT(DISTINCT roster.user_id) AS count_of_rostered_students
      , COUNT(DISTINCT CASE WHEN growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students
      , AVG(growth.initial_level) AS initial_avg_level
      , AVG(growth.current_level) AS current_avg_level
      , AVG(CASE WHEN (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level
      , AVG(CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) AS initial_avg_level_near_level
      , AVG(CASE WHEN (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level
      , AVG(CASE WHEN (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level
      , AVG(CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level
      , AVG(CASE WHEN (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) >= 0 then growth.user_id END) AS current_student_count_at_above_level
      --- USAGE_LEVEL = '0_to_2'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' THEN roster.user_id END) AS count_of_rostered_students_0_to_2
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '0_to_2' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' THEN growth.initial_level END) AS initial_avg_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' THEN growth.current_level END) AS current_avg_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_0_to_2
      , AVG(CASE when roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_0_to_2
      --- USAGE_LEVEL = '2_to_5'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' THEN roster.user_id END) AS count_of_rostered_students_2_to_5
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '2_to_5' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' THEN growth.initial_level END) AS initial_avg_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' THEN growth.current_level END) AS current_avg_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_2_to_5
      , AVG(CASE when roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_2_to_5
      --- USAGE_LEVEL = '5_or_more'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' THEN roster.user_id END) AS count_of_rostered_students_5_or_more
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '5_or_more' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' THEN growth.initial_level END) AS initial_avg_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' THEN growth.current_level END) AS current_avg_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_5_or_more
      , AVG(CASE when roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_5_or_more
  FROM roster
  LEFT JOIN {{ schema }}.olaf_cagg_student_month_growth growth
  USING (day_key, user_id, district_id, student_grade, academic_mapping_id, start_year)
  GROUP BY
        roster.day_key
      , roster.is_screener
      , roster.start_year
      , roster.academic_mapping_id
      , roster.subject_id
      , roster.district_id
      , roster.after_school_program_id
      , roster.student_grade
      , roster.max_processed_day_key
;


  ---------------------------------------------------------------------------------
  -- PROCESS : INSERT INTO {{ schema }}.olaf_cagg_grade_month_growth
  -- CONTEXT : As specified by OLAF requirements, write each metric for district
   ---------------------------------------------------------------------------------
  INSERT INTO {{ schema }}.olaf_cagg_grade_month_growth
  WITH roster AS (
    SELECT DISTINCT
          day_key
        , max_processed_day_key
        , is_screener
        , start_year
        , subject_id
        , user_id
        , district_id
        , student_grade
        , academic_mapping_id
        , usage_level
    FROM temp_district_roster
  )
  SELECT
        roster.day_key
      , TO_DATE(roster.day_key,'yyyymmdd') AS calendar_date
      , roster.max_processed_day_key
      , roster.is_screener
      , roster.start_year
      , roster.academic_mapping_id
      , roster.subject_id
      , roster.district_id
      , roster.student_grade
      , COUNT(DISTINCT roster.user_id) AS count_of_rostered_students
      , COUNT(DISTINCT CASE WHEN growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students
      , AVG(growth.initial_level) AS initial_avg_level
      , AVG(growth.current_level) AS current_avg_level
      , AVG(CASE WHEN (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level
      , AVG(CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) AS initial_avg_level_near_level
      , AVG(CASE WHEN (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level
      , AVG(CASE WHEN (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level
      , AVG(CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level
      , AVG(CASE WHEN (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) >= 0 then growth.user_id END) AS current_student_count_at_above_level
      --- USAGE_LEVEL = '0_to_2'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' THEN roster.user_id END) AS count_of_rostered_students_0_to_2
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '0_to_2' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' THEN growth.initial_level END) AS initial_avg_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' THEN growth.current_level END) AS current_avg_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_0_to_2
      , AVG(CASE when roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_0_to_2
      --- USAGE_LEVEL = '2_to_5'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' THEN roster.user_id END) AS count_of_rostered_students_2_to_5
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '2_to_5' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' THEN growth.initial_level END) AS initial_avg_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' THEN growth.current_level END) AS current_avg_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_2_to_5
      , AVG(CASE when roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_2_to_5
      --- USAGE_LEVEL = '5_or_more'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' THEN roster.user_id END) AS count_of_rostered_students_5_or_more
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '5_or_more' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' THEN growth.initial_level END) AS initial_avg_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' THEN growth.current_level END) AS current_avg_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_5_or_more
      , AVG(CASE when roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_5_or_more
  FROM roster
  LEFT JOIN {{ schema }}.olaf_cagg_student_month_growth growth
  USING (day_key, user_id, district_id, student_grade, academic_mapping_id, start_year)
  GROUP BY
        roster.day_key
      , roster.is_screener
      , roster.start_year
      , roster.academic_mapping_id
      , roster.subject_id
      , roster.district_id
      , roster.student_grade
      , roster.max_processed_day_key
;


  ---------------------------------------------------------------------------------
  -- PROCESS : INSERT INTO {{ schema }}.olaf_cagg_school_month_growth
  -- CONTEXT : As specified by OLAF requirements, write each metric for district, school
   ---------------------------------------------------------------------------------
  INSERT INTO {{ schema }}.olaf_cagg_school_month_growth
  WITH roster AS (
    SELECT DISTINCT
          day_key
        , max_processed_day_key
        , is_screener
        , start_year
        , subject_id
        , user_id
        , district_id
        , student_grade
        , academic_mapping_id
        , usage_level
        , school_id
    FROM temp_school_roster
  )
  SELECT
        roster.day_key
      , TO_DATE(roster.day_key,'yyyymmdd') AS calendar_date
      , roster.max_processed_day_key
      , roster.is_screener
      , roster.start_year
      , roster.academic_mapping_id
      , roster.subject_id
      , roster.district_id
      , roster.school_id
      , COUNT(DISTINCT roster.user_id) AS count_of_rostered_students
      , COUNT(DISTINCT CASE WHEN growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students
      , AVG(growth.initial_level) AS initial_avg_level
      , AVG(growth.current_level) AS current_avg_level
      , AVG(CASE WHEN (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level
      , AVG(CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) AS initial_avg_level_near_level
      , AVG(CASE WHEN (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level
      , AVG(CASE WHEN (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level
      , AVG(CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level
      , AVG(CASE WHEN (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) >= 0 then growth.user_id END) AS current_student_count_at_above_level
      --- USAGE_LEVEL = '0_to_2'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' THEN roster.user_id END) AS count_of_rostered_students_0_to_2
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '0_to_2' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' THEN growth.initial_level END) AS initial_avg_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' THEN growth.current_level END) AS current_avg_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_0_to_2
      , AVG(CASE when roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_0_to_2
      --- USAGE_LEVEL = '2_to_5'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' THEN roster.user_id END) AS count_of_rostered_students_2_to_5
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '2_to_5' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' THEN growth.initial_level END) AS initial_avg_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' THEN growth.current_level END) AS current_avg_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_2_to_5
      , AVG(CASE when roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_2_to_5
      --- USAGE_LEVEL = '5_or_more'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' THEN roster.user_id END) AS count_of_rostered_students_5_or_more
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '5_or_more' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' THEN growth.initial_level END) AS initial_avg_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' THEN growth.current_level END) AS current_avg_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_5_or_more
      , AVG(CASE when roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_5_or_more
  FROM roster
  LEFT JOIN {{ schema }}.olaf_cagg_student_month_growth growth
  USING (day_key, user_id, district_id, student_grade, academic_mapping_id, start_year)
  GROUP BY
        roster.day_key
      , roster.is_screener
      , roster.start_year
      , roster.academic_mapping_id
      , roster.subject_id
      , roster.district_id
      , roster.school_id
      , roster.max_processed_day_key
;


  ---------------------------------------------------------------------------------
  -- PROCESS : INSERT INTO {{ schema }}.olaf_cagg_school_grade_month_growth
  -- CONTEXT : As specified by OLAF requirements, write each metric for district, school, student_grade
   ---------------------------------------------------------------------------------
  INSERT INTO {{ schema }}.olaf_cagg_school_grade_month_growth
  WITH roster AS (
    SELECT DISTINCT
          day_key
        , max_processed_day_key
        , is_screener
        , start_year
        , subject_id
        , user_id
        , district_id
        , student_grade
        , academic_mapping_id
        , usage_level
        , school_id
    FROM temp_school_roster
  )
  SELECT
        roster.day_key
      , TO_DATE(roster.day_key,'yyyymmdd') AS calendar_date
      , roster.max_processed_day_key
      , roster.is_screener
      , roster.start_year
      , roster.academic_mapping_id
      , roster.subject_id
      , roster.district_id
      , roster.school_id
      , roster.student_grade
      , COUNT(DISTINCT roster.user_id) AS count_of_rostered_students
      , COUNT(DISTINCT CASE WHEN growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students
      , AVG(growth.initial_level) AS initial_avg_level
      , AVG(growth.current_level) AS current_avg_level
      , AVG(CASE WHEN (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level
      , AVG(CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) AS initial_avg_level_near_level
      , AVG(CASE WHEN (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level
      , AVG(CASE WHEN (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level
      , AVG(CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level
      , AVG(CASE WHEN (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) >= 0 then growth.user_id END) AS current_student_count_at_above_level
      --- USAGE_LEVEL = '0_to_2'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' THEN roster.user_id END) AS count_of_rostered_students_0_to_2
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '0_to_2' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' THEN growth.initial_level END) AS initial_avg_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' THEN growth.current_level END) AS current_avg_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_0_to_2
      , AVG(CASE when roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_0_to_2
      --- USAGE_LEVEL = '2_to_5'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' THEN roster.user_id END) AS count_of_rostered_students_2_to_5
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '2_to_5' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' THEN growth.initial_level END) AS initial_avg_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' THEN growth.current_level END) AS current_avg_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_2_to_5
      , AVG(CASE when roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_2_to_5
      --- USAGE_LEVEL = '5_or_more'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' THEN roster.user_id END) AS count_of_rostered_students_5_or_more
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '5_or_more' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' THEN growth.initial_level END) AS initial_avg_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' THEN growth.current_level END) AS current_avg_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_5_or_more
      , AVG(CASE when roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_5_or_more
  FROM roster
  LEFT JOIN {{ schema }}.olaf_cagg_student_month_growth growth
  USING (day_key, user_id, district_id, student_grade, academic_mapping_id, start_year)
  GROUP BY
        roster.day_key
      , roster.is_screener
      , roster.start_year
      , roster.academic_mapping_id
      , roster.subject_id
      , roster.district_id
      , roster.school_id
      , roster.student_grade
      , roster.max_processed_day_key
;


  ---------------------------------------------------------------------------------
  -- PROCESS : INSERT INTO {{ schema }}.olaf_cagg_school_teacher_month_growth
  -- CONTEXT : As specified by OLAF requirements, write each metric for district, school, customer
   ---------------------------------------------------------------------------------
  INSERT INTO {{ schema }}.olaf_cagg_school_teacher_month_growth
  WITH roster AS (
    SELECT DISTINCT
          day_key
        , max_processed_day_key
        , is_screener
        , start_year
        , subject_id
        , user_id
        , district_id
        , student_grade
        , academic_mapping_id
        , usage_level
        , school_id
        , customer_id
    FROM temp_school_teacher_roster
  )
  SELECT
        roster.day_key
      , TO_DATE(roster.day_key,'yyyymmdd') AS calendar_date
      , roster.max_processed_day_key
      , roster.is_screener
      , roster.start_year
      , roster.academic_mapping_id
      , roster.subject_id
      , roster.district_id
      , roster.school_id
      , roster.customer_id
      , COUNT(DISTINCT roster.user_id) AS count_of_rostered_students
      , COUNT(DISTINCT CASE WHEN growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students
      , AVG(growth.initial_level) AS initial_avg_level
      , AVG(growth.current_level) AS current_avg_level
      , AVG(CASE WHEN (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level
      , AVG(CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) AS initial_avg_level_near_level
      , AVG(CASE WHEN (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level
      , AVG(CASE WHEN (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level
      , AVG(CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level
      , AVG(CASE WHEN (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) >= 0 then growth.user_id END) AS current_student_count_at_above_level
      --- USAGE_LEVEL = '0_to_2'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' THEN roster.user_id END) AS count_of_rostered_students_0_to_2
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '0_to_2' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' THEN growth.initial_level END) AS initial_avg_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' THEN growth.current_level END) AS current_avg_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_0_to_2
      , AVG(CASE when roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_0_to_2
      --- USAGE_LEVEL = '2_to_5'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' THEN roster.user_id END) AS count_of_rostered_students_2_to_5
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '2_to_5' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' THEN growth.initial_level END) AS initial_avg_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' THEN growth.current_level END) AS current_avg_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_2_to_5
      , AVG(CASE when roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_2_to_5
      --- USAGE_LEVEL = '5_or_more'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' THEN roster.user_id END) AS count_of_rostered_students_5_or_more
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '5_or_more' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' THEN growth.initial_level END) AS initial_avg_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' THEN growth.current_level END) AS current_avg_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_5_or_more
      , AVG(CASE when roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_5_or_more
  FROM roster
  LEFT JOIN {{ schema }}.olaf_cagg_student_month_growth growth
  USING (day_key, user_id, district_id, student_grade, academic_mapping_id, start_year)
  GROUP BY
        roster.day_key
      , roster.is_screener
      , roster.start_year
      , roster.academic_mapping_id
      , roster.subject_id
      , roster.district_id
      , roster.school_id
      , roster.customer_id
      , roster.max_processed_day_key
;


  ---------------------------------------------------------------------------------
  -- PROCESS : INSERT INTO {{ schema }}.olaf_cagg_school_teacher_grade_month_growth
  -- CONTEXT : As specified by OLAF requirements, write each metric for district, student_grade, school, customer
   ---------------------------------------------------------------------------------
  INSERT INTO {{ schema }}.olaf_cagg_school_teacher_grade_month_growth
  WITH roster AS (
    SELECT DISTINCT
          day_key
        , max_processed_day_key
        , is_screener
        , start_year
        , subject_id
        , user_id
        , district_id
        , student_grade
        , academic_mapping_id
        , usage_level
        , school_id
        , customer_id
    FROM temp_school_teacher_roster
  )
  SELECT
        roster.day_key
      , TO_DATE(roster.day_key,'yyyymmdd') AS calendar_date
      , roster.max_processed_day_key
      , roster.is_screener
      , roster.start_year
      , roster.academic_mapping_id
      , roster.subject_id
      , roster.district_id
      , roster.school_id
      , roster.customer_id
      , roster.student_grade
      , COUNT(DISTINCT roster.user_id) AS count_of_rostered_students
      , COUNT(DISTINCT CASE WHEN growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students
      , AVG(growth.initial_level) AS initial_avg_level
      , AVG(growth.current_level) AS current_avg_level
      , AVG(CASE WHEN (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level
      , AVG(CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) AS initial_avg_level_near_level
      , AVG(CASE WHEN (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level
      , AVG(CASE WHEN (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level
      , AVG(CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level
      , AVG(CASE WHEN (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level
      , COUNT(DISTINCT CASE WHEN (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level
      , COUNT(DISTINCT CASE WHEN (current_level - current_target_level) >= 0 then growth.user_id END) AS current_student_count_at_above_level
      --- USAGE_LEVEL = '0_to_2'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' THEN roster.user_id END) AS count_of_rostered_students_0_to_2
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '0_to_2' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' THEN growth.initial_level END) AS initial_avg_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' THEN growth.current_level END) AS current_avg_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_0_to_2
      , AVG(CASE when roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_0_to_2
      , AVG(CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_0_to_2
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '0_to_2' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_0_to_2
      --- USAGE_LEVEL = '2_to_5'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' THEN roster.user_id END) AS count_of_rostered_students_2_to_5
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '2_to_5' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' THEN growth.initial_level END) AS initial_avg_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' THEN growth.current_level END) AS current_avg_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_2_to_5
      , AVG(CASE when roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_2_to_5
      , AVG(CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_2_to_5
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '2_to_5' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_2_to_5
      --- USAGE_LEVEL = '5_or_more'
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' THEN roster.user_id END) AS count_of_rostered_students_5_or_more
      , COUNT(DISTINCT CASE WHEN growth.usage_level = '5_or_more' AND growth.current_level IS NOT NULL THEN growth.user_id END) AS count_of_placed_students_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' THEN growth.initial_level END) AS initial_avg_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' THEN growth.current_level END) AS current_avg_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN initial_level END) AS initial_avg_level_below_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN initial_level END) initial_avg_level_near_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN initial_level END) AS initial_avg_level_at_above_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN current_level END) AS current_avg_level_below_level_5_or_more
      , AVG(CASE when roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN current_level END) AS current_avg_level_near_level_5_or_more
      , AVG(CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN current_level END) AS current_avg_level_at_above_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) < -0.5 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_below_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) >= -0.5 AND (initial_level - initial_target_level) < 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_near_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (initial_level - initial_target_level) >= 0 AND growth.initial_level IS NOT NULL THEN growth.user_id END) AS initial_student_count_at_above_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) < -0.5 THEN growth.user_id END) AS current_student_count_below_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= -0.5 AND (current_level - current_target_level) < 0 THEN growth.user_id END) AS current_student_count_near_level_5_or_more
      , COUNT(DISTINCT CASE WHEN roster.usage_level = '5_or_more' AND (current_level - current_target_level) >= 0 THEN growth.user_id END) AS current_student_count_at_above_level_5_or_more
  FROM roster
  LEFT JOIN {{ schema }}.olaf_cagg_student_month_growth growth
  USING (day_key, user_id, district_id, student_grade, academic_mapping_id, start_year)
  GROUP BY
        roster.day_key
      , roster.is_screener
      , roster.start_year
      , roster.academic_mapping_id
      , roster.subject_id
      , roster.district_id
      , roster.school_id
      , roster.customer_id
      , roster.student_grade
      , roster.max_processed_day_key
;

-- Delete pre-screener data
DELETE FROM {{ schema }}.olaf_cagg_district_month_growth
WHERE
  EXISTS (
    SELECT 1
    FROM {{ schema }}.dim_district_dates AS dd
    WHERE
          {{ schema }}.olaf_cagg_district_month_growth.district_id = dd.district_id
      AND {{ schema }}.olaf_cagg_district_month_growth.start_year  = dd.start_year
      AND {{ schema }}.olaf_cagg_district_month_growth.is_screener <> dd.is_screener
  )
;

DELETE FROM {{ schema }}.olaf_cagg_class_month_growth
WHERE
  EXISTS (
    SELECT 1
    FROM {{ schema }}.dim_district_dates AS dd
    WHERE
          {{ schema }}.olaf_cagg_class_month_growth.district_id = dd.district_id
      AND {{ schema }}.olaf_cagg_class_month_growth.start_year  = dd.start_year
      AND {{ schema }}.olaf_cagg_class_month_growth.is_screener <> dd.is_screener
  )
;

DELETE FROM {{ schema }}.olaf_cagg_class_grade_month_growth
WHERE
  EXISTS (
    SELECT 1
    FROM {{ schema }}.dim_district_dates AS dd
    WHERE
          {{ schema }}.olaf_cagg_class_grade_month_growth.district_id = dd.district_id
      AND {{ schema }}.olaf_cagg_class_grade_month_growth.start_year  = dd.start_year
      AND {{ schema }}.olaf_cagg_class_grade_month_growth.is_screener <> dd.is_screener
  )
;

DELETE FROM {{ schema }}.olaf_cagg_grade_month_growth
WHERE
  EXISTS (
    SELECT 1
    FROM {{ schema }}.dim_district_dates AS dd
    WHERE
          {{ schema }}.olaf_cagg_grade_month_growth.district_id = dd.district_id
      AND {{ schema }}.olaf_cagg_grade_month_growth.start_year  = dd.start_year
      AND {{ schema }}.olaf_cagg_grade_month_growth.is_screener <> dd.is_screener
  )
;

DELETE FROM {{ schema }}.olaf_cagg_school_month_growth
WHERE
  EXISTS (
    SELECT 1
    FROM {{ schema }}.dim_district_dates AS dd
    WHERE
          {{ schema }}.olaf_cagg_school_month_growth.district_id = dd.district_id
      AND {{ schema }}.olaf_cagg_school_month_growth.start_year  = dd.start_year
      AND {{ schema }}.olaf_cagg_school_month_growth.is_screener <> dd.is_screener
  )
;

DELETE FROM {{ schema }}.olaf_cagg_school_grade_month_growth
WHERE
  EXISTS (
    SELECT 1
    FROM {{ schema }}.dim_district_dates AS dd
    WHERE
          {{ schema }}.olaf_cagg_school_grade_month_growth.district_id = dd.district_id
      AND {{ schema }}.olaf_cagg_school_grade_month_growth.start_year  = dd.start_year
      AND {{ schema }}.olaf_cagg_school_grade_month_growth.is_screener <> dd.is_screener
  )
;

DELETE FROM {{ schema }}.olaf_cagg_school_teacher_month_growth
WHERE
  EXISTS (
    SELECT 1
    FROM {{ schema }}.dim_district_dates AS dd
    WHERE
          {{ schema }}.olaf_cagg_school_teacher_month_growth.district_id = dd.district_id
      AND {{ schema }}.olaf_cagg_school_teacher_month_growth.start_year  = dd.start_year
      AND {{ schema }}.olaf_cagg_school_teacher_month_growth.is_screener <> dd.is_screener
  )
;

DELETE FROM {{ schema }}.olaf_cagg_school_teacher_grade_month_growth
WHERE
  EXISTS (
    SELECT 1
    FROM {{ schema }}.dim_district_dates AS dd
    WHERE
          {{ schema }}.olaf_cagg_school_teacher_grade_month_growth.district_id = dd.district_id
      AND {{ schema }}.olaf_cagg_school_teacher_grade_month_growth.start_year  = dd.start_year
      AND {{ schema }}.olaf_cagg_school_teacher_grade_month_growth.is_screener <> dd.is_screener
  )
;

END TRANSACTION;

ANALYZE {{ schema }}.olaf_cagg_district_month_growth;
ANALYZE {{ schema }}.olaf_cagg_class_month_growth;
ANALYZE {{ schema }}.olaf_cagg_class_grade_month_growth;
ANALYZE {{ schema }}.olaf_cagg_grade_month_growth;
ANALYZE {{ schema }}.olaf_cagg_school_month_growth;
ANALYZE {{ schema }}.olaf_cagg_school_grade_month_growth;
ANALYZE {{ schema }}.olaf_cagg_school_teacher_month_growth;
ANALYZE {{ schema }}.olaf_cagg_school_teacher_grade_month_growth;
