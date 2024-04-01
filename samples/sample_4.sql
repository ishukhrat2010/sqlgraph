  ---------------------------------------------------------------------------------
  -- PROCESS : CREATE TABLE temp_school_teacher_roster
  -- CONTEXT : Create table containing school_id and customer_id
  ---------------------------------------------------------------------------------

  INSERT INTO schema.target_table
  WITH cte_1 AS (
    SELECT DISTINCT
        col_1
        , col_2
    FROM source_table_1
  ),
  cte_2 AS (
    SELECT DISTINCT
        col_3
        , col_4
    FROM source_table_2
  )
  SELECT
    ct.*
    , s2.*
  FROM cte_1 AS ct1
  LEFT JOIN cte_2 AS ct2 on ct1.col_1 = ct2.col_3
  LEFT JOIN source_table_3 as s2 on s2.source_id = ct1.col_2
;
