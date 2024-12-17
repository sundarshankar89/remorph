-- snowflake sql:
SELECT
  ARRAY_FLATTEN([[1, 2, 3], [4], [5, 6]]) AS COL
, ARRAY_FLATTEN([[[1, 2], [3]], [[4], [5]]]) AS COL1
, ARRAY_FLATTEN([[1, 2, 3], NULL, [5, 6]]) AS COL3;

-- databricks sql:
SELECT
  FLATTEN(ARRAY(ARRAY(1, 2, 3) , ARRAY(4) , ARRAY(5, 6))) AS COL,
  FLATTEN(ARRAY(ARRAY(ARRAY(1, 2) , ARRAY(3)) , ARRAY(ARRAY(4) , ARRAY(5)))) AS COL1,
  FLATTEN(ARRAY(ARRAY(1, 2, 3) , NULL, ARRAY(5, 6))) AS COL3;