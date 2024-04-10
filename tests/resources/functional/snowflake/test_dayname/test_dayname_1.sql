
-- snowflake sql:
SELECT DAYNAME(TO_TIMESTAMP('2015-04-03 10:00:00')) AS MONTH;

-- databricks sql:
SELECT DATE_FORMAT(cast('2015-04-03 10:00:00' as timestamp), 'E') AS MONTH;
