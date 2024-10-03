-- snowflake sql:
SELECT OBJECT_CONSTRUCT('Key_One', PARSE_JSON('NULL'), 'Key_Two', NULL, 'Key_Three', 'null') as obj;

-- databricks sql:
SELECT STRUCT(PARSE_JSON('NULL') AS Key_One, NULL AS Key_Two, 'null' AS Key_Three) AS obj;
