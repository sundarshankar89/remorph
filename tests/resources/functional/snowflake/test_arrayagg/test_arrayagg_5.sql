
-- snowflake sql:

              SELECT
                col2,
                ARRAYAGG(col4) WITHIN GROUP (ORDER BY col3)
              FROM test_table
              WHERE col3 > 450000
              GROUP BY col2
              ORDER BY col2 DESC;

-- databricks sql:

            SELECT
              col2,
              TRANSFORM(ARRAY_SORT(ARRAY_AGG(NAMED_STRUCT('value', col4, 'sort_by', col3)),
                  (left, right) -> CASE
                                          WHEN left.sort_by < right.sort_by THEN -1
                                          WHEN left.sort_by > right.sort_by THEN 1
                                          ELSE 0
                                      END), s -> s.value)
            FROM test_table
            WHERE
              col3 > 450000
            GROUP BY
              col2
            ORDER BY
              col2 DESC NULLS FIRST;
