import re

##This run only after transpile is done!


def execute(databricks_ddl):
    # Get the database_name.table_name
    tableName_pattern = r'(CREATE TABLE IF NOT EXISTS)\s+(\S+)\s*[\s\(]'
    tableName_match = re.search(tableName_pattern, databricks_ddl)
    ##check if it's table since defaults can be set only on tables
    if tableName_match is None:
        return databricks_ddl
    else:
        db_tableName = tableName_match.group(2)
        # databricks_ddl = databricks_ddl.replace(';','')
        if '`' in databricks_ddl:
            databricks_ddl = (
                databricks_ddl
                + "TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'enabled', 'delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5');"
            )
        else:
            databricks_ddl = databricks_ddl + "TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'enabled');"

        columnName_pattern = re.compile(r'^\s*(\w+)(?:(?!\bCOMMENT \'TIME\b_).)*\bCOMMENT \'TIME\b', re.MULTILINE)
        columnName_match = columnName_pattern.findall(databricks_ddl)

        for i in range(len(columnName_match)):
            constraint_name = f"CHK_TIME_NTZ_COL_{i}"
            databricks_ddl = (
                databricks_ddl
                + f"\nALTER TABLE {db_tableName} ADD CONSTRAINT {constraint_name} CHECK (({columnName_match[i]} IS NULL) OR CAST ({columnName_match[i]} AS DATE) = '0000-01-01');"
            )

        return databricks_ddl
