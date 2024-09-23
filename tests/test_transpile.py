from sqlglot import parse, transpile
from sqlglot.expressions import Create
import re
from databricks.labs.remorph.snow.databricks import Databricks
from databricks.labs.remorph.snow.teradata import Teradata


def decompose(node):
    print(f"{node} is a {type(node)} object")
    if type(node) == Create:
        print(node)

    return node


def identify_root(node):
    """
    Function used in a transform to find the root SQL command to identify the type of statement being executed
    :param node:
    :return type of node:
    """
    if node.parent is None:
        return type(node)
    else:
        pass


def print_sep(msg="", sep="**"):
    print("\n")
    print(msg)
    print(sep * 40)


# def test(node):
#     if node.parent is None and type(node) == Create:
#         print(node)
#         return node
#     else:
#         pass


def print_transpiled(sql):
    print_sep()
    print_sep("Transpiling Code...")
    # transpiled_sql = ""
    transpiled_sql = transpile(sql, read=Teradata, write=Databricks, pretty=True, error_level=None)

    expression_tree = parse(sql=sql, read='teradata')

    print("******************************************************************")

    print("******************************************************************")
    print(repr(expression_tree))
    print("******************************************************************")
    for sql in transpiled_sql:
        print(sql)


def pretty_print_exp(expr):
    return print(expr)


def read_file(filename):
    with open(filename, 'r') as file:
        return file.read()


def replace_order_by_values(sql):
    # This regex will match "ORDER BY VALUES ( depDate )" and capture "depDate"
    pattern = r"ORDER BY VALUES \(\s*(\w+)\s*\)"
    replacement = r"ORDER BY \1"
    return re.sub(pattern, replacement, sql)


def execute():
    """

    :rtype: object
    """
    "COLUMNCONSTRAINT"
    "CHARACTERSETCOLUMNCONSTRAINT"
    "CASESPECIFICCOLUMNCONSTRAINT"
    "TitleColumnConstraint"

    sql = """
    CREATE SET TABLE GTM_WORK_PDB.FUEL_PER_LH_TRN_SEG_ACTL
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    ( GTM_YEAR INTEGER,
    STATED_DIR_TONS TIMESTAMP(6),
    REVERSE_DIR_TONS BIGINT,
    TOTAL_TONS BIGINT,
    TD_LD_TS TIMESTAMP(0) WITH TIME ZONE)
    PRIMARY INDEX ( FROM_MSTR_LINE_NM );


     """

    # sql="""delete from table a where a.id in (10,20)"""

    # "../td_input/DB_IDM.LOCATION_HIER_LEVEL.sql" DB_IDM.AIRLINE.sql
    file_name = "../td_scripts/INT_IDM_INVENTORY/ETL_PROD/invent.795.DELETE_IDM_TABLES.sql"

    sql = (
        read_file(file_name).replace("MAP = TD_MAP1", "")
        # .replace("COMPRESS ,", "COMPRESS '',")
        # .replace("COMPRESS )", "COMPRESS '')")
    )
    sql = replace_order_by_values(sql)
    print("******* Before *******")
    # print(sql)
    print_transpiled(sql)
    # print(parse_one(sql))


execute()
