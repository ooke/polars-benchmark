from __future__ import annotations

import sys
import argparse
from pathlib import Path

import pyexasol

from settings import Settings

settings = Settings()

DDL = {
    "nation": """
CREATE TABLE IF NOT EXISTS nation (
    n_nationkey  INTEGER NOT NULL,
    n_name       CHAR(25) NOT NULL,
    n_regionkey  INTEGER NOT NULL,
    n_comment    VARCHAR(152)
);
""",
    "region": """
CREATE TABLE IF NOT EXISTS region (
    r_regionkey  INTEGER NOT NULL,
    r_name       CHAR(25) NOT NULL,
    r_comment    VARCHAR(152)
);
""",
    "part": """
CREATE TABLE IF NOT EXISTS part (
    p_partkey     INTEGER NOT NULL,
    p_name        VARCHAR(55) NOT NULL,
    p_mfgr        CHAR(25) NOT NULL,
    p_brand       CHAR(10) NOT NULL,
    p_type        VARCHAR(25) NOT NULL,
    p_size        INTEGER NOT NULL,
    p_container   CHAR(10) NOT NULL,
    p_retailprice DECIMAL(15,2) NOT NULL,
    p_comment     VARCHAR(23) NOT NULL
);
""",
    "supplier": """
CREATE TABLE IF NOT EXISTS supplier (
    s_suppkey     INTEGER NOT NULL,
    s_name        CHAR(25) NOT NULL,
    s_address     VARCHAR(40) NOT NULL,
    s_nationkey   INTEGER NOT NULL,
    s_phone       CHAR(15) NOT NULL,
    s_acctbal     DECIMAL(15,2) NOT NULL,
    s_comment     VARCHAR(101) NOT NULL
);
""",
    "partsupp": """
CREATE TABLE IF NOT EXISTS partsupp (
    ps_partkey     INTEGER NOT NULL,
    ps_suppkey     INTEGER NOT NULL,
    ps_availqty    INTEGER NOT NULL,
    ps_supplycost  DECIMAL(15,2) NOT NULL,
    ps_comment     VARCHAR(199) NOT NULL
);
""",
    "customer": """
CREATE TABLE IF NOT EXISTS customer (
    c_custkey     INTEGER NOT NULL,
    c_name        VARCHAR(25) NOT NULL,
    c_address     VARCHAR(40) NOT NULL,
    c_nationkey   INTEGER NOT NULL,
    c_phone       CHAR(15) NOT NULL,
    c_acctbal     DECIMAL(15,2) NOT NULL,
    c_mktsegment  CHAR(10) NOT NULL,
    c_comment     VARCHAR(117) NOT NULL
);
""",
    "orders": """
CREATE TABLE IF NOT EXISTS orders (
    o_orderkey       INTEGER NOT NULL,
    o_custkey        INTEGER NOT NULL,
    o_orderstatus    CHAR(1) NOT NULL,
    o_totalprice     DECIMAL(15,2) NOT NULL,
    o_orderdate      DATE NOT NULL,
    o_orderpriority  CHAR(15) NOT NULL,
    o_clerk          CHAR(15) NOT NULL,
    o_shippriority   INTEGER NOT NULL,
    o_comment        VARCHAR(79) NOT NULL
);
""",
    "lineitem": """
CREATE TABLE IF NOT EXISTS lineitem (
    l_orderkey    INTEGER NOT NULL,
    l_partkey     INTEGER NOT NULL,
    l_suppkey     INTEGER NOT NULL,
    l_linenumber  INTEGER NOT NULL,
    l_quantity    DECIMAL(15,2) NOT NULL,
    l_extendedprice  DECIMAL(15,2) NOT NULL,
    l_discount    DECIMAL(15,2) NOT NULL,
    l_tax         DECIMAL(15,2) NOT NULL,
    l_returnflag  CHAR(1) NOT NULL,
    l_linestatus  CHAR(1) NOT NULL,
    l_shipdate    DATE NOT NULL,
    l_commitdate  DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct CHAR(25) NOT NULL,
    l_shipmode     CHAR(10) NOT NULL,
    l_comment      VARCHAR(44) NOT NULL
);
""",
}


SQL_DIR = Path(__file__).resolve().parent.parent / "queries" / "exasol" / "queries"

# Map TPC-H tables to CSV column ranges to skip the trailing empty field.
CSV_COL_RANGES: dict[str, str] = {
    "nation": "1..4",
    "region": "1..3",
    "part": "1..9",
    "supplier": "1..7",
    "partsupp": "1..5",
    "customer": "1..8",
    "orders": "1..9",
    "lineitem": "1..16",
}


def _execute_sql_file(conn: pyexasol.ExaConnection, path: Path) -> None:
    sql = path.read_text()
    sql = sql.replace("tpc", settings.exasol.schema_name)
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for statement in statements:
        if statement.lower().startswith("set autocommit"):
            continue
        conn.execute(statement)


def create_schema_and_tables(conn: pyexasol.ExaConnection) -> None:
    _execute_sql_file(conn, SQL_DIR / "create_schema.sql")


def create_indices(conn: pyexasol.ExaConnection) -> None:
    _execute_sql_file(conn, SQL_DIR / "create_indices_1node.sql")


def analyze_database(conn: pyexasol.ExaConnection) -> None:
    _execute_sql_file(conn, SQL_DIR / "analyze_database.sql")


def import_table(conn: pyexasol.ExaConnection, table: str, data_dir: Path) -> None:
    file_path = data_dir / f"{table}.tbl"
    conn.import_from_file(
        src=str(file_path),
        table=table,
        import_params={
            'column_separator': '|',
            'row_separator': 'LF',
            'csv_cols': [CSV_COL_RANGES[table]],
        }
    )
    conn.execute("COMMIT")


def main(data_dir: str) -> None:
    try:
        conn = pyexasol.connect(
            dsn=settings.exasol.dsn,
            user=settings.exasol.user,
            password=settings.exasol.password,
        )
        conn.set_autocommit(False)
    except Exception as e:
        print(
            f"Error: unable to connect to Exasol at {settings.exasol.dsn} as {settings.exasol.user}: {e}",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        create_schema_and_tables(conn)
        base = Path(data_dir)
        # record expected row counts from source .tbl files
        expected_rows: dict[str, int] = {}
        for table in DDL.keys():
            file_path = base / f"{table}.tbl"
            with open(file_path, 'rb') as f:
                expected_rows[table] = sum(1 for _ in f)
        # load each table from CSV via HTTP import
        for table in DDL.keys():
            import_table(conn, table, base)
        create_indices(conn)
        analyze_database(conn)
        # sanity-check: verify loaded row counts match source files
        for table, exp in expected_rows.items():
            actual = conn.execute(
                f"SELECT COUNT(*) FROM {settings.exasol.schema_name}.{table}"
            ).fetchone()[0]
            if actual != exp:
                raise RuntimeError(
                    f"Table '{table}' has {actual} rows but expected {exp} from source file"
                )
    except Exception as e:
        print(f"Error loading tables into Exasol: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-dir",
        default=str(settings.dataset_base_dir),
        help="Path containing generated .tbl files",
    )
    args = parser.parse_args()
    main(args.data_dir)
