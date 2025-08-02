import duckdb
import sqlite3
from typing import Tuple, List


DATABASE_PATH = "data_files/index.db"
DUCK_DB_PATH = "data_files/index.duckdb"

def get_table_names() -> List[str]:
    """
    Connect to sqlite database and discovver all tables and their structures
    """
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        print(f"Connected to database: {DATABASE_PATH}")
        print("="*50)

        # Get all table names
        cursor.execute("""
                       SELECT name
                       FROM sqlite_master
                       WHERE type = 'table'
                         AND name NOT LIKE 'sqlite_%'
                       """)
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]
        return table_names

    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
    except FileNotFoundError:
        print(f"Database file not found: {DATABASE_PATH}")
        return


def get_sqlite_table_schema(table_name: str) -> List[Tuple[str, str]]:
    schema = []
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        schema = [(row[1], row[2] )for row in cursor.fetchall()]

    return schema

def create_duckdb_table_from_sqlite_schema(schema: List[Tuple[str, str]], table_name: str):
    # duckdb type mapping
    type_mapping = {
        "INTEGER": "INTEGER",
        "TEXT": "VARCHAR",
        "REAL": "DOUBLE",
        "BLOB": "BLOB",
        "NUMERIC": "DECIMAL",
        "VARCHAR": "VARCHAR",
        "DATETIME": "TIMESTAMP",
        "DATE": "DATE",
    }

    columns = []
    for col_name, col_type in schema:
        duck_type = type_mapping[col_type]
        columns.append(f"{col_name} {duck_type}")

    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
    duck_conn = duckdb.connect(DUCK_DB_PATH)
    duck_conn.execute(create_sql)
    duck_conn.close()
    print(f"Created DuckDB table: {table_name}")

def stream_sqlite_table_data(table_name):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_rows = cursor.fetchone()[0]
        print(f"Streaming {total_rows:,} rows from {table_name}")

        # Stream data in batches
        offset = 0
        batch_size = 50000
        while True:
            cursor.execute(f"""
                    SELECT * FROM {table_name} 
                    LIMIT {batch_size} OFFSET {offset}
                """)

            batch = cursor.fetchall()
            if not batch:
                break

            print(f"Yielding batch: rows {offset + 1} to {offset + len(batch)}")
            yield batch

            offset += batch_size

    finally:
        conn.close()


def migrate_sqlite_to_duckdb(table_schema, table_name):
    """
    Migrate data from SQlite to duckdb
    """
    duck_conn = duckdb.connect(DUCK_DB_PATH)

    try:
        # Prepare insert statement
        placeholders = ', '.join(['?' for _ in table_schema])
        insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

        total_inserted = 0

        for batch in stream_sqlite_table_data(table_name):
            # Insert batch into DuckDB
            duck_conn.executemany(insert_sql, batch)
            total_inserted += len(batch)

            print(f"Inserted {len(batch)} rows (total: {total_inserted:,})")

        # Commit transaction
        print("\n")
        duck_conn.commit()

        result = duck_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        print(f"\nMigration complete! DuckDB table {table_name} now has {result[0]:,} rows")

    finally:
        duck_conn.close()

def delete_data_from_duckdb(table_name: str):
    try:
        duck_conn = duckdb.connect(DUCK_DB_PATH)
        duck_conn.execute(f"DROP TABLE IF EXISTS {table_name};")
        duck_conn.commit()
        print(f"All data deleted from DuckDB table {table_name}")
    except Exception as e:
        print(f"Error deleting data: {e}")
    finally:
        duck_conn.close()

def deleter():
    table_names = get_table_names()
    for table_name in table_names:
        delete_data_from_duckdb(table_name)

def runner():
    table_names = get_table_names()
    for table_name in table_names:
        table_schema = get_sqlite_table_schema(table_name)
        create_duckdb_table_from_sqlite_schema(table_schema, table_name)
        migrate_sqlite_to_duckdb(table_schema, table_name)


if __name__ == "__main__":
    # runner()
    deleter()

