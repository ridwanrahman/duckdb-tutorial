import duckdb

DATABASE_PATH = "data_files/index.db"
DUCK_DB_PATH = "data_files/index.duckdb"


def runner():
    conn = duckdb.connect(DUCK_DB_PATH, read_only=False)

    try:
        conn.execute("INSTALL sqlite_scanner")
        conn.execute("LOAD sqlite_scanner")

        tables_query = f"""
        SELECT name FROM sqlite_scan('{DATABASE_PATH}', 'sqlite_master') 
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        """
        tables = conn.execute(tables_query).fetchall()
        print(f"Migrating {len(tables)} tables from SQLite to DuckDB...")
        for table_tuple in tables:
            table_name = table_tuple[0]

            # Create table in DuckDB by copying structure and data in one command
            copy_query = f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM sqlite_scan('{DATABASE_PATH}', '{table_name}')
            """

            conn.execute(copy_query)

            # Get row count for confirmation
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"✓ Copied table '{table_name}': {count:,} rows")

            print(f"\n🎉 Migration complete! DuckDB database created at: {DUCK_DB_PATH}")

    except Exception as e:
        print(f"Error during migration: {e}")
    finally:
        conn.close()
        print("Connection closed.")


if __name__ == "__main__":
    runner()
