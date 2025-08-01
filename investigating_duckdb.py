import duckdb
import pyarrow.csv as csv
import pyarrow.parquet as pq


def generate_parquet_file_using_csv():
    """
    Pyarrow ->  it is an in-memory columnar data format that is used to exchange data between systems.
    """
    table = csv.read_csv("csv_file.csv")
    pq.write_table(table, 'csv_file.parquet', compression='snappy')

    print("Parquet file created with Apache arrow")
    print(f"Schema: {table.schema}")
    print(f"Rows: {table.num_rows}")


def make_duck_db_query():
    """
    This  is running the querries using an in-memory database that is stored
    globally inside the python module.
    The result of the query is returned as a Relation. Which is a symbolic
    representation of the query. The query is not executed until the result
    is fetched or requested to be printed to the screeen.
    """
    r1 = duckdb.sql("SELECT 42 as i")
    duckdb.sql("SELECT i*2 AS k FROM r1").show()

def ingest_csv_file():
    file = duckdb.read_csv("csv_file.csv")
    # parquet_file = duckdb.read_csv("csv_file.parquet")
    print(file)

def persistent_storage():
    """
    Any data written to this connection will be persisted to disk
    """
    con = duckdb.connect("file.db")
    con.sql("CREATE TABLE IF NOT EXISTS test (i INTEGER)")
    con.sql("INSERT INTO test VALUES (1), (2), (3)")

    # query the table
    con.table("test").show()
    # explicitly close the connection
    con.close()


if __name__ == "__main__":
    # generate_parquet_file_using_csv()
    # ingest_csv_file()
    persistent_storage()

