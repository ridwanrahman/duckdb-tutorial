import duckdb


def create_table(con):



def main() -> None:
    con = duckdb.connect("bank_data.duckdb")
    create_table()
    print("connection worked fine")
    con.execute("""
                CREATE TABLE IF NOT EXISTS bank AS
                SELECT * FROM read_csv('bank-marketing.csv')
    """)
    con.execute("SHOW ALL TABLES").fetchdf()


if __name__ == "__main__":
    main()
