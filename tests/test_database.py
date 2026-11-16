import sqlite3

def main():
    database_path = "data/output/streampulse.db"

    connection = sqlite3.connect(database_path)

    query = """
    SELECT name
    FROM sqlite_master
    WHERE type = 'table'
    ORDER BY name
    """

    tables = connection.execute(query).fetchall()

    print("Database verification results:")
    for table in tables:
        print("-", table[0])

    connection.close()


if __name__ == "__main__":
    main()