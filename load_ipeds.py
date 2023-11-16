import pandas as pd
import psycopg
import sys
import re
import os
import credentials


def connect_to_database():
    """
    Connect to the PostgreSQL database.

    Returns:
        A tuple containing the connection and cursor objects.
    """
    conn = psycopg.connect(
        host="pinniped.postgres.database.azure.com",
        dbname="xwu5",
        user=credentials.DB_USER,
        password=credentials.DB_PASSWORD
    )

    cur = conn.cursor()

    return conn, cur


def clean_csv(csv_file_path):
    df = pd.read_csv(csv_file_path, encoding='ISO-8859-1')

    # Replace empty content with 'NULL'
    df = df.fillna('NULL')

    # Extract the year from the file name
    year_match = re.search(r'\d{4}', csv_file_path)
    year = year_match.group(0) if year_match else 'Unknown'

    # Character variable: remove special characters
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].replace('[^\w\s]', '', regex=True)

    # Numeric variable: remove special characters and alphabet
    for col in df.select_dtypes(include=['int64', 'float64']).columns:
        df[col] = df[col].astype(str).replace('[^\d\.]', '', regex=True).astype(df[col].dtype)

    return df


def create_table(df, table_name, conn):

    if not isinstance(df, pd.DataFrame):
        raise ValueError("df is not a DataFrame in create_table function")

    if df.empty:
        raise ValueError("df is empty in create_table function")

    cur = conn.cursor()

    # check whether the table alreadly existed
    cur.execute(f"SELECT EXISTS(SELECT FROM pg_tables WHERE tablename = '{table_name}');")
    if cur.fetchone()[0]:
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")

    # create a table
    create_table_query = f"CREATE TABLE {table_name} ("
    for col, dtype in df.dtypes.items():
        if dtype == 'int64':
            sql_dtype = 'INTEGER'
        elif dtype == 'float64':
            sql_dtype = 'FLOAT'
        elif dtype == 'object':
            sql_dtype = 'TEXT'
        else:
            sql_dtype = 'TEXT'
        create_table_query += f"{col} {sql_dtype}, "
    create_table_query = create_table_query[:-2] + ");"

    cur.execute(create_table_query)
    conn.commit()

    inserted_rows = 0
    rejected_rows = 0

    # insert data
    try:
        df.to_sql(name=table_name, con=conn, if_exists='append', index=False)
        inserted_rows = len(df)
    except Exception as e:
        # reject the count if failed to insert data
        rejected_rows = len(df)
        print(f"Error inserting data: {e}")

    cur.close()
    return inserted_rows, rejected_rows


def main(csv_file_path):
    # clean the data
    df = clean_csv(csv_file_path)

    # connect to database
    conn, _ = connect_to_database()

    table_name = os.path.basename(csv_file_path).split('.')[0] + "_new"

    if not isinstance(df, pd.DataFrame):
        raise ValueError("df is not a DataFrame in main function before calling create_table")

    # create a table and get inserted and rejected rows count
    inserted_rows, rejected_rows = create_table(df, table_name, conn)

    # close the connection
    conn.close()

    # Data summary
    print(f"Total rows read from CSV: {len(df)}")
    print(f"Total rows successfully inserted: {inserted_rows}")
    print(f"Total rows rejected: {rejected_rows}")


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python load-ipeds.py <csv_file_path>")
        sys.exit(1)

    csv_file_path = sys.argv[1]
    main(csv_file_path)
