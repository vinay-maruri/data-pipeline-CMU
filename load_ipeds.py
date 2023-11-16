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
    df = pd.read_csv(csv_file_path)

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

    cur = conn.cursor()

    # create a table
    create_table_query = f"CREATE TABLE {table_name} ("

    # add sql type to each column
    for col, dtype in df.dtypes.iteritems():
        if dtype == 'int64':
            sql_dtype = 'INTEGER'
        elif dtype == 'float64':
            sql_dtype = 'FLOAT'
        elif dtype == 'object':
            sql_dtype = 'TEXT'
        else:
            sql_dtype = 'TEXT'
        create_table_query += f"{col} {sql_dtype}, "

    # finish sql
    create_table_query = create_table_query[:-2] + ");"

    # commit change and close
    cur.execute(create_table_query)
    conn.commit()
    cur.close()


def main(csv_file_path):

    # clean the data
    df = clean_csv(csv_file_path)

    # connect to database
    conn, _ = connect_to_database()

    table_name = os.path.basename(csv_file_path).split('.')[0]

    # create a table
    create_table(df, table_name, conn)

    # close the connection
    conn.close()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python load-ipeds.py <csv_file_path>")
        sys.exit(1)

    csv_file_path = sys.argv[1]
    main(csv_file_path)