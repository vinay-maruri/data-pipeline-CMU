import pandas as pd
import psycopg
import sys
import re
import credentials


def connect_to_database():
    # Establishing a connection to the database
    conn = psycopg.connect(
        host = "pinniped.postgres.database.azure.com",
        dbname = credentials.DB_USER,
        user=credentials.DB_USER,
        password=credentials.DB_PASSWORD
    )

    cur = conn.cursor()
    return conn,cur


def read_csv(filename):
    # Extracting year from the filename using regular expression
    year = re.findall(r'\d{4}', filename)[0]
    df = pd.read_csv(filename, encoding='ISO-8859-1', na_values=['', -999])
    # Selecting specific variables from the dataframe
    df = df[['UNITID', 'INSTNM', 'ADDR', 'CONTROL', 'CCBASIC', 'LATITUDE', 'LONGITUD']]
    df = df.replace(-999, None)
    # Adding a 'year' column to the dataframe
    df['year'] = year
    return df, year


def data_type(pandas_dtype):
    # Mapping pandas dtypes to SQL data types
    if pd.api.types.is_integer_dtype(pandas_dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(pandas_dtype):
        return "FLOAT"
    else:
        return "TEXT"


def create_table(df, table_name, conn, cur):
    # Creating SQL column definitions based on dataframe dtypes
    columns = [f"{col} {data_type(df[col].dtype)}" for col in df.columns]
    columns_str = ', '.join(columns)
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    cur.execute(f'CREATE TABLE {table_name} ({columns_str});')
    conn.commit()


def insert_data(df, table_name, conn, cur):
    total_rows = len(df)
    inserted_rows = 0
    failed_rows = 0
    failed_data = []

    # Replacing pandas NA values with None for SQL compatibility
    df = df.replace({pd.NA: None})
    try:
        for index, row in df.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            columns = ', '.join(row.index)
            sql = (
                f"INSERT INTO {table_name} "
                f"({columns}) "
                f"VALUES ({placeholders})"
            )
            try:
                cur.execute(sql, list(row.values))
                inserted_rows += 1
            except Exception as e:
                print(f"Error in row {index}: {e}")
                failed_rows += 1
                failed_data.append(row)
                continue
        conn.commit()
    except Exception as e:
        print(f"Error during insertion: {e}")
        conn.rollback()
        inserted_rows = 0
        failed_rows = total_rows

    # Writing failed data rows to a CSV file
    if failed_rows > 0:
        pd.DataFrame(failed_data).to_csv('failed_data.csv', index=False)

    return total_rows, inserted_rows, failed_rows


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python load-ipeds.py <filename>")
        sys.exit(1)

    filename = sys.argv[1]
    df, year = read_csv(filename)
    table_name = f'ipeds_{year}'
    conn, cur = connect_to_database()
    create_table(df, table_name, conn, cur)
    total_rows, inserted_rows, failed_rows = insert_data(df, table_name, conn, cur)
    print(f"Total rows read from CSV: {total_rows}")
    print(f"Total rows successfully inserted: {inserted_rows}")
    print(f"Total rows failed to insert: {failed_rows}")

    cur.close()
    conn.close()

