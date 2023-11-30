
import pandas as pd
import credentials
import psycopg
import sys


def connect_to_database():
    """
    Connect to the PostgreSQL database.

    Returns:
        A tuple containing the connection and cursor objects.
    """
    conn = psycopg.connect(
        host="pinniped.postgres.database.azure.com",
        dbname=credentials.DB_NAME,
        user=credentials.DB_USER,
        password=credentials.DB_PASSWORD
    )
    cur = conn.cursor()
    return conn, cur


def read_csv(csv_file_path):
    """
    Read the CSV file and return the data frame.

    Args:
        csv_file_path: The path to the CSV file.

    Returns:
        A data frame containing the CSV data.
    """
    df = pd.read_csv(csv_file_path, encoding='ISO-8859-1', na_values=['', -999])
    return df


def data_type(pandas_dtype):
    # Mapping pandas dtypes to SQL data types
    if pd.api.types.is_integer_dtype(pandas_dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(pandas_dtype):
        return "FLOAT"
    else:
        return "TEXT"


def change_InstitutionInformation(df, year, unitid):
    # Connect to the database
    conn, cur = connect_to_database()

    rowstodelete = []
    rowstoinsert = []
    # Loop through the unitid column
    for index, value in df[unitid].iteritems():
        # Row to be deleted
        query = f"SELECT * FROM institutioninformation WHERE unitid = {value} AND year = {year}"
        cur.execute(query)
        rowstodelete.append(cur.fetchone())
        # Delete the row
        cur.execute(f"DELETE FROM institutioninformation WHERE unitid = {value} AND year = {year}")
        # Row to be inserted
        rowstoinsert.append(df.iloc[index])
        # Insert the row
        cur.execute(f"INSERT INTO institutioninformation VALUES {tuple(rowstoinsert[index])}")

    # Write rows to the CSV file
    df_to_write = pd.DataFrame(rowstodelete, columns=df.columns)
    df_to_write['change'] = 'deleted'
    df_to_write.to_csv('output.csv', index=False, mode='a')

    df_to_write = pd.DataFrame(rowstoinsert, columns=df.columns)
    df_to_write['change'] = 'inserted'
    df_to_write.to_csv('overwritten.csv', index=False, header=False, mode='a')

    conn.commit()
    conn.close()


if __name__ == "__main__":
    # Read the CSV file
    df = read_csv(sys.argv[1])
    # Get the year from the filename
    year = sys.argv[2]
    # Get the unitid column name from the filename
    tabletochange = sys.argv[3]

    if tabletochange == "institutioninformation":
        # Change the InstitutionInformation table
        change_InstitutionInformation(df, year)
