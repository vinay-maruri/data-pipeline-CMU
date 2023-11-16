
import pandas as pd
import numpy as np
import psycopg
import sys
import credentials
import csv


def connect_to_database():
    """
    Connect to the PostgreSQL database.

    Returns:
        A tuple containing the connection and cursor objects.
    """
    conn = psycopg.connect(
        host="pinniped.postgres.database.azure.com",
        dbname="shauck",
        user=credentials.DB_USER,
        password=credentials.DB_PASSWORD
    )

    cur = conn.cursor()

    return conn, cur


def clean_csv(csv_file_path):
    file = pd.read_csv(csv_file_path, dtype={1729: 'object',
                                             1909: 'object',
                                             1910: 'object',
                                             1911: 'object',
                                             1912: 'object',
                                             1913: 'object',
                                             2376: 'object',  # should be float64
                                             2377: 'object',  # should be float64
                                             2958: 'object'})  # should be int64

    # Extract year from csv_file_path
    year1 = csv_file_path.split('_')[0].replace('MERGED', '')
    year2 = str(int(year1) + 1)

    # Add year as a new column and convert to date data type
    file['year1'] = pd.to_datetime(year1, format='%Y')
    file['year2'] = pd.to_datetime(year2, format='%Y')

    # Remove alphabet and punctuation from numeric columns
    numeric_cols = file.select_dtypes(include=['int64', 'float64']).columns
    file[numeric_cols] = file[numeric_cols].replace('[^0-9\.]+', '',
                                                    regex=True)

    # Remove symbols and punctuation from object columns
    object_cols = file.select_dtypes(include=['object']).columns
    file[object_cols] = file[object_cols].replace('[^0-9a-zA-Z]+', '',
                                                  regex=True)

    return file


def split_dataframe(df):
    # Get the number of columns in the dataframe
    num_cols = df.shape[1]

    # Calculate the number of columns to keep in each dataframe
    num_cols_per_df = int(np.ceil((num_cols - 1) / 10))

    # Create a list of dataframes
    dfs = []

    # Split the dataframe into three dataframes
    for i in range(10):
        start_col = i * num_cols_per_df
        end_col = min((i + 1) * num_cols_per_df, num_cols - 1)
        if i > 0:
            cols = ['UNITID'] + list(df.columns[start_col:end_col])
        else:
            cols = list(df.columns[start_col:end_col])
        dfs.append(df[cols])

    return dfs


def get_column_types(df):
    """
    Get a list of all int64 columns, a list of all float64 columns, and a list of all object columns.

    Args:
        df: The pandas dataframe.

    Returns:
        A tuple containing three lists: a list of all int64 columns, a list of all float64 columns, and a list of all object columns.
    """
    # List of int64 columns
    int_cols = df.select_dtypes(include='int64').columns.tolist()

    # List of float64 columns
    float_cols = df.select_dtypes(include='float64').columns.tolist()

    # List of object columns
    object_cols = df.select_dtypes(include='object').columns.tolist()

    return int_cols, float_cols, object_cols


def create_tables(df):
    """
    Create a table in the PostgreSQL database for all columns in each dataframe.

    Args:
        csv_file_path: The file path of the CSV file.

    Returns:
        None.
    """
    # Connect to the database
    conn, cur = connect_to_database()

    # Split the dataframe into three dataframes
    dfs = split_dataframe(df)

    # Create a table for each dataframe with all columns for the dataframe
    for i, df in enumerate(dfs):
        columns = []
        int_cols, float_cols, object_cols = get_column_types(df)

        for col in df.columns:
            if str(col) in int_cols:
                columns.append(f'{col} INTEGER')
            elif str(col) in float_cols:
                columns.append(f'{col} FLOAT')
            else:
                columns.append(f'{col} VARCHAR(255)')
        columns_str = ', '.join(columns)

        # Drop the table if it exists
        cur.execute(f"DROP TABLE IF EXISTS scorecard_{i}")

        # Create the table
        cur.execute(f'CREATE TABLE scorecard_{i} ({columns_str})')

    # Commit changes and close the connection
    conn.commit()
    conn.close()


def insert_rows(df):
    df = df.iloc[:20]

    # Connect to the database
    conn, cur = connect_to_database()

    # Initialize counters
    total_rows = 0
    inserted_rows = 0
    rejected_rows = 0

    # Initialize the CSV writer for rejected rows
    rejected_csv = csv.writer(open('rejected_rows.csv', 'w'))
    rejected_csv.writerow(['error_message', 'row'])

    # Split the dataframe into three dataframes
    dfs = split_dataframe(df)

    for i, df in enumerate(dfs):

        # Create a list of placeholders for the SQL query
        placeholders = ','.join(['%s'] * len(df.columns))

        # Create a list of tuples containing the values for each row
        values = [tuple(x) for x in df.values]
        

        # Insert the rows into the database
        try:
            cur.executemany(f'INSERT INTO scorecard_{i} VALUES ({placeholders})', values)
            inserted_rows += len(values)
        except psycopg.Error as e:
            # If the row is invalid, print an error message and write it to the rejected CSV file
            print(f'Error inserting rows into scorecard_{i}: {e}')
            for index, row in enumerate(values):
                print(f'Error inserting row {index}: {e}')
                rejected_csv.writerow([str(e), row])
            rejected_rows += len(values)
   
    # Commit changes and close the connection
    conn.commit()
    conn.close()

    # Print a summary of the results
    print(f'Total rows: {total_rows}')
    print(f'Inserted rows: {inserted_rows}')
    print(f'Rejected rows: {rejected_rows}')


cleaned = clean_csv('MERGED2018_19_PP.csv')
create_tables(cleaned)

insert_rows(cleaned)


#if __name__ == '__main__':
   # filename = sys.argv[1]
   # clean_csv(filename)
