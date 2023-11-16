
import pandas as pd
import numpy as np
import psycopg
import sys
import credentials
import csv
from sqlalchemy import create_engine


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
    file = pd.read_csv(csv_file_path, low_memory=False)

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

    # replace privacy suppressed values with NaN in numeric columns
    numeric_cols = file.select_dtypes(include=['int64', 'float64']).columns
    file[numeric_cols] = file[numeric_cols].replace('PrivacySuppressed', np.nan,
                                                    regex=True)

    # replace privacy suppressed values with NaN in object columns
    numeric_cols = file.select_dtypes(include=['object']).columns
    file[numeric_cols] = file[numeric_cols].replace('PrivacySuppressed', 'Null',
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


def create_tables(df, year):
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
    yr = year
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
        # cur.execute(f"DROP TABLE IF EXISTS scorecard_{i}")

        # Create the table
        cur.execute(f'CREATE TABLE scorecard_{yr}_{i} ({columns_str} , PRIMARY KEY (UNITID))')

    # Commit changes and close the connection
    conn.commit()
    conn.close()


def insert_rows(df, year):
    df = df.iloc[:50]

    # Connect to the database
    #conn, cur = connect_to_database()

    conn_string = f'postgresql://{credentials.DB_USER}:{credentials.DB_PASSWORD}@pinniped.postgres.database.azure.com/shauck'
    db = create_engine(conn_string)
    conn = db.connect()
    # Initialize counters
    # total_rows = 0
    # inserted_rows = 0
    # rejected_rows = 0

    # Initialize the CSV writer for rejected rows
    #rejected_csv = csv.writer(open('rejected_rows.csv', 'w'))
    #rejected_csv.writerow(['error_message', 'row'])

    # Split the dataframe into three dataframes
    dfs = split_dataframe(df)
    for i in range(len(dfs)):
        df_all = dfs[i]

        # Identify rows with different data types than the first row
        #weird = (df_all.applymap(type) != df.iloc[0].apply(type)).any(axis=1)
        # Exclude missing values from being considered as weird
        #weird &= ~(df_all.isnull() & df.iloc[0].notnull()).any(axis=1)
        #df_rejected = df_all[weird]

        #df_accepted = df_all[~weird]

        # Write the accepted rows to a PostgreSQL table
       # df_accepted.to_sql(f'scorecard_{year}_{i}', con=conn, if_exists="replace", index = False)

        # Write the rejected rows to a CSV file
        #df_rejected.to_csv(f'rejected_rows_{year}_{i}.csv', index=False)

        # Identify and drop rows with different data types
        rows_to_drop = pd.DataFrame()

        for column_name in df_all.columns:
            rows_to_drop = pd.concat([rows_to_drop, df_all[df_all[column_name].apply(lambda x: type(x) is not df_all[column_name].dtype)]], ignore_index=True)

        
        # Write the rows to drop to a CSV file
        rows_to_drop.to_csv(f'rejected_rows_{year}_{i}.csv', index=False)

        # Drop rows with different data types from the original DataFrame
        df_cleaned = df_all[~df_all['UNITID'].isin(rows_to_drop['UNITID'])]


        df_cleaned.to_sql(f'scorecard_{year}_{i}', con=conn, if_exists="replace", index=False)

        
    
    """for i, df in enumerate(dfs):
        cols = ",".join([str(i) for i in df.columns.tolist()])

        # Create a list of tuples containing the values for each row
        values = [tuple(x) for x in df.values]

        # Insert the rows into the database
        for row in values:
            try:
                cur.execute(f'INSERT INTO scorecard_{i} ({cols}) VALUES ({", ".join(["%s"] * len(row))})', row)
                conn.commit()
                #print(f'INSERT INTO scorecard_{i} ({cols}) VALUES ({", ".join(["%s"] * len(row))})', row)
                inserted_rows += 1
            except psycopg.Error as e:
                # If the row is invalid, print an error message and write it to the rejected CSV file
                print(f'Error inserting row into scorecard_{i}: {e}')
                rejected_csv.writerow([str(e), row])
                rejected_rows += 1"""

    # Commit changes and close the connection
    #conn.commit()
    conn.close()

    # Print a summary of the results
    #print(f'Total rows: {total_rows}')
    #print(f'Inserted rows: {inserted_rows}')
    #print(f'Rejected rows: {rejected_rows}')


# set this flag.
new_tables = True

filename = sys.argv[1]
year = filename.split('_')[0].replace('MERGED', '')
print(year)
cleaned = clean_csv(filename)
if new_tables:
    create_tables(cleaned, year)
insert_rows(cleaned, year)
