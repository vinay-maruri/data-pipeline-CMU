import pandas as pd
import psycopg
import sys
import re
import os
import credentials
from collections import Counter


def write_invalid_row_to_csv(row, file_path='invalid_rows.csv'):
    """Write invalid rows to a separate CSV file."""
    with open(file_path, 'a') as f:
        f.write(','.join(str(v) for v in row) + '\n')


def connect_to_database():
    """
    Connect to the PostgreSQL database using psycopg.

    Returns:
        A tuple containing the psycopg connection and cursor objects.
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
    """Clean the CSV data and add a 'year' column."""
    df = pd.read_csv(csv_file_path, encoding='ISO-8859-1')

    year_match = re.search(r'\d{4}', csv_file_path)
    year = year_match.group(0) if year_match else 'Unknown'
    df['year'] = year

    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].replace(['-2', '-1'], pd.NA)
        df[col] = df[col].replace(r'[^\w\s]', '', regex=True)

    for col in df.select_dtypes(include=['int64', 'float64']).columns:
        df[col] = df[col].astype(str).replace(r'[^\d\.-]', '', regex=True)
        df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def infer_column_types(df, threshold=0.9):
    """
    Infer the expected data type for each column in the DataFrame.

    Args:
        df: Pandas DataFrame.
        threshold: The percentage of values in a column that must be of
        a certain type to infer it as the column type.

    Returns:
        A dictionary with column names as keys and inferred data types.
    """
    expected_types = {}
    for column in df.columns:
        types_count = Counter(df[column].dropna().map(type))
        total_count = sum(types_count.values())

        most_common_type, count = types_count.most_common(1)[0]
        if count / total_count >= threshold:
            expected_types[column] = most_common_type
        else:
            expected_types[column] = str

    return expected_types


def create_table(df, table_name):
    """Create a table in the database based on the DataFrame structure."""
    if not isinstance(df, pd.DataFrame):
        raise ValueError("df is not a DataFrame")

    if df.empty:
        raise ValueError("df is empty")

    conn, cur = connect_to_database()
    column_types = infer_column_types(df)

    columns = []
    for col, dtype in column_types.items():
        column_type = f'{col} TEXT'
        if dtype == int:
            column_type = f'{col} INTEGER'
        elif dtype == float:
            column_type = f'{col} FLOAT'
        columns.append(column_type)

    columns_str = ', '.join(columns)
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    cur.execute(f'CREATE TABLE {table_name} ({columns_str});')
    conn.commit()
    cur.close()


def insert_data(df, table_name, expected_types):
    """Insert data into the table and handle invalid rows."""
    conn, cur = connect_to_database()
    inserted_rows = 0
    rejected_rows = 0

    invalid_rows_index = pd.Index([])
    for column_name, expected_type in expected_types.items():
        invalid_rows = df[~df[column_name].apply(
            lambda x: isinstance(x, expected_type) or pd.isna(x))]
        invalid_rows_index = invalid_rows_index.union(invalid_rows.index)

    if not invalid_rows_index.empty:
        rejected_rows_df = df.loc[invalid_rows_index]
        rejected_rows_df.to_csv('rejected_rows.csv', index=False)
        rejected_rows = len(invalid_rows_index)
        df = df.drop(invalid_rows_index)

    df = df.replace({pd.NA: None})

    try:
        with conn.transaction():
            for index, row in df.iterrows():
                placeholders = ', '.join(['%s'] * len(row))
                columns = ', '.join(row.index)
                sql = (
                    f"INSERT INTO {table_name} "
                    f"({columns}) "
                    f"VALUES ({placeholders})"
                )
                cur.execute(sql, list(row.values))
            inserted_rows = len(df)
    except Exception as e:
        print(f"Error: {e}")
        inserted_rows = 0

    conn.commit()
    return inserted_rows, rejected_rows


def main(csv_file_path):
    """Main function to process the CSV file and update the database."""
    df = clean_csv(csv_file_path)
    base_name = os.path.basename(csv_file_path)
    table_name, _ = os.path.splitext(base_name)

    create_table(df, table_name)
    expected_types = infer_column_types(df)
    conn, cur = connect_to_database()
    inserted_rows, rejected_rows = insert_data(df, table_name, expected_types)
    conn.close()

    print(f"Total rows from CSV: {len(df)}")
    print(f"Inserted: {inserted_rows}, Rejected: {rejected_rows}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: <csv_file_path>")
        sys.exit(1)

    csv_file_path = sys.argv[1]
    main(csv_file_path)
