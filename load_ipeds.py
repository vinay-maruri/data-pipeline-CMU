import pandas as pd
import psycopg
import sys
import re
import os
import credentials
from sqlalchemy import create_engine, text
from collections import Counter


# write invalid rows to a separate CSV
def write_invalid_row_to_csv(row, file_path='invalid_rows.csv'):
    with open(file_path, 'a') as f:
        f.write(','.join(str(v) for v in row) + '\n')


def connect_to_database():
    """
    Connect to the PostgreSQL database using both psycopg and SQLAlchemy.

    Returns:
        A tuple containing the SQLAlchemy engine, psycopg connection, and cursor objects.
    """
    # SQLAlchemy engine
    # database_uri = 'postgresql://{user}:{password}@pinniped.postgres.database.azure.com/xwu5'.format(user=credentials.DB_USER, password=credentials.DB_PASSWORD)
    # engine = create_engine(database_uri)

    # psycopg connection
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


def infer_column_types(df, threshold=0.9):
    """
    Infer the expected data type for each column in the DataFrame.
    Args:
        df: Pandas DataFrame.
        threshold: The percentage of values in a column that must be of a certain type to infer it as the column type.
    Returns:
        A dictionary with column names as keys and inferred data types as values.
    """
    expected_types = {}
    for column in df.columns:
        # Count the types of all non-null values in the column
        types_count = Counter(df[column].dropna().map(type))
        total_count = sum(types_count.values())

        # Find the most common type and check if it meets the threshold
        most_common_type, count = types_count.most_common(1)[0]
        if count / total_count >= threshold:
            expected_types[column] = most_common_type
        else:
            expected_types[column] = str  # Default to string if no dominant type

    return expected_types


def create_table(df, table_name):
    if not isinstance(df, pd.DataFrame):
        raise ValueError("df is not a DataFrame in create_table function")

    if df.empty:
        raise ValueError("df is empty in create_table function")

    conn, cur = connect_to_database()

    # 使用 infer_column_types 函数来获取数据类型
    column_types = infer_column_types(df)

    # 定义列
    columns = []
    for col, dtype in column_types.items():
        if dtype == int:
            columns.append(f'{col} INTEGER')
        elif dtype == float:
            columns.append(f'{col} FLOAT')
        elif dtype == str:
            columns.append(f'{col} TEXT')
        else:
            # 为了安全起见，如果数据类型不是 int、float 或 str，可以默认为 VARCHAR
            columns.append(f'{col} TEXT')

    columns_str = ', '.join(columns)

    # 删除已存在的表并创建新表
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    cur.execute(f'CREATE TABLE {table_name} ({columns_str}, PRIMARY KEY (UNITID));')

    conn.commit()
    cur.close()



def insert_data(df, table_name, expected_types):
    conn, cur = connect_to_database()
    inserted_rows = 0
    rejected_rows = 0

    # 检查每一列的数据类型，如果不符合预期，则标记为无效
    invalid_rows_index = pd.Index([])
    for column_name, expected_type in expected_types.items():
        # 如果列中的数据类型不符合预期，则标记该行为无效
        invalid_rows = df[~df[column_name].apply(lambda x: isinstance(x, expected_type) or pd.isna(x))]
        invalid_rows_index = invalid_rows_index.union(invalid_rows.index)

    # 计算被删除的行数并存储到 CSV 文件中
    if not invalid_rows_index.empty:
        rejected_rows_df = df.loc[invalid_rows_index]
        rejected_rows_df.to_csv('rejected_rows.csv', index=False)
        rejected_rows = len(invalid_rows_index)

        # 从原始 DataFrame 中删除无效行
        df = df.drop(invalid_rows_index)

    # 使用清洗后的数据进行插入
    try:
        with conn.transaction():
            for index, row in df.iterrows():
                placeholders = ', '.join(['%s'] * len(row))
                columns = ', '.join(row.index)
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                cur.execute(sql, list(row.values))
            inserted_rows = len(df)
    except Exception as e:
        print(f"Error inserting data into {table_name}: {e}")
        inserted_rows = 0
    
    conn.commit()
    return inserted_rows, rejected_rows


def main(csv_file_path):
    df = clean_csv(csv_file_path)
    # 从文件路径中提取基础文件名作为表名
    base_name = os.path.basename(csv_file_path)
    table_name, _ = os.path.splitext(base_name)

    create_table(df, table_name)

    expected_types = infer_column_types(df)
    
    # connect to the database
    conn, cur = connect_to_database()

    # insert data
    inserted_rows, rejected_rows = insert_data(df, table_name, expected_types)

    # close connection
    conn.close()

    # data sammary
    print(f"Total rows read from CSV: {len(df)}")
    print(f"Total rows successfully inserted: {inserted_rows}")
    print(f"Total rows rejected: {rejected_rows}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python load_ipeds.py <csv_file_path>")
        sys.exit(1)

    csv_file_path = sys.argv[1]
    main(csv_file_path)
