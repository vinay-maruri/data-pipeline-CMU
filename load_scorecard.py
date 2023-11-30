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
    # Extract 'year' from the last two numbers in the filename
    year_suffix = re.findall(r'\d{2}', filename)[-1]
    year = int("20" + year_suffix)

    # Selecting specific variables from the dataframe
    columns_to_keep = ['REGION', 'ACCREDAGENCY', 'PREDDEG', 'HIGHDEG', 'AVGFACSAL', 'SAT_AVG', 
                       'ADM_RATE', 'PPTUG_EF', 'UGDS_WHITE', 'UGDS_BLACK', 'UGDS_HISP', 'UGDS_ASIAN', 'UGDS_NRA', 'UG', 
                       'INEXPFTE', 'C150_4', 'C150_L4', 'TUITFTE', 'TUITIONFEE_IN', 'TUITIONFEE_OUT', 'TUITIONFEE_PROG',
                       'GRAD_DEBT_MDN', 'WDRAW_DEBT_MDN', 'LO_INC_DEBT_MDN', 'MD_INC_DEBT_MDN', 'HI_INC_DEBT_MDN', 
                       'DEP_DEBT_MDN', 'IND_DEBT_MDN', 'PELL_DEBT_MDN', 'NOPELL_DEBT_MDN', 'FEMALE_DEBT_MDN', 
                       'MALE_DEBT_MDN', 'FIRSTGEN_DEBT_MDN', 'NOTFIRSTGEN_DEBT_MDN', 'CDR2', 'CDR3', 'MD_EARN_WNE_P6',
                       'PCT25_EARN_WNE_P6', 'PCT75_EARN_WNE_P6', 'COUNT_WNE_INC1_P6', 'COUNT_WNE_INC2_P6', 'COUNT_WNE_INC3_P6']

    df = pd.read_csv(filename, encoding='ISO-8859-1', na_values=['', -999])
    df = df[columns_to_keep]
    df = df.replace(-999, None)
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


def is_invalid_data(row, numeric_columns):
    for col in numeric_columns:
        if isinstance(row[col], str) and re.search(r'[a-zA-Z]', row[col]):
            return True
    return False
 

def insert_data(df, table_name, conn, cur):
    total_rows = len(df)
    inserted_rows = 0
    failed_rows = 0
    failed_data = []

    numeric_columns = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]

    # Replacing pandas NA values with None for SQL compatibility
    df = df.replace({pd.NA: None})
    try:
        for index, row in df.iterrows():
            if is_invalid_data(row, numeric_columns):
                failed_rows += 1
                failed_data.append(row)
                continue

            # prepare for SQL
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
    table_name = f'scorecard_{year}'
    conn, cur = connect_to_database()
    create_table(df, table_name, conn, cur)
    total_rows, inserted_rows, failed_rows = insert_data(df, table_name, conn, cur)
    print(f"Total rows read from CSV: {total_rows}")
    print(f"Total rows successfully inserted: {inserted_rows}")
    print(f"Total rows failed to insert: {failed_rows}")

    cur.close()
    conn.close()

