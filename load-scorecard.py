import pandas as pd
import psycopg
import sys
import credentials
import csv
import numpy as np


def connect_to_database():
    """
    Connect to the PostgreSQL database.

    Returns:
        A tuple containing the connection and cursor objects.
    """
    conn = psycopg.connect(
        host="pinniped.postgres.database.azure.com",
        dbname="vmaruri",
        user=credentials.DB_USER,
        password=credentials.DB_PASSWORD
    )

    cur = conn.cursor()

    return conn, cur


def clean_csv(csv_file_path):
    columns_to_select = [
        "UNITID",
        "INSTNM",
        "CONTROL",
        "ACCREDAGENCY",
        "ADDR",
        "REGION",
        "CCBASIC",
        "LATITUDE",
        "LONGITUDE",
        "PREDDEG",
        "HIGHDEG",
        "AVGFACSAL",
        "SAT_AVG",
        "ADM_RATE",
        "PPTUG_EF",
        "UGDS_WHITE",
        "UGDS_BLACK",
        "UGDS_HISP",
        "UGDS_ASIAN",
        "UGDS_NRA",
        "UG",
        "INEXPFTE",
        "C150_4",
        "C150_L4",
        "TUITFTE",
        "TUITIONFEE_IN",
        "TUITIONFEE_OUT",
        "TUITIONFEE_PROG",
        "GRAD_DEBT_MDN",
        "WDRAW_DEBT_MDN",
        "LO_INC_DEBT_MDN",
        "MD_INC_DEBT_MDN",
        "HI_INC_DEBT_MDN",
        "DEP_DEBT_MDN",
        "IND_DEBT_MDN",
        "PELL_DEBT_MDN",
        "NOPELL_DEBT_MDN",
        "FEMALE_DEBT_MDN",
        "MALE_DEBT_MDN",
        "FIRSTGEN_DEBT_MDN",
        "NOTFIRSTGEN_DEBT_MDN",
        "CDR2",
        "CDR3",
        "MD_EARN_WNE_P6",
        "PCT25_EARN_WNE_P6",
        "PCT75_EARN_WNE_P6",
        "COUNT_WNE_INC1_P6",
        "COUNT_WNE_INC2_P6",
        "COUNT_WNE_INC3_P6"]
    file = pd.read_csv(
        filepath_or_buffer=csv_file_path,
        usecols=columns_to_select)
    if object_dtypes := {
        c: dtype for c in file.columns if (
            dtype := pd.api.types.infer_dtype(
            file[c])).startswith("mixed")}:
        raise TypeError(
            f"Dataframe has one more object dtypes: {object_dtypes}")
    # Extract year from csv_file_path
    year1 = csv_file_path.split('_')[0].replace('MERGED', '')
    year2 = str(int(year1) + 1)

    # Add year as a new column and convert to date data type
    file['year1'] = year1
    file['year2'] = year2

    # replace privacy suppressed values with NaN in numeric columns
    numeric_cols = file.select_dtypes(include=['int64', 'float64']).columns
    file[numeric_cols] = file[numeric_cols].replace(
        'PrivacySuppressed', 999, regex=True)

    # replace privacy suppressed values with NaN in object columns
    object_cols = file.select_dtypes(include=['object']).columns
    file[object_cols] = file[object_cols].replace(
        'PrivacySuppressed', "999", regex=True)
    print(f"Number of rows read in: {len(file)}")

    # replace nan's with 999.
    numeric_cols = file.select_dtypes(include=['int64', 'float64']).columns
    file[numeric_cols] = file[numeric_cols].replace(
        np.nan, 999, regex=True)

    return file


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
    conn, cur = connect_to_database()
    # Split the dataframe into three dataframes
    yr = year
    int_cols, float_cols, _ = get_column_types(df)
    columns = []
    for col in df.columns:
        if str(col) in int_cols:
            # print(f"this is an int col: {col}")
            columns.append(f'{col} INTEGER')
        elif str(col) in float_cols:
            # print(f"this is a float col: {col}")
            columns.append(f'{col} FLOAT')
        else:
            # print(f"this is a text col: {col}")
            columns.append(f'{col} VARCHAR(255)')
    print(len(columns))
    column_str_1 = ', '.join(columns)
    print(column_str_1)
    cur.execute(f'CREATE TABLE scorecard_{yr} ({column_str_1});')
    conn.commit()
    conn.close()


def insert_rows(df, year):
    # Connect to the database
    conn, cur = connect_to_database()
    num_rows_inserted = 0
    num_rows_rejected = 0
    rejected_csv = csv.writer(open(f'rejected_rows_{year}.csv', 'w'))
    # make a new transaction
    with conn.transaction():
        for row in df.itertuples():
            row = tuple(row)[1:]
            try:
                with conn.transaction():
                    query1 = f'INSERT INTO scorecard_{year} VALUES {row};'
                    cur.execute(query1)
            except Exception as e:
                # print("row rejected")
                rejected_csv.writerow([str(e), row])
                num_rows_rejected += 1
            else:
                # print("row inserted")
                num_rows_inserted += 1
    # now we commit the entire transaction
    conn.commit()
    conn.close()
    print(f"Total number of rows inserted: {num_rows_inserted}")
    print(f"Total number of rows rejected: {num_rows_rejected}")


# set this flag.
new_tables = True

filename = sys.argv[1]
year = filename.split('_')[0].replace('MERGED', '')
print(f"loading in {year} data")
cleaned = clean_csv(filename)
# pick out the columns that we need.
if new_tables:
    create_tables(cleaned, year)
insert_rows(cleaned, year)
