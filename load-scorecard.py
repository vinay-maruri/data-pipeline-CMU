
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
        dbname="vmaruri",
        user=credentials.DB_USER,
        password=credentials.DB_PASSWORD
    )

    cur = conn.cursor()

    return conn, cur


def clean_csv(csv_file_path):
    file = pd.read_csv(
        csv_file_path,
        dtype={
            'LPSTAFFORD_CNT': 'object',
            'LPSTAFFORD_AMT': 'object',
            'GT_THRESHOLD_P6_SUPP': 'object',
            'DEBT_MDN': 'object',
            'DEBT_N': 'object',
            'PLUS_DEBT_INST_N': 'object',
            'PLUS_DEBT_INST_MD': 'object',
            'PLUS_DEBT_ALL_N': 'object',
            'PLUS_DEBT_ALL_MD': 'object',
            'PLUS_DEBT_INST_COMP_N': 'object',
            'PLUS_DEBT_INST_COMP_MD': 'object',
            'PLUS_DEBT_INST_COMP_MDPAY10': 'object',
            'PLUS_DEBT_INST_COMP_MD_SUPP': 'object',
            'PLUS_DEBT_INST_COMP_MDPAY10_SUPP': 'object',
            'PLUS_DEBT_ALL_COMP_N': 'object',
            'PLUS_DEBT_ALL_COMP_MD': 'object',
            'PLUS_DEBT_ALL_COMP_MDPAY10': 'object',
            'PLUS_DEBT_ALL_COMP_MD_SUPP': 'object',
            'PLUS_DEBT_ALL_COMP_MDPAY10_SUPP': 'object',
            'PLUS_DEBT_INST_NOCOMP_N': 'object',
            'PLUS_DEBT_INST_NOCOMP_MD': 'object',
            'PLUS_DEBT_ALL_NOCOMP_N': 'object',
            'PLUS_DEBT_ALL_NOCOMP_MD': 'object',
            'PLUS_DEBT_INST_MALE_N': 'object',
            'PLUS_DEBT_INST_MALE_MD': 'object',
            'PLUS_DEBT_ALL_MALE_N': 'object',
            'PLUS_DEBT_ALL_MALE_MD': 'object',
            'PLUS_DEBT_INST_NOMALE_N': 'object',
            'PLUS_DEBT_INST_NOMALE_MD': 'object',
            'PLUS_DEBT_ALL_NOMALE_N': 'object',
            'PLUS_DEBT_ALL_NOMALE_MD': 'object',
            'PLUS_DEBT_INST_PELL_N': 'object',
            'PLUS_DEBT_INST_PELL_MD': 'object',
            'PLUS_DEBT_ALL_PELL_N': 'object',
            'PLUS_DEBT_ALL_PELL_MD': 'object',
            'PLUS_DEBT_INST_NOPELL_N': 'object',
            'PLUS_DEBT_INST_NOPELL_MD': 'object',
            'PLUS_DEBT_ALL_NOPELL_N': 'object',
            'PLUS_DEBT_ALL_NOPELL_MD': 'object',
            'PLUS_DEBT_INST_STAFFTHIS_N': 'object',
            'PLUS_DEBT_INST_STAFFTHIS_MD': 'object',
            'PLUS_DEBT_ALL_STAFFTHIS_N': 'object',
            'PLUS_DEBT_ALL_STAFFTHIS_MD': 'object',
            'PLUS_DEBT_INST_NOSTAFFTHIS_N': 'object',
            'PLUS_DEBT_INST_NOSTAFFTHIS_MD': 'object',
            'PLUS_DEBT_ALL_NOSTAFFTHIS_N': 'object',
            'PLUS_DEBT_ALL_NOSTAFFTHIS_MD': 'object',
            'PLUS_DEBT_INST_STAFFANY_N': 'object',
            'PLUS_DEBT_INST_STAFFANY_MD': 'object',
            'PLUS_DEBT_ALL_STAFFANY_N': 'object',
            'PLUS_DEBT_ALL_STAFFANY_MD': 'object',
            'PLUS_DEBT_INST_NOSTAFFANY_N': 'object',
            'PLUS_DEBT_INST_NOSTAFFANY_MD': 'object',
            'PLUS_DEBT_ALL_NOSTAFFANY_N': 'object',
            'PLUS_DEBT_ALL_NOSTAFFANY_MD': 'object',
            'DBRR1_FED_UG_N': 'object',
            'DBRR1_FED_UG_NUM': 'object',
            'DBRR1_FED_UG_DEN': 'object',
            'DBRR1_FED_UG_RT': 'object',
            'DBRR1_FED_GR_N': 'object',
            'DBRR1_FED_GR_NUM': 'object',
            'DBRR1_FED_GR_DEN': 'object',
            'DBRR1_FED_GR_RT': 'object',
            'DBRR1_FED_UGCOMP_N': 'object',
            'DBRR1_FED_UGCOMP_NUM': 'object',
            'DBRR1_FED_UGCOMP_DEN': 'object',
            'DBRR1_FED_UGCOMP_RT': 'object',
            'DBRR1_FED_UGNOCOMP_N': 'object',
            'DBRR1_FED_UGNOCOMP_NUM': 'object',
            'DBRR1_FED_UGNOCOMP_DEN': 'object',
            'DBRR1_FED_UGNOCOMP_RT': 'object',
            'DBRR1_FED_UGUNK_N': 'object',
            'DBRR1_FED_UGUNK_NUM': 'object',
            'DBRR1_FED_UGUNK_DEN': 'object',
            'DBRR1_FED_UGUNK_RT': 'object',
            'DBRR1_FED_GRCOMP_N': 'object',
            'DBRR1_FED_GRCOMP_NUM': 'object',
            'DBRR1_FED_GRCOMP_DEN': 'object',
            'DBRR1_FED_GRCOMP_RT': 'object',
            'DBRR1_FED_GRNOCOMP_N': 'object',
            'DBRR1_FED_GRNOCOMP_NUM': 'object',
            'DBRR1_FED_GRNOCOMP_DEN': 'object',
            'DBRR1_FED_GRNOCOMP_RT': 'object',
            'DBRR5_FED_UG_N': 'object',
            'DBRR5_FED_UG_NUM': 'object',
            'DBRR5_FED_UG_DEN': 'object',
            'DBRR5_FED_UG_RT': 'object',
            'DBRR5_FED_GR_N': 'object',
            'DBRR5_FED_GR_NUM': 'object',
            'DBRR5_FED_GR_DEN': 'object',
            'DBRR5_FED_GR_RT': 'object',
            'DBRR10_FED_UG_N': 'object',
            'DBRR10_FED_UG_NUM': 'object',
            'DBRR10_FED_UG_DEN': 'object',
            'DBRR10_FED_UG_RT': 'object',
            'DBRR10_FED_GR_N': 'object',
            'DBRR10_FED_GR_NUM': 'object',
            'DBRR10_FED_GR_DEN': 'object',
            'DBRR10_FED_GR_RT': 'object',
            'DBRR20_FED_UG_N': 'object',
            'DBRR20_FED_UG_NUM': 'object',
            'DBRR20_FED_UG_DEN': 'object',
            'DBRR20_FED_UG_RT': 'object',
            'DBRR20_FED_GR_N': 'object',
            'DBRR20_FED_GR_NUM': 'object',
            'DBRR20_FED_GR_DEN': 'object',
            'DBRR20_FED_GR_RT': 'object',
            'DBRR1_PP_UG_N': 'object',
            'DBRR1_PP_UG_NUM': 'object',
            'DBRR1_PP_UG_DEN': 'object',
            'DBRR1_PP_UG_RT': 'object',
            'DBRR1_PP_UGCOMP_N': 'object',
            'DBRR1_PP_UGCOMP_NUM': 'object',
            'DBRR1_PP_UGCOMP_DEN': 'object',
            'DBRR1_PP_UGCOMP_RT': 'object',
            'DBRR1_PP_UGNOCOMP_N': 'object',
            'DBRR1_PP_UGNOCOMP_NUM': 'object',
            'DBRR1_PP_UGNOCOMP_DEN': 'object',
            'DBRR1_PP_UGNOCOMP_RT': 'object',
            'DBRR1_PP_UGUNK_N': 'object',
            'DBRR1_PP_UGUNK_NUM': 'object',
            'DBRR1_PP_UGUNK_DEN': 'object',
            'DBRR1_PP_UGUNK_RT': 'object',
            'DBRR5_PP_UG_N': 'object',
            'DBRR5_PP_UG_NUM': 'object',
            'DBRR5_PP_UG_DEN': 'object',
            'DBRR5_PP_UG_RT': 'object',
            'DBRR10_PP_UG_N': 'object',
            'DBRR10_PP_UG_NUM': 'object',
            'DBRR10_PP_UG_DEN': 'object',
            'DBRR10_PP_UG_RT': 'object',
            'DBRR20_PP_UG_N': 'object',
            'DBRR20_PP_UG_NUM': 'object',
            'DBRR20_PP_UG_DEN': 'object',
            'DBRR20_PP_UG_RT': 'object',
            'C150_L4_POOLED_SUPP': 'object',
            'C150_4_POOLED_SUPP': 'object',
            'C200_L4_POOLED_SUPP': 'object',
            'C200_4_POOLED_SUPP': 'object',
            'TRANS_4_POOLED_SUPP': 'object',
            'TRANS_L4_POOLED_SUPP': 'object',
            'C100_4_POOLED_SUPP': 'object',
            'C100_L4_POOLED_SUPP': 'object',
            'OMAWDP6_FTFT_POOLED_SUPP': 'object',
            'OMAWDP8_FTFT_POOLED_SUPP': 'object',
            'OMENRYP8_FTFT_POOLED_SUPP': 'object',
            'OMENRAP8_FTFT_POOLED_SUPP': 'object',
            'OMENRUP8_FTFT_POOLED_SUPP': 'object',
            'OMAWDP6_FTNFT_POOLED_SUPP': 'object',
            'OMAWDP8_FTNFT_POOLED_SUPP': 'object',
            'OMENRYP8_FTNFT_POOLED_SUPP': 'object',
            'OMENRAP8_FTNFT_POOLED_SUPP': 'object',
            'OMENRUP8_FTNFT_POOLED_SUPP': 'object',
            'OMENRYP_ALL_POOLED_SUPP': 'object',
            'OMENRAP_ALL_POOLED_SUPP': 'object',
            'OMAWDP8_ALL_POOLED_SUPP': 'object',
            'OMENRUP_ALL_POOLED_SUPP': 'object',
            'OMENRYP_FIRSTTIME_POOLED_SUPP': 'object',
            'OMENRAP_FIRSTTIME_POOLED_SUPP': 'object',
            'OMAWDP8_FIRSTTIME_POOLED_SUPP': 'object',
            'OMENRUP_FIRSTTIME_POOLED_SUPP': 'object',
            'OMENRYP_NOTFIRSTTIME_POOLED_SUPP': 'object',
            'OMENRAP_NOTFIRSTTIME_POOLED_SUPP': 'object',
            'OMAWDP8_NOTFIRSTTIME_POOLED_SUPP': 'object',
            'OMENRUP_NOTFIRSTTIME_POOLED_SUPP': 'object',
            'OMENRYP_FULLTIME_POOLED_SUPP': 'object',
            'OMENRAP_FULLTIME_POOLED_SUPP': 'object',
            'OMAWDP8_FULLTIME_POOLED_SUPP': 'object',
            'OMENRUP_FULLTIME_POOLED_SUPP': 'object',
            'C150_4_PELL_POOLED_SUPP': 'object',
            'OMAWDP8_PELL_FTFT_POOLED_SUPP': 'object',
            'OMENRYP8_PELL_FTFT_POOLED_SUPP': 'object',
            'OMENRAP8_PELL_FTFT_POOLED_SUPP': 'object',
            'OMENRUP8_PELL_FTFT_POOLED_SUPP': 'object',
            'OMAWDP8_PELL_PTFT_POOLED_SUPP': 'object',
            'OMENRYP8_PELL_PTFT_POOLED_SUPP': 'object',
            'OMENRAP8_PELL_PTFT_POOLED_SUPP': 'object',
            'OMENRUP8_PELL_PTFT_POOLED_SUPP': 'object',
            'OMAWDP8_PELL_PTNFT_POOLED_SUPP': 'object',
            'OMENRYP8_PELL_PTNFT_POOLED_SUPP': 'object',
            'OMENRAP8_PELL_PTNFT_POOLED_SUPP': 'object',
            'OMENRUP8_PELL_PTNFT_POOLED_SUPP': 'object',
            'OMENRYP_PELL_ALL_POOLED_SUPP': 'object',
            'OMENRAP_PELL_ALL_POOLED_SUPP': 'object',
            'OMAWDP8_PELL_ALL_POOLED_SUPP': 'object',
            'OMENRUP_PELL_ALL_POOLED_SUPP': 'object',
            'OMENRYP_PELL_FTT_POOLED_SUPP': 'object',
            'OMENRAP_PELL_FTT_POOLED_SUPP': 'object',
            'OMAWDP8_PELL_FTT_POOLED_SUPP': 'object',
            'OMENRUP_PELL_FTT_POOLED_SUPP': 'object',
            'OMENRYP_PELL_NFT_POOLED_SUPP': 'object',
            'OMENRAP_PELL_NFT_POOLED_SUPP': 'object',
            'OMAWDP8_PELL_NFT_POOLED_SUPP': 'object',
            'OMENRUP_PELL_NFT_POOLED_SUPP': 'object',
            'OMENRYP_PELL_FT_POOLED_SUPP': 'object',
            'OMENRAP_PELL_FT_POOLED_SUPP': 'object',
            'OMAWDP8_PELL_FT_POOLED_SUPP': 'object',
            'OMENRUP_PELL_FT_POOLED_SUPP': 'object',
            'OMENRYP_PELL_PT_POOLED_SUPP': 'object',
            'OMENRAP_PELL_PT_POOLED_SUPP': 'object',
            'OMAWDP8_PELL_PT_POOLED_SUPP': 'object',
            'OMENRUP_PELL_PT_POOLED_SUPP': 'object'})
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
    file['year1'] = pd.to_datetime(year1, format='%Y')
    file['year2'] = pd.to_datetime(year2, format='%Y')

    # Remove alphabet and punctuation from numeric columns
    numeric_cols = file.select_dtypes(include=['int64', 'float64']).columns
    file[numeric_cols] = file[numeric_cols].replace('[^0-9\\.]+', '',
                                                    regex=True)

    # Remove symbols and punctuation from object columns
    object_cols = file.select_dtypes(include=['object']).columns
    file[object_cols] = file[object_cols].replace('[^0-9a-zA-Z]+', '',
                                                  regex=True)

    # replace privacy suppressed values with NaN in numeric columns
    numeric_cols = file.select_dtypes(include=['int64', 'float64']).columns
    file[numeric_cols] = file[numeric_cols].replace(
        'PrivacySuppressed', 999, regex=True)

    # replace privacy suppressed values with NaN in object columns
    object_cols = file.select_dtypes(include=['object']).columns
    file[object_cols] = file[object_cols].replace(
        'PrivacySuppressed', 999, regex=True)

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
    yr = year
    columns = []
    int_cols, float_cols, object_cols = get_column_types(df)
    for col in df.columns:
        if str(col) in int_cols:
            columns.append(f'{col} INTEGER')
        elif str(col) in float_cols:
            columns.append(f'{col} FLOAT')
        else:
            columns.append(f'{col} VARCHAR(255)')
        # Create the table
    column_str_1 = ', '.join(columns[0:462])
    column_str_2 = ', '.join(columns[462:924])
    column_str_3 = ', '.join(columns[924:1386])
    column_str_4 = ', '.join(columns[1386:1848])
    column_str_5 = ', '.join(columns[1848:2310])
    column_str_6 = ', '.join(columns[2310:2772])
    column_str_7 = ', '.join(columns[2772:3232])
    cur.execute(f'CREATE TABLE scorecard_{yr}_1 ({column_str_1});')
    cur.execute(f'CREATE TABLE scorecard_{yr}_2 ({column_str_2});')
    cur.execute(f'CREATE TABLE scorecard_{yr}_3 ({column_str_3});')
    cur.execute(f'CREATE TABLE scorecard_{yr}_4 ({column_str_4});')
    cur.execute(f'CREATE TABLE scorecard_{yr}_5 ({column_str_5});')
    cur.execute(f'CREATE TABLE scorecard_{yr}_6 ({column_str_6});')
    cur.execute(f'CREATE TABLE scorecard_{yr}_7 ({column_str_7});')
    # Commit changes and close the connection
    conn.commit()
    conn.close()


def insert_rows(df, year):
    # Connect to the database
    conn, cur = connect_to_database()
    num_rows_inserted = 0
    num_rows_rejected = 0
    rejected_csv = csv.writer(open(f'rejected_rows_{year}.csv', 'w'))
    df.drop(labels='ALIAS', axis=1)
    # make a new transaction
    with conn.transaction():
        for row in df.fillna(999).itertuples():
            row = tuple(row)
            try:
                with conn.transaction():
                    query1 = f'INSERT INTO scorecard_{year}_1 VALUES {row[0:462]};'
                    cur.execute(query1)
                    query2 = f'INSERT INTO scorecard_{year}_2 VALUES {row[462:924]};'
                    cur.execute(query2)
                    query3 = f'INSERT INTO scorecard_{year}_3 VALUES {row[924:1386]};'
                    cur.execute(query3)
                    query4 = f'INSERT INTO scorecard_{year}_4 VALUES {row[1386:1848]};'
                    cur.execute(query4)
                    query5 = f'INSERT INTO scorecard_{year}_5 VALUES {row[1848:2310]};'
                    cur.execute(query5)
                    query6 = f'INSERT INTO scorecard_{year}_6 VALUES {row[2310:2772]};'
                    cur.execute(query6)
                    query7 = f'INSERT INTO scorecard_{year}_7 VALUES {row[2772:3231]};'
                    cur.execute(query7)
            except Exception as e:
                rejected_csv.writerow([str(e), row])
                num_rows_rejected += 1
            else:
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
cleaned = clean_csv(filename)
if new_tables:
    create_tables(cleaned, year)
insert_rows(cleaned, year)
