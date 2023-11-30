import pandas as pd
import psycopg
import credentials
import csv
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


def create_tables_schema():
    # Connect to the database
    conn, cur = connect_to_database()
    # Create the tables
    cur.execute("""
        CREATE TABLE InstitutionInformation (
            UNITID INTEGER PRIMARY KEY,
            INSTNM TEXT,
            LOCATION TEXT,
            ADDR TEXT,
            REGION INTEGER,
            CONTROL INTEGER,
            CCBASIC INTEGER,
            CENSUSIDS INTEGER,
            LATITUDE INTEGER,
            LONGITUDE INTEGER,
            ACCREDAGENCY TEXT,
            PREDDEG INTEGER,
            HIGHDEG INTEGER,
            AVGFACSAL INTEGER
        );

        CREATE TABLE StudentBody (
            UNITID INTEGER PRIMARY KEY,
            PPTUG_EF FLOAT,
            UGDS_WHITE FLOAT,
            UGDS_BLACK FLOAT,
            UGDS_HISP FLOAT,
            UGDS_ASIAN FLOAT,
            UGDS_NRA FLOAT,
            UG INTEGER,
            INEXPFTE INTEGER,
            C150_4 INTEGER,
            C150_L4 INTEGER,
            TUITFTE INTEGER,
            TUITIONFEE_IN INTEGER,
            TUITIONFEE_OUT INTEGER,
            TUITIONFEE_PROG INTEGER
        );

        CREATE TABLE Debt (
            UNITID INTEGER PRIMARY KEY,
            GRAD_DEBT_MDN INTEGER,
            WDRAW_DEBT_MDN INTEGER,
            LO_INC_DEBT_MDN INTEGER,
            MD_INC_DEBT_MDN INTEGER,
            HI_INC_DEBT_MDN INTEGER,
            DEP_DEBT_MDN INTEGER,
            IND_DEBT_MDN INTEGER,
            PELL_DEBT_MDN INTEGER,
            NOPELL_DEBT_MDN INTEGER,
            FEMALE_DEBT_MDN INTEGER,
            MALE_DEBT_MDN INTEGER,
            FIRSTGEN_DEBT_MDN INTEGER,
            NOTFIRSTGEN_DEBT_MDN INTEGER,
            CDR2 INTEGER,
            CDR3 INTEGER
        );

        CREATE TABLE StudentOutcomes (
            UNITID INTEGER PRIMARY KEY,
            PCT25_EARN_WNE_P6 INTEGER,
            PCT75_EARN_WNE_P6 INTEGER,
            COUNT_WNE_INC1_P6 INTEGER,
            COUNT_WNE_INC2_P6 INTEGER,
            COUNT_WNE_INC3_P6 INTEGER
        );
        CREATE TABLE LoanRepayments(
            UNITID INTEGER PRIMARY KEY,
            DBRR1_FED_UG_N INTEGER,
            DBRR1_FED_UG_RT FLOAT,
            DBRR4_FED_UG_N INTEGER,
            DBRR4_FED_UG_RT FLOAT,
            DBRR5_FED_UG_N INTEGER,
            DBRR5_FED_UG_RT FLOAT,
            DBRR10_FED_UG_N INTEGER,
            DBRR10_FED_UG_RT FLOAT,
            DBRR20_FED_UG_N INTEGER,
            DBRR20_FED_UG_RT FLOAT
        );
        CREATE TABLE Admissions(
            UNITID INTEGER PRIMARY KEY,
            SAT_AVG INTEGER,
            ADM_RATE FLOAT,
            OPENADMP INTEGER,
            ADMCON7 INTEGER
        );
        """)
    # Commit the changes
    conn.commit()
    conn.close()


def select_data(year):
    conn, cur = connect_to_database()
    scorecard_df = pd.read_sql_query(
        f'SELECT * from scorecard_{year - 1};', conn)
    hd_df = pd.read_sql_query(f'SELECT * from hd{year};', conn)
    merged_df = pd.merge(
        left=scorecard_df,
        right=hd_df,
        how='inner',
        on='unitid')
    return merged_df


def insert_data(df, table_name):
    """Insert data into the table and handle invalid rows."""
    conn, cur = connect_to_database()
    inserted_rows = 0
    rejected_rows = 0
    rejected_csv = csv.writer(open(f'rejected_rows_{table_name}.csv', 'w'))
    if table_name == "InstitutionInformation":
        df = df.loc[:,
                    ['unitid',
                     'instnm',
                     'location',
                     'addr',
                     'region',
                     'control',
                     'ccbasic',
                     'censusids',
                     'latitude',
                     'longitude',
                     'accredagency',
                     'preddeg',
                     'highdeg',
                     'avgfacsal']]
    elif table_name == "StudentBody":
        df = df.loc[:,
                    ['unitid',
                     'sat_avg',
                     'adm_rate',
                     'pptug_ef',
                     'ugds_white',
                     'ugds_black',
                     'ugds_hisp',
                     'ugds_asian',
                     'ugds_nra',
                     'ug',
                     'inexpfte',
                     'c150_4',
                     'c150_l4',
                     'tuitfte',
                     'tuitionfee_in',
                     'tuitionfee_out',
                     'tuitionfee_prog']]
    elif table_name == "Debt":
        df = df.loc[:,
                    ['unitid',
                     'grad_debt_mdn',
                     'wdraw_debt_mdn',
                     'lo_inc_debt_mdn',
                     'md_inc_debt_mdn',
                     'hi_inc_debt_mdn',
                     'dep_debt_mdn',
                     'ind_debt_mdn',
                     'pell_debt_mdn',
                     'nopell_debt_mdn',
                     'female_debt_mdn',
                     'male_debt_mdn',
                     'firstgen_debt_mdn',
                     'notfirstgen_debt_mdn',
                     'cdr2',
                     'cdr3']]
    elif table_name == "LoanRepayments":
        df = df.loc[:,
                    ['unitid',
                     'dbrr1_fed_ug_n',
                     'dbrr1_fed_ug_rt',
                     'dbrr4_fed_ug_n',
                     'dbrr4_fed_ug_rt',
                     'dbrr5_fed_ug_n',
                     'dbrr5_fed_ug_rt',
                     'dbrr10_fed_ug_n',
                     'dbrr10_fed_ug_rt',
                     'dbrr20_fed_ug_n',
                     'dbrr20_fed_ug_rt']]
    elif table_name == "Admissions":
        df = df.loc[:,
                    ['unitid',
                     'sat_avg',
                     'adm_rate',
                     'openadmp',
                     'admcon7']]
    else:
        df = df.loc[:,
                    ['unitid',
                     'pct25_earn_wne_p6',
                     'pct75_earn_wne_p6',
                     'count_wne_inc1_p6',
                     'count_wne_inc2_p6',
                     'count_wne_inc3_p6']]
    with conn.transaction():
        for index, row in df.iterrows():
            try:
                placeholders = ', '.join(['%s'] * len(row))
                columns = ', '.join(row.index)
                sql = (
                    f"INSERT INTO {table_name} "
                    f"({columns}) "
                    f"VALUES ({placeholders})"
                )
                cur.execute(sql, list(row.values))
            except Exception as e:
                rejected_csv.writerow([str(e), row])
                print(f"Error: {e}")
                rejected_rows += 1
            else:
                inserted_rows += 1
    conn.commit()
    return inserted_rows, rejected_rows


def main(years, table_name, user_flag):
    """Main function to process the CSV file and update the database."""
    if user_flag == "True":
        print("entered this area")
        create_tables_schema()
    df = select_data(years)
    conn, cur = connect_to_database()
    inserted_rows, rejected_rows = insert_data(df, table_name)
    conn.close()

    print(f"Total rows from CSV: {len(df)}")
    print(f"Inserted: {inserted_rows}, Rejected: {rejected_rows}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python load-schema.py [<years>] <table_name>")
        sys.exit(1)
    years = int(sys.argv[1])
    print(years)
    table_name = sys.argv[2]
    print(table_name)
    user_flag = sys.argv[3]
    print(user_flag)
    main(years, table_name, user_flag)
