

import pandas as pd
import psycopg
import credentials


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
            LONGITUD INTEGER,
            ACCREDAGENCY TEXT,
            PREDDEG INTEGER,
            HIGHDEG INTEGER,
            AVGFACSAL INTEGER
        );

        CREATE TABLE StudentBody (
            UNITID INTEGER PRIMARY KEY,
            SAT_AVG INTEGER,
            ADM_RATE FLOAT,
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
            TUTIONFEE_IN INTEGER,
            TUTIONFEE_OUT INTEGER,
            TUTIONFEE_PROG INTEGER,
            FOREIGN KEY (UNITID) REFERENCES InstitutionInformation(UNITID)
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
            CDR3 INTEGER,
            FOREIGN KEY (UNITID) REFERENCES InstitutionInformation(UNITID)
        );

        CREATE TABLE StudentOutcomes (
            UNITID INTEGER PRIMARY KEY,
            MD_EARN_WNE_6 INTEGER,
            PCT25_EARN_WNE_P6 INTEGER,
            PCT75_EARN_WNE_P6 INTEGER,
            COUNT_WNE_INC1_P6 INTEGER,
            COUNT_WNE_INC2_P6 INTEGER,
            COUNT_WNE_INC3_P6 INTEGER,
            FOREIGN KEY (UNITID) REFERENCES InstitutionInformation(UNITID)
        );
        """)
    
    # Commit the changes
    conn.commit()
    conn.close()


def create_table_scorecard(scorecard_df):
    conn, cur = connect_to_database()
    int_cols, float_cols, _ = get_column_types(scorecard_df)
    columns = []
    for col in scorecard_df.columns:
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
    cur.execute(f'CREATE TABLE scorecard_full ({column_str_1});')
    conn.commit()
    conn.close()


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


