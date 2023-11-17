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
        dbname="vmaruri",
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
            TUITIONFEE_IN INTEGER,
            TUITIONFEE_OUT INTEGER,
            TUITIONFEE_PROG INTEGER,
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


def select_data(years):
    conn, cur = connect_to_database()
    scorecards = []
    hds = []
    for i in years:
        scorecards.append(pd.read_sql_query(f'SELECT * from scorecard_{i};', conn))
    scorecard_df = pd.concat(scorecards)
    for y in years:
        hds.append(pd.read_sql_query(f'SELECT * from hd_{i};', conn))
    hd_df = pd.concat(hds)
    merged_df = pd.merge(left=scorecard_df, right=hd_df, how='inner', on='UNITID')
    return merged_df


def insert_data(df, table_name):
    """Insert data into the table and handle invalid rows."""
    conn, cur = connect_to_database()
    inserted_rows = 0
    rejected_rows = 0
    rejected_csv = csv.writer(open(f'rejected_rows_{table_name}.csv', 'w'))
    if table_name == "InstitutionInformation":
        df = df.loc[:, ["UNITID", "INSTNM", "LOCATION", "ADDR", "REGION", "CONTROL", "CCBASIC", "CENSUSIDS", "LATITUDE", "LONGITUDE", "ACCREDAGENCY", "PREDDEG", "HIGHDEG", "AVGFACSAL"]]
    elif table_name == "StudentBody":
        df = df.loc[:, ["UNITID", "SAT_AVG", "ADM_RATE", "PPTUG_EF", "UGDS_WHITE", "UGDS_BLACK", "UGDS_HISP", "UGDS_ASIAN", "UGDS_NRA", "UG", "INEXPFTE", "C150_4", "C150_L4", "TUITFTE", "TUITIONFEE_IN", "TUITIONFEE_OUT", "TUITIONFEE_PROG"]]
    elif table_name == "Debt":
        df = df.loc[:, ["UNITID", "GRAD_DEBT_MDN", "WDRAW_DEBT_MDN", "LO_INC_DEBT_MDN", "MD_INC_DEBT_MDN", "HI_INC_DEBT_MDN", "DEP_DEBT_MDN", "IND_DEBT_MDN", "PELL_DEBT_MDN", "NOPELL_DEBT_MDN", "FEMALE_DEBT_MDN", "MALE_DEBT_MDN", "FIRSTGEN_DEBT_MDN", "NOTFIRSTGEN_DEBT_MDN", "CDR2", "CDR3"]]
    else:
        df = df.loc[:, ["UNITID", "PCT25_EARN_WNE_P6", "PCT75_EARN_WNE_P6", "COUNT_WNE_INC1_P6", "COUNT_WNE_INC2_P6", "COUNT_WNE_INC3_P6"]]
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


def main(years, table_name):
    """Main function to process the CSV file and update the database."""
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
    years = sys.argv[1]
    table_name = sys.argv[2]
    main(years, table_name)
