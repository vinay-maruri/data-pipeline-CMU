
import pandas as pd
import psycopg
import sys
import credentials
from tableCols import get_matching_cols, tableColumns


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


def load_scorecard_data(filename):
    """
    Load data from a CSV file into a PostgreSQL database table.

    Args:
        filename (str): The path to the CSV file to load.

    Returns:
        None
    """
    conn, cur = connect_to_database()

    collegeScorecard = pd.read_csv(filename)

    inserted_rows = []
    rejected_rows = []

    InstitutionInformation = collegeScorecard[['UNITID', 'INSTNM', 'REGION', 'CONTROL', 'CCBASIC',
                                              'LATITUDE', 'LONGITUD', 'ACCREDAGENCY', 'PREDDEG', 'HIGHDEG', 'AVGFACSAL']] 

    for index, row in InstitutionInformation.iterrows():
        try:
            cur.execute("""
                INSERT INTO InstitutionInformation (unitid, instnm, region, control, ccbasic, latitude, longitud, accredagency, preddeg, highdeg, avgfacsal)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (row['UNITID'], row['INSTNM'], row['REGION'], row['CONTROL'], row['CCBASIC'], row['LATITUDE'], row['LONGITUD'], row['ACCREDAGENCY'], row['PREDDEG'], row['HIGHDEG'], row['AVGFACSAL']))
            inserted_rows.append(index)
        except Exception as e:
            print(f"Error inserting row {index}: {e}")
            rejected_rows.append(index)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted {len(inserted_rows)} rows into the database.")
    print(f"Rejected {len(rejected_rows)} rows from the database.")
    
    


if __name__ == '__main__':
    filename = sys.argv[1]
    load_scorecard_data(filename)
