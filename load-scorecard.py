
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

    # Connect to the database
    conn, cur = connect_to_database()

    # Read the CSV file into a pandas DataFrame
    collegeScorecard = pd.read_csv(filename)

    inserted_rows = []
    rejected_rows = []

    IIcols = pd.read_csv('IIcols.csv', encoding='latin1')
    IImatchingcols = get_matching_cols(collegeScorecard, IIcols)

    IIcollegeScorecard = collegeScorecard[IImatchingcols]
    IIcollegeScorecard = IIcollegeScorecard[IIcols.columns.tolist()]

    # Iterate over each row in the DataFrame and insert it into the database
    for index, row in IIcollegeScorecard.iterrows():
        # do any necessary processing on the row
        # e.g. convert -999 to None or NULL, parse dates into Python date objects, etc.
        # ...

        # Construct the SQL INSERT statement and execute it
        insert_statement = """
            INSERT INTO scorecard_table (col1, col2, col3, ...)
            VALUES (%s, %s, %s, ...);
        """
        try:
            cur.execute(insert_statement, tuple(row))
            inserted_rows.append(row)
        except psycopg.Error as e:
            conn.rollback()
            rejected_rows.append(row)
            print(f"Error inserting row {row}: {e}")
            insert_statement = f"""
                INSERT INTO scorecard_table ({", ".join(IIcollegeScorecard.columns)})
                VALUES ({", ".join(["%s"] * len(IIcollegeScorecard.columns))});
            """

    # Commit the changes to the database
    conn.commit()

    # Print a summary of the results
    print(f"Read {len(collegeScorecard)} rows from {filename}")
    print(f"Inserted {len(inserted_rows)} rows into the database")
    if rejected_rows:
        collegeScorecard_rejected = pd.DataFrame(rejected_rows, columns=collegeScorecard.columns)
        collegeScorecard_rejected.to_csv('rejected_rows.csv', index=False)
        print(f"{len(rejected_rows)} rows were rejected and written to rejected_rows.csv")

    # Close the database connection
    cur.close()
    conn.close()


#if __name__ == '__main__':
    filename = sys.argv[1]
    load_scorecard_data(filename)
