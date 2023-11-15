
import csv
import psycopg
import sys
import credentials


def load_scorecard_data(filename):
    """
    Load data from a CSV file into a PostgreSQL database table.

    Args:
        filename (str): The path to the CSV file to load.

    Returns:
        None
    """
    # Open the CSV file and read its contents into a list of rows
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        next(reader) # skip header row
        rows = [row for row in reader]

    # Connect to the PostgreSQL database
    conn = psycopg.connect(
        host = "pinniped.postgres.database.azure.com",
        dbname = "shauck",
        user = credentials.DB_USER,
        password = credentials.DB_PASSWORD
    )
    cur = conn.cursor()

    inserted_rows = []
    rejected_rows = []

    # Iterate over each row in the list and insert it into the database
    for row in rows:
        # do any necessary processing on the row
        # e.g. convert -999 to None or NULL, parse dates into Python date objects, etc.
        # ...

        # Construct the SQL INSERT statement and execute it
        insert_statement = """
            INSERT INTO scorecard_table (col1, col2, col3, ...)
            VALUES (%s, %s, %s, ...);
        """
        try:
            cur.execute(insert_statement, row)
            inserted_rows.append(row)
        except psycopg.Error as e:
            conn.rollback()
            rejected_rows.append(row)
            print(f"Error inserting row {row}: {e}")

    # Commit the changes to the database
    conn.commit()

    # Print a summary of the results
    print(f"Read {len(rows)} rows from {filename}")
    print(f"Inserted {len(inserted_rows)} rows into the database")
    if rejected_rows:
        with open('rejected_rows.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(rejected_rows)
        print(f"{len(rejected_rows)} rows were rejected and written to rejected_rows.csv")

    # Close the database connection
    cur.close()
    conn.close()


if __name__ == '__main__':
    filename = sys.argv[1]
    load_scorecard_data(filename)
