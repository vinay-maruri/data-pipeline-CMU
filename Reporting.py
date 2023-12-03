
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
        dbname=credentials.DB_NAME,
        user=credentials.DB_USER,
        password=credentials.DB_PASSWORD
    )
    cur = conn.cursor()
    return conn, cur


year = 2021

# 1.
# Summaries of how many colleges and universities are included in the data for the selected year,
# by state and type of institution (private, public, for-profit, and so on).


def summarize_colleges_by_state_and_type(year):
    """
    Retrieves the summary of how many colleges and universities are included in the data for the selected year,
    grouped by state and type of institution.

    Args:
        year (int): The selected year for which the data is retrieved.

    Returns:
        pandas.DataFrame: A DataFrame containing the summary information, with columns 'region', 'control', and 'count'.
                          'region' represents the state, 'control' represents the type of institution, and 'count' represents
                          the number of colleges/universities in that state and institution type.
    """
    # Connect to the database
    conn, cur = connect_to_database()

    # Query the data for the selected year
    query = f"SELECT region, control,  COUNT(*) as count FROM institutioninformation WHERE year = {year} GROUP BY region, control ORDER BY region, control;"
    cur.execute(query)
    rows = cur.fetchall()

    # Create a DataFrame from the query results
    df = pd.DataFrame(rows, columns=['region', 'control', 'count'])

    # Map region codes to region names
    region_names = {
        0: 'U.S. Service schools',
        1: 'New England',
        2: 'Mid East',
        3: 'Great Lakes',
        4: 'Plains',
        5: 'Southeast',
        6: 'Southwest',
        7: 'Rocky Mountains',
        8: 'Far West',
        9: 'Outlying areas'
    }

    control_names = {
        1: 'Public',
        2: 'Private nonprofit',
        3: 'Private for-profit'
    }

    df['region'] = df['region'].map(region_names)
    df['control'] = df['control'].map(control_names)

    # Close the database connection
    cur.close()
    conn.close()

    return df


print('colleges by state and type')
print(summarize_colleges_by_state_and_type(year))


# 2.
# Summaries of current college tuition rates, by state and Carnegie Classification of institution.
def summarize_tuition_rates_by_state_and_classification(year):
    """
    Retrieves the summary of current college tuition rates, grouped by state and Carnegie Classification of institution.

    Args:
        year (int): The selected year for which the data is retrieved.

    Returns:
        pandas.DataFrame: A DataFrame containing the summary information, with columns 'region', 'classification', and 'tuition_rate'.
                          'region' represents the state, 'classification' represents the Carnegie Classification of institution,
                          and 'tuition_rate' represents the current tuition rate for that state and classification.
    """
    # Connect to the database
    conn, cur = connect_to_database()

    # Query the data for the selected year
    query = f"SELECT region, ccbasic, AVG(tuitfte) AS avg_tuition FROM institutioninformation NATURAL JOIN studentbody WHERE year = {year} GROUP BY region, ccbasic ORDER BY region;"
    cur.execute(query)
    rows = cur.fetchall()

    # Create a DataFrame from the query results
    df = pd.DataFrame(rows, columns=['region', 'classification', 'tuition_rate'])

    # Map region codes to region names
    region_names = {
        0: 'U.S. Service schools',
        1: 'New England',
        2: 'Mid East',
        3: 'Great Lakes',
        4: 'Plains',
        5: 'Southeast',
        6: 'Southwest',
        7: 'Rocky Mountains',
        8: 'Far West',
        9: 'Outlying areas'
    }

    classification_names = {
        0: 'Not classified',
        1: 'Associates Colleges: High Transfer-High Traditional',
        2: 'Traditional/Nontraditional',
        3: 'Associate\'s Colleges: High Transfer-High Nontraditional',
        4: 'Associate\'s Colleges: Mixed Transfer/Career & Technical-High Traditional',
        5: 'Associate\'s Colleges: Mixed Transfer/Career & Technical-Mixed Traditional/Nontraditional',
        6: 'Associate\'s Colleges: Mixed Transfer/Career & Technical-High Nontraditional',
        7: 'Associate\'s Colleges: High Career & Technical-High Traditional',
        8: 'Associate\'s Colleges: High Career & Technical-Mixed Traditional/Nontraditional',
        9: 'Associate\'s Colleges: High Career & Technical-High Nontraditional',
        10: 'Special Focus Two-Year: Health Professions',
        11: 'Special Focus Two-Year: Technical Professions',
        12: 'Special Focus Two-Year: Arts & Design',
        13: 'Special Focus Two-Year: Other Fields',
        14: 'Baccalaureate/Associate\'s Colleges: Associate\'s Dominant',
        15: 'Doctoral Universities: Very High Research Activity',
        16: 'Doctoral Universities: High Research Activity',
        17: 'Doctoral/Professional Universities',
        18: 'Master\'s Colleges & Universities: Larger Programs',
        19: 'Master\'s Colleges & Universities: Medium Programs',
        20: 'Master\'s Colleges & Universities: Small Programs',
        21: 'Baccalaureate Colleges: Arts & Sciences Focus',
        22: 'Baccalaureate Colleges: Diverse Fields',
        23: 'Baccalaureate/Associate\'s Colleges: Mixed Baccalaureate/Associate\'s',
        24: 'Special Focus Four-Year: Faith-Related Institutions',
        25: 'Special Focus Four-Year: Medical Schools & Centers',
        26: 'Special Focus Four-Year: Other Health Professions Schools',
        27: 'Special Focus Four-Year: Research Schools',
        28: 'Special Focus Four-Year: Engineering and Other Technology-Related Schools',
        29: 'Special Focus Four-Year: Business & Management Schools',
        30: 'Special Focus Four-Year: Arts, Music & Design Schools',
        31: 'Special Focus Four-Year: Law Schools',
        32: 'Special Focus Four-Year: Other Special Focus Institutions',
        33: 'Tribal Colleges'
    }

    df['region'] = df['region'].map(region_names)
    df['classification'] = df['classification'].map(classification_names)

    # Close the database connection
    cur.close()
    conn.close()

    return df


print('tuition rates by state and classification')
print(summarize_tuition_rates_by_state_and_classification(year))


def summarize_tuition_rates_by_state(year):
    """
    Retrieves the summary of current college tuition rates, grouped by state of institution.

    Args:
        year (int): The selected year for which the data is retrieved.

    Returns:
        pandas.DataFrame: A DataFrame containing the summary information, with columns 'region', and 'tuition_rate'.
                          'region' represents the state, 'classification' 
                          and 'tuition_rate' represents the current tuition rate for that state and classification.
    """
    # Connect to the database
    conn, cur = connect_to_database()

    # Query the data for the selected year
    query = f"SELECT region, AVG(tuitfte) AS avg_tuition FROM institutioninformation NATURAL JOIN studentbody WHERE year = {year} GROUP BY region ORDER BY region;"
    cur.execute(query)
    rows = cur.fetchall()

    # Create a DataFrame from the query results
    df = pd.DataFrame(rows, columns=['region', 'tuition_rate'])

    # Map region codes to region names
    region_names = {
        0: 'U.S. Service schools',
        1: 'New England',
        2: 'Mid East',
        3: 'Great Lakes',
        4: 'Plains',
        5: 'Southeast',
        6: 'Southwest',
        7: 'Rocky Mountains',
        8: 'Far West',
        9: 'Outlying areas'
    }

    df['region'] = df['region'].map(region_names)

    # Close the database connection
    cur.close()
    conn.close()

    return df


print('tuition rates by state')
print(summarize_tuition_rates_by_state(year))


def summarize_tuition_rates_by_classification(year):
    """
    Retrieves the summary of current college tuition rates, grouped by Carnegie Classification of institution.

    Args:
        year (int): The selected year for which the data is retrieved.

    Returns:
        pandas.DataFrame: A DataFrame containing the summary information, with columns 'classification', and 'tuition_rate'.
                          'classification' represents the Carnegie Classification of institution,
                          and 'tuition_rate' represents the current tuition rate for that classification.
    """
    # Connect to the database
    conn, cur = connect_to_database()

    # Query the data for the selected year
    query = f"SELECT ccbasic, AVG(tuitfte) AS avg_tuition FROM institutioninformation NATURAL JOIN studentbody WHERE year = {year} GROUP BY ccbasic ORDER BY ccbasic;"
    cur.execute(query)
    rows = cur.fetchall()

    # Create a DataFrame from the query results
    df = pd.DataFrame(rows, columns=['classification', 'tuition_rate'])

    # Map region codes to classification names
    classification_names = {
        0: 'Not classified',
        1: 'Associates Colleges: High Transfer-High Traditional',
        2: 'Traditional/Nontraditional',
        3: 'Associate\'s Colleges: High Transfer-High Nontraditional',
        4: 'Associate\'s Colleges: Mixed Transfer/Career & Technical-High Traditional',
        5: 'Associate\'s Colleges: Mixed Transfer/Career & Technical-Mixed Traditional/Nontraditional',
        6: 'Associate\'s Colleges: Mixed Transfer/Career & Technical-High Nontraditional',
        7: 'Associate\'s Colleges: High Career & Technical-High Traditional',
        8: 'Associate\'s Colleges: High Career & Technical-Mixed Traditional/Nontraditional',
        9: 'Associate\'s Colleges: High Career & Technical-High Nontraditional',
        10: 'Special Focus Two-Year: Health Professions',
        11: 'Special Focus Two-Year: Technical Professions',
        12: 'Special Focus Two-Year: Arts & Design',
        13: 'Special Focus Two-Year: Other Fields',
        14: 'Baccalaureate/Associate\'s Colleges: Associate\'s Dominant',
        15: 'Doctoral Universities: Very High Research Activity',
        16: 'Doctoral Universities: High Research Activity',
        17: 'Doctoral/Professional Universities',
        18: 'Master\'s Colleges & Universities: Larger Programs',
        19: 'Master\'s Colleges & Universities: Medium Programs',
        20: 'Master\'s Colleges & Universities: Small Programs',
        21: 'Baccalaureate Colleges: Arts & Sciences Focus',
        22: 'Baccalaureate Colleges: Diverse Fields',
        23: 'Baccalaureate/Associate\'s Colleges: Mixed Baccalaureate/Associate\'s',
        24: 'Special Focus Four-Year: Faith-Related Institutions',
        25: 'Special Focus Four-Year: Medical Schools & Centers',
        26: 'Special Focus Four-Year: Other Health Professions Schools',
        27: 'Special Focus Four-Year: Research Schools',
        28: 'Special Focus Four-Year: Engineering and Other Technology-Related Schools',
        29: 'Special Focus Four-Year: Business & Management Schools',
        30: 'Special Focus Four-Year: Arts, Music & Design Schools',
        31: 'Special Focus Four-Year: Law Schools',
        32: 'Special Focus Four-Year: Other Special Focus Institutions',
        33: 'Tribal Colleges'
    }

    df['classification'] = df['classification'].map(classification_names)

    # Close the database connection
    cur.close()
    conn.close()

    return df


print('tuition rates by classification')
print(summarize_tuition_rates_by_classification(year))


# 3.
# A table showing the best- and worst-performing institutions by loan repayment rates.


def get_best_and_worst_performing_institutions_by_loan_repayment_rates(year):
    # Connect to the database
    conn, cur = connect_to_database()

    # Query the data for the selected year
    query = f"SELECT * FROM loanrepayments WHERE year = {year};"

    cur.execute(query)
    rows = cur.fetchall()

    # Create a DataFrame from the query results
    df = pd.DataFrame(rows, columns=[
        'unitid',
        'year',
        'dbrr1_fed_ug_n',
        'dbrr1_fed_ug_rt',
        'dbrr4_fed_ug_n',
        'dbrr4_fed_ug_rt',
        'dbrr5_fed_ug_n',
        'dbrr5_fed_ug_rt',
        'dbrr10_fed_ug_n',
        'dbrr10_fed_ug_rt',
        'dbrr20_fed_ug_n',
        'dbrr20_fed_ug_rt'
        ])

    # Join the institutioninformation table to get the institution name (instnm)
    institution_query = f"SELECT unitid, instnm FROM institutioninformation WHERE year = {year};"
    cur.execute(institution_query)
    institution_rows = cur.fetchall()
    institution_df = pd.DataFrame(institution_rows, columns=['unitid', 'instnm'])

    # Calculate scaled values for each loan repayment column
    for column in df.columns[2:]:
        min_value = df[column].min()
        max_value = df[column].max()
        df[column] = (df[column] - min_value) / (max_value - min_value)

    # Calculate the average scaled value for each institution
    df['avg_scaled_value'] = df.iloc[:, 2:].mean(axis=1)

    # Sort the DataFrame by the average scaled value
    df = df.sort_values(by=['avg_scaled_value'])

    # Merge the institutioninformation DataFrame with the loanrepayments DataFrame
    df = pd.merge(df, institution_df, on='unitid')

    # Keep only the instnm, and average scaled value columns
    df = df[['instnm', 'avg_scaled_value']]

    # Get the top 10 best performing institutions
    worst = df.head(10)

    # Get the top 10 worst performing institutions
    best = df.tail(10)

    # Close the database connection
    cur.close()
    conn.close()

    return worst, best


year = 2021

worst, best = get_best_and_worst_performing_institutions_by_loan_repayment_rates(year)
print('worst performing institutions by loan repayment rates')
print(worst)
print('best performing institutions by loan repayment rates')
print(best)
