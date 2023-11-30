# data-pipeline-CMU

There are 3 code files in this repository:

1] load-scorecard.py: This code file takes in raw college scorecard data and loads it into a postgres RDBMS

2] load_ipeds.py: This code file takes in raw IPEDS data and loads it into a postgres RDBMS

3] load-schema.py: This code file uses the loaded scorecard and IPEDS data to build user-requested tables within our prepared data table schema of InstitutionInformation, Debt, StudentBody, and StudentOutcomes. 


Instructions to run: 

- Create a database connection using the server account credentials provided to you. You will need to create a separate credentials.py file and store your username/password there. You will also have to make sure that youre connected to a DB that you can access.
- Run load-scorecard and/or load-ipeds before load-schema. 

- To load the college scorecard data:
- - 1 argument to pass

python load_scorecard.py raw scorecard file name

example: python load_scorecard.py MERGED2018_19_PP.csv

It will show a warning message: DtypeWarning: Columns (1729,1909,1910,1911,1912,1913,2376,2377,2958) have mixed types. But we are not interested in these columns, just ignore it. For columns we are interested in, if they have mixed data types, the row that has the problem will be rejected.

- To load the IPEDS data:
-- 1 argument to pass
  
python load_ipeds.py raw IPEDS file name

example: python load_ipeds.py hd2019.csv

- To load the final tables (defined by our schema):
  --3 arguments to pass
  
python load-schema.py year final table name to generate from InstitutionInformation, Debt, StudentBody, StudentOutcomes flag_to_generate_tables

example: python load-schema.py 2019 Debt True

valid years: 2019, 2020, 2021, 2022



timecards sheet: https://docs.google.com/spreadsheets/d/1rTJWASwnHNq7ZSrI6INrh1z1rssZ8Vs91-evc56JbSQ/edit?usp=sharing

project notes master document: https://docs.google.com/document/d/122vHfMRxXLKWhUTRgul8CLrdzfFJjwUiFIX7puxCojM/edit?usp=sharing
