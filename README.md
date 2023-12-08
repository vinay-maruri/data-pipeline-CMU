# data-pipeline-CMU

Short Instructions for TA or Professor Reinhart:

To run the pipeline (for all of the current data files):
- clone this repository
- create a credentials.py file and define DB_NAME, DB_USER, and DB_PASSWORD in that file as strings (these are for your azure server account)
- run this command (assuming that your data files are unpacked from the gz files): python load-scorecard.py MERGED2018_19_PP.csv; python load-scorecard.py MERGED2019_20_PP.csv; python load-scorecard.py MERGED2020_21_PP.csv; python load-scorecard.py MERGED2021_22_PP.csv; python load_ipeds.py hd2019.csv; python load_ipeds.py hd2020.csv;  python load_ipeds.py hd2021.csv; python load_ipeds.py hd2022.csv; python load-schema.py 2019 True; python load-schema.py 2020 False; python load-schema.py 2021 False; python load-schema.py 2022 False
- install papermill (for the reports): pip install papermill
- To generate the reports (for years 2019-2021), run this command: jupyter nbconvert --to script "Reporting Notebook.ipynb"; papermill "Reporting Notebook.ipynb" "Report<year>.ipynb" -p year <year>; jupyter nbconvert --no-input --to html Report<year>.ipynb
- be prepared for a runtime of ~30 minutes. We are making 6 tables, inserting in data 120,000 times.


There are 3 code files in this repository:

1] load-scorecard.py: This code file takes in raw college scorecard data and loads it into a postgres RDBMS

2] load_ipeds.py: This code file takes in raw IPEDS data and loads it into a postgres RDBMS

3] load-schema.py: This code file uses the loaded scorecard and IPEDS data to build user-requested tables within our prepared data table schema of InstitutionInformation, Debt, StudentBody, and StudentOutcomes. 


Instructions to run: 

- Create a database connection using the server account credentials provided to you. You will need to create a separate credentials.py file and store your username/password there. You will also have to make sure that youre connected to a DB that you can access.
- Run load-scorecard and/or load-ipeds before load-schema. 

- To load the college scorecard data:
- - 1 argument to pass

python load-scorecard.py raw scorecard file name

example: python load-scorecard.py MERGED2018_19_PP.csv

- To load the IPEDS data:
-- 1 argument to pass
  
python load_ipeds.py raw IPEDS file name

example: python load_ipeds.py hd2019.csv

- To load the final tables (defined by our schema):
  --2 arguments to pass: The year that you want data for, and whether to rebuild the tables from scratch (true/false). Unless there is a data corruption problem, you should only need to build the table once, and then append additional years of data onto it.  
  
python load-schema.py year flag_to_generate_tables

one year example: python load-schema.py 2019 True
all years example: python load-schema.py 2019 True; python load-schema.py 2020 False; python load-schema.py 2021 False; python load-schema.py 2022 False

valid years: 2019, 2020, 2021, 2022
