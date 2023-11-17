# data-pipeline-CMU


Instructions to run: 

- Create a database connection using the server account credentials provided to you. You will need to create a separate credentials.py file and store your username/password there. You will also have to make sure that youre connected to a DB that you can access. 

- To load the college scorecard data:
- - 1 argument to pass

python load-scorecard.py raw scorecard file name

- To load the IPEDS data:
-- 1 argument to pass
  
python load-scorecard.py raw IPEDS file name

- To load the final tables (defined by our schema):
  --2 arguments to pass
  
python load-schema.py [year range of analysis/data files] final table name to generate from InstitutionInformation, Debt, StudentBody, StudentOutcomes



timecards sheet: https://docs.google.com/spreadsheets/d/1rTJWASwnHNq7ZSrI6INrh1z1rssZ8Vs91-evc56JbSQ/edit?usp=sharing

project notes master document: https://docs.google.com/document/d/122vHfMRxXLKWhUTRgul8CLrdzfFJjwUiFIX7puxCojM/edit?usp=sharing
