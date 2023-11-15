
# Code looking for columns that are present in both our database and whichever file we're trying to load

import pandas as pd

# read in hd2019.csv and DBcols.csv
hd2019 = pd.read_csv('hd2019.csv', encoding='latin1')
DBcols = pd.read_csv('DBcols.csv', encoding='latin1')

# find the columns that are present in both
matching_cols = list(set(hd2019.columns) & set([col.upper() for col in DBcols.columns]))

print(hd2019.columns)
print(DBcols.columns)
print(matching_cols)

MERGED2018_19_PP = pd.read_csv('MERGED2018_19_PP.csv', encoding='latin1')
matching_cols2 = list(set(MERGED2018_19_PP.columns) & set([col.upper() for col in DBcols.columns]))

print(MERGED2018_19_PP.columns)
print(DBcols.columns)
print(matching_cols2)

# find the columns that are in DBcols but not in matching_cols or matching_cols2
not_matching_cols = [col for col in DBcols.columns if col.upper() not in matching_cols and col.upper() not in matching_cols2]

print(not_matching_cols)
