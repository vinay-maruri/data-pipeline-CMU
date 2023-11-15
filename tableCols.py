
import pandas as pd


def get_matching_cols(filename1, filename2):
    df1 = pd.read_csv(filename1, encoding='latin1')
    df2 = pd.read_csv(filename2, encoding='latin1')
    matching_cols = list(set(df1.columns) &
                         set([col.upper() for col in df2.columns]))
    return matching_cols
