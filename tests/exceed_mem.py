import pandas as pd
from time import sleep


def exceed_mem():
    """This test takes a large draw file and reads it into a dataframe,
    concatenating it together and then summing across each column, this is
    intended to use about 700MB of memory so that we can test that it exceeds
     memory constraints on the fair cluster"""

    file = '/ihme/centralcomp/auto_test_data/burdenator/72/loc_agg_draws/burden/166/4/4_2005_166_104_2.h5'

    df = pd.read_hdf(file)
    for i in range(20):
        new_df = pd.read_hdf(file)
        df = pd.concat([new_df, df])

    df = pd.concat([df, df])
    cols = df.columns
    for col in cols:
        el = df[col].sum()

exceed_mem()
