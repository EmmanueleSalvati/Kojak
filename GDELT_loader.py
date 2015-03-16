"""Load specified csv files into a dataframe"""

import pandas as pd
import numpy as np
from os import listdir

COLUMN_NAMES = ['GLOBALEVENTID', 'SQLDATE',
                'Actor1Code', 'Actor1Name', 'Actor1CountryCode',
                'Actor1KnownGroupCode', 'Actor1EthnicCode',
                'Actor1Religion1Code', 'Actor1Religion2Code',
                'Actor1Type1Code', 'Actor1Type2Code', 'Actor1Type3Code',
                'Actor2Code', 'Actor2Name', 'Actor2CountryCode',
                'Actor2KnownGroupCode', 'Actor2EthnicCode',
                'Actor2Religion1Code', 'Actor2Religion2Code',
                'Actor2Type1Code', 'Actor2Type2Code', 'Actor2Type3Code',
                'EventCode', 'GoldsteinScale', 'NumSources', 'AvgTone',
                'Actor1Geo_ADM1Code', 'Actor1Geo_FeatureID',
                'Actor2Geo_ADM1Code', 'Actor2Geo_FeatureID',
                'ActionGeo_ADM1Code', 'ActionGeo_FeatureID',
                'DATEADDED', 'SOURCEURL'
                ]

typeslist = [str,
             str, str, str,
             str, str,
             str, str,
             str, str, str,
             str, str, str,
             str, str,
             str, str,
             str, str, str,
             np.int64, np.float64, np.int64, np.float64,
             str, str,
             str, str,
             str, str,
             str, str]


typesdict = {}
for i in range(len(typeslist)):
    typesdict[COLUMN_NAMES[i+1]] = typeslist[i]


def load_csv(path):
    """Creates a DataFrame with all the csv files in the path directory"""

    df = pd.DataFrame()
    csvs = listdir(path)
    for csvfile in csvs:
        temp_df = pd.read_csv(path + '/' + csvfile, index_col='GLOBALEVENTID',
                              dtype=typesdict)
        df = pd.concat([df, temp_df], join='inner')
        print df.shape

    return df
