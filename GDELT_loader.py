"""Load specified csv files into a dataframe"""

import pandas as pd
import numpy as np
from os import listdir

# the index is 'GLOBALEVENTID'
COLUMN_NAMES = ['SQLDATE',
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

TYPESLIST = [str,
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


TYPESDICT = {}
for i in range(len(TYPESLIST)):
    TYPESDICT[COLUMN_NAMES[i]] = TYPESLIST[i]


def load_csv(path, singlefile=None):
    """Creates a DataFrame with all the csv files in the path directory"""

    if singlefile:
        df = pd.read_csv(path + '/' + singlefile, dtype=TYPESDICT)
        df.set_index('GLOBALEVENTID', inplace=True)

        return df

    df = pd.DataFrame()
    csvs = listdir(path)
    for csvfile in csvs:
        temp_df = pd.read_csv(path + '/' + csvfile, dtype=TYPESDICT)
        df = pd.concat([df, temp_df], join='inner')
        print csvfile, df.shape
    df.set_index('GLOBALEVENTID', inplace=True)

    return df


def country_codes():
    countries = pd.read_table("countrynames.txt", sep='; ', header=None,
                              skiprows=23, encoding='utf-8', usecols=[0, 4])

    return countries


def country_name(code):
    codes_table = country_codes()

    return codes_table.loc[codes_table[0] == code, 4].iloc[0]
