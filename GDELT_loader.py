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
                ''
                'EventCode', 'GoldsteinScale', 'NumSources', 'AvgTone',
                # 'Actor1Geo_ADM1Code', 'Actor1Geo_FeatureID',
                # 'Actor2Geo_ADM1Code', 'Actor2Geo_FeatureID',
                # 'ActionGeo_ADM1Code', 'ActionGeo_FeatureID',
                'ActionCountry',
                'DATEADDED', 'SOURCEURL'
                ]

# TYPESLIST = [str, str, str, str, str, str, str, str, str, str, str, str, str,
#              str, str, str, str, str, str, str, str, str, np.float64,
#              np.int64, np.float64, str, str, str, str, str, str, str, str]

TYPESDICT = {}
TYPESDICT['GLOBALEVENTID'] = str
TYPESDICT['SQLDATE'] = str
TYPESDICT['Actor1Name'] = str
TYPESDICT['Actor2Name'] = str
TYPESDICT['GoldsteinScale'] = np.float64
TYPESDICT['NumSources'] = np.int64
TYPESDICT['AvgTone'] = np.float64
# TYPESDICT['Actor1Geo_ADM1Code'] = str
# TYPESDICT['Actor1Geo_FeatureID'] = str
# TYPESDICT['Actor2Geo_ADM1Code'] = str
# TYPESDICT['Actor2Geo_FeatureID'] = str
# TYPESDICT['ActionGeo_ADM1Code'] = str
# TYPESDICT['ActionGeo_FeatureID'] = str
TYPESDICT['DATEADDED'] = str
TYPESDICT['SOURCEURL'] = str
TYPESDICT['DomainCountry'] = str
TYPESDICT['Actor1Country'] = str
TYPESDICT['Actor2Country'] = str
TYPESDICT['Actor1Type1'] = str
TYPESDICT['Actor2Type1'] = str
TYPESDICT['Actor1KnownGroup'] = str
TYPESDICT['Actor2KnownGroup'] = str
TYPESDICT['Actor1Ethnic'] = str
TYPESDICT['Actor2Ethnic'] = str
TYPESDICT['Actor1Religion1'] = str
TYPESDICT['Actor2Religion1'] = str
TYPESDICT['Actor1Type2'] = str
TYPESDICT['Actor2Type2'] = str
TYPESDICT['Actor1Type3'] = str
TYPESDICT['Actor2Type3'] = str
TYPESDICT['Actor1Religion2'] = str
TYPESDICT['Actor2Religion2'] = str
TYPESDICT['ActionCountry'] = str
TYPESDICT['EVENTDESCRIPTION'] = str


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
