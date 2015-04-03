"""Load specified csv files into a dataframe"""

import pandas as pd
import numpy as np
from os import listdir

TYPESDICT = {}
TYPESDICT['GLOBALEVENTID'] = str
TYPESDICT['SQLDATE'] = str
TYPESDICT['Actor1Name'] = str
TYPESDICT['Actor2Name'] = str
TYPESDICT['GoldsteinScale'] = np.float64
TYPESDICT['NumSources'] = np.int64
TYPESDICT['AvgTone'] = np.float64
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

COLUMNS = [u'GLOBALEVENTID', u'SQLDATE', u'Actor1Name', u'Actor2Name',
           u'GoldsteinScale', u'NumSources', u'AvgTone', u'DATEADDED',
           u'SOURCEURL', u'DomainCountry', u'Actor1Country', u'Actor2Country',
           u'Actor1Type1', u'Actor2Type1',
           u'Actor1KnownGroup', u'Actor2KnownGroup',
           u'Actor1Ethnic', u'Actor2Ethnic',
           u'Actor1Religion1', u'Actor2Religion1',
           u'Actor1Type2', u'Actor2Type2', u'Actor1Type3', u'Actor2Type3',
           u'Actor1Religion2', u'Actor2Religion2',
           u'ActionCountry', u'EVENTDESCRIPTION']

COLUMNS_AVGTONE = [u'ActionCountry', u'EVENTDESCRIPTION',
                   u'AvgTone', u'DomainCountry', u'SOURCEURL']

COLUMNS_TFIDF = [u'EVENTDESCRIPTION', u'DomainCountry']
COLUMNS_TFIDF_debug = [u'EVENTDESCRIPTION', u'DomainCountry', u'AvgTone']


def load_csv(path, singlefile=None, columns=COLUMNS):
    """Creates a DataFrame with all the csv files in the path directory"""

    if singlefile:
        df = pd.read_csv(path + '/' + singlefile, dtype=TYPESDICT,
                         usecols=columns)

        df.dropna(subset=['DomainCountry'], inplace=True)
        return df

    df = pd.DataFrame()
    csvs = listdir(path)
    for csvfile in csvs:
        temp_df = pd.read_csv(path + '/' + csvfile, dtype=TYPESDICT,
                              usecols=columns)
        df = pd.concat([df, temp_df], join='inner')
    df.dropna(subset=['DomainCountry'], inplace=True)

    return df
