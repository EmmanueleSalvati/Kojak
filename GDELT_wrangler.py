"""Data wrangler for GDELT data

Happily taken from:
http://nbviewer.ipython.org/github/JamesPHoughton/Published_Blog_Scripts/blob/master/GDELT%20Wrangler%20-%20Clean.ipynb

Thank you!
"""

import pandas as pd
import numpy as np
from dateutil import parser
import requests
import lxml.html as lh
import os.path
import urllib
import zipfile
import glob
import re

COLUMN_NAMES = ['GLOBALEVENTID', 'SQLDATE',
                'Actor1Name', 'Actor1CountryCode',
                'Actor1KnownGroupCode', 'Actor1EthnicCode',
                'Actor1Religion1Code', 'Actor1Religion2Code',
                'Actor1Type1Code', 'Actor1Type2Code', 'Actor1Type3Code',
                'Actor2Name', 'Actor2CountryCode',
                'Actor2KnownGroupCode', 'Actor2EthnicCode',
                'Actor2Religion1Code', 'Actor2Religion2Code',
                'Actor2Type1Code', 'Actor2Type2Code', 'Actor2Type3Code',
                'EventCode', 'GoldsteinScale', 'NumSources', 'AvgTone',
                'ActionGeo_CountryCode',
                'DATEADDED', 'SOURCEURL'
                ]

TYPESLIST = [str, str,
             str, str,
             str, str,
             str, str,
             str, str, str,
             str, str,
             str, str,
             str, str,
             str, str, str,
             str, np.float64, np.int64, np.float64,
             str,
             str, str]

TYPESDICT = {}
for i in range(len(TYPESLIST)):
    TYPESDICT[COLUMN_NAMES[i]] = TYPESLIST[i]

gdelt_base_url = 'http://data.gdeltproject.org/events/'
local_path = '/Users/JerkFace/Metis/Projects/Kojak/'

# Load the table of countries' news
SOURCE_DF = pd.read_table('DOMAINSBYCOUNTRY-ENGLISH.TXT')
SOURCE_DF.drop(['FIPSCountryCode'], axis=1, inplace=True)
SOURCE_DF.set_index('Domain', inplace=True)
SOURCE_DICT = SOURCE_DF.to_dict()['CountryHumanName']

CAMEO_DF = pd.read_table('CAMEO.eventcodes.txt', dtype=str)

CAMEO_TYPE_DF = pd.read_table('CAMEO.type.txt', dtype=str)
CAMEO_KNOWNGROUP_DF = pd.read_table('CAMEO.knowngroup.txt', dtype=str)
CAMEO_ETHNIC_DF = pd.read_table('CAMEO.ethnic.txt', dtype=str)
CAMEO_RELIGION_DF = pd.read_table('CAMEO.religion.txt', dtype=str)
CAMEO_COUNTRY_DF = pd.read_table('CAMEO.country.txt', dtype=str)

FIPS_CODES = pd.read_csv('wikipedia-fips-codes.csv')
ISO_CODES = pd.read_csv('wikipedia-iso-country-codes.csv')


def url_domain(url):
    """Get the domain from a given url,
    e.g. http://100r.org becomes 100r.org"""

    match = re.search(r'(https?)://(.+)', url)
    if match:
        url = match.group(2)

    wmatch = re.search(r'(www)\.(.+)', url)
    if wmatch:
        url = wmatch.group(2)

    if '/' in url:
        domain = url.split('/')[0]
        return domain
    else:
        return url


def min_date(early, current):
    begin_date = parser.parse(early)
    current_date = parser.parse(current)

    if current_date < begin_date:
        return False
    else:
        return True


def filelist(mindate):
    """Get the file list to download"""

    # get the list of all the links on the gdelt file page
    page = requests.get(gdelt_base_url+'index.html')
    doc = lh.fromstring(page.content)
    link_list = doc.xpath("//*/ul/li/a/@href")

    # separate out those links that begin with four digits
    file_list = [x for x in link_list if str.isdigit(x[0:4]) and
                 x.startswith('2015') and
                 min_date(mindate, x.split('.export')[0])]

    return file_list


def key_in_dict(key, dictionary):
    if key in dictionary:
        return dictionary[key]


def match_columns(df):
    """Match all the CAMEO and country codes in the df;
    discard the useless columns

    It is a long function, but it repeats the same thing over and over again,
    on different columns"""

    # Match country code of actors
    countries1 = pd.merge(df, CAMEO_COUNTRY_DF, left_on='Actor1CountryCode',
                          right_on='CODE', how='left', left_index=True)
    countries1['Actor1Country'] = countries1['LABEL']
    countries1.drop(['CODE', 'LABEL', 'Actor1CountryCode'],
                    axis=1, inplace=True)
    countries2 = pd.merge(countries1, CAMEO_COUNTRY_DF,
                          left_on='Actor2CountryCode', right_on='CODE',
                          how='left', left_index=True)
    countries2['Actor2Country'] = countries2['LABEL']
    countries2.drop(['CODE', 'LABEL', 'Actor2CountryCode'],
                    axis=1, inplace=True)
    del countries1, df

    # Match type code of actors
    type1 = pd.merge(countries2, CAMEO_TYPE_DF, left_on='Actor1Type1Code',
                     right_on='CODE', how='left', left_index=True)
    type1['Actor1Type1'] = type1['LABEL']
    type1.drop(['CODE', 'LABEL', 'Actor1Type1Code'], axis=1, inplace=True)
    type2 = pd.merge(type1, CAMEO_TYPE_DF, left_on='Actor2Type1Code',
                     right_on='CODE', how='left', left_index=True)
    if 'LABEL' in type2:
        type2['Actor2Type1'] = type2['LABEL']
        type2.drop(['LABEL', 'CODE'], axis=1, inplace=True)
    else:
        type2['Actor2Type1'] = np.nan
    type2.drop('Actor2Type1Code', axis=1, inplace=True)
    del countries2, type1

    # Match group code of actors
    group1 = pd.merge(type2, CAMEO_KNOWNGROUP_DF,
                      left_on='Actor1KnownGroupCode',
                      right_on='CODE', how='left', left_index=True)
    group1['Actor1KnownGroup'] = group1['LABEL']
    group1.drop(['CODE', 'LABEL', 'Actor1KnownGroupCode'], axis=1,
                inplace=True)
    group2 = pd.merge(group1, CAMEO_KNOWNGROUP_DF,
                      left_on='Actor2KnownGroupCode',
                      right_on='CODE', how='left', left_index=True)
    if 'LABEL' in group2:
        group2['Actor2KnownGroup'] = group2['LABEL']
        group2.drop(['LABEL', 'CODE'], axis=1, inplace=True)
    else:
        group2['Actor2KnownGroup'] = np.nan
    group2.drop('Actor2KnownGroupCode', axis=1,
                inplace=True)
    del type2, group1

    # Match ethnic code of actors
    ethnic1 = pd.merge(group2, CAMEO_ETHNIC_DF,
                       left_on='Actor1EthnicCode',
                       right_on='CODE', how='left', left_index=True)
    ethnic1['Actor1Ethnic'] = ethnic1['LABEL']
    ethnic1.drop(['CODE', 'LABEL', 'Actor1EthnicCode'], axis=1, inplace=True)
    ethnic2 = pd.merge(ethnic1, CAMEO_ETHNIC_DF,
                       left_on='Actor2EthnicCode',
                       right_on='CODE', how='left', left_index=True)
    if 'LABEL' in ethnic2:
        ethnic2['Actor2Ethnic'] = ethnic2['LABEL']
        ethnic2.drop(['LABEL', 'CODE'], axis=1, inplace=True)
    else:
        ethnic2['Actor2Ethnic'] = np.nan
    ethnic2.drop('Actor2EthnicCode', axis=1, inplace=True)
    del group2, ethnic1

    # Match religion code of actors
    religion1 = pd.merge(ethnic2, CAMEO_RELIGION_DF,
                         left_on='Actor1Religion1Code',
                         right_on='CODE', how='left', left_index=True)
    if 'LABEL' in religion1:
        religion1['Actor1Religion1'] = religion1['LABEL']
        religion1.drop(['LABEL', 'CODE'], axis=1, inplace=True)
    else:
        religion1['Actor1Religion1'] = np.nan
    religion1.drop('Actor1Religion1Code', axis=1,
                   inplace=True)
    religion2 = pd.merge(religion1, CAMEO_RELIGION_DF,
                         left_on='Actor2Religion1Code',
                         right_on='CODE', how='left', left_index=True)
    if 'LABEL' in religion2:
        religion2['Actor2Religion1'] = religion2['LABEL']
        religion2.drop(['LABEL', 'CODE'], axis=1, inplace=True)
    else:
        religion2['Actor2Religion1'] = np.nan
    religion2.drop('Actor2Religion1Code', axis=1,
                   inplace=True)
    del ethnic2, religion1

    # Match Type2 code of actors
    sectype1 = pd.merge(religion2, CAMEO_TYPE_DF, left_on='Actor1Type2Code',
                        right_on='CODE', how='left', left_index=True)
    if 'LABEL' in sectype1:
        sectype1['Actor1Type2'] = sectype1['LABEL']
        sectype1.drop(['LABEL', 'CODE'], axis=1, inplace=True)
    else:
        sectype1['Actor1Type2'] = np.nan
    sectype1.drop('Actor1Type2Code', axis=1, inplace=True)
    sectype2 = pd.merge(sectype1, CAMEO_TYPE_DF, left_on='Actor2Type2Code',
                        right_on='CODE', how='left', left_index=True)
    if 'LABEL' in sectype2:
        sectype2['Actor2Type2'] = sectype2['LABEL']
        sectype2.drop(['LABEL', 'CODE'], axis=1, inplace=True)
    else:
        sectype2['Actor2Type2'] = np.nan
    sectype2.drop('Actor2Type2Code', axis=1, inplace=True)
    del religion2, sectype1

    # Match Type3 code of actors
    thirdtype1 = pd.merge(sectype2, CAMEO_TYPE_DF, left_on='Actor1Type3Code',
                          right_on='CODE', how='left', left_index=True)
    if 'LABEL' in thirdtype1:
        thirdtype1['Actor1Type3'] = thirdtype1['LABEL']
        thirdtype1.drop(['LABEL', 'CODE'], axis=1, inplace=True)
    else:
        thirdtype1['Actor1Type3'] = np.nan
    thirdtype1.drop('Actor1Type3Code', axis=1, inplace=True)
    thirdtype2 = pd.merge(thirdtype1, CAMEO_TYPE_DF, left_on='Actor2Type3Code',
                          right_on='CODE', how='left', left_index=True)
    if 'LABEL' in thirdtype2:
        thirdtype2['Actor2Type3'] = thirdtype2['LABEL']
        thirdtype2.drop(['LABEL', 'CODE'], axis=1, inplace=True)
    else:
        thirdtype2['Actor2Type3'] = np.nan
    thirdtype2.drop('Actor2Type3Code', axis=1, inplace=True)
    del sectype2, thirdtype1

    # Match Religion2 code of actors
    secrel1 = pd.merge(thirdtype2, CAMEO_RELIGION_DF,
                       left_on='Actor1Religion2Code',
                       right_on='CODE', how='left', left_index=True)
    if 'LABEL' in secrel1:
        secrel1['Actor1Religion2'] = secrel1['LABEL']
        secrel1.drop(['LABEL', 'CODE'], axis=1, inplace=True)
    else:
        secrel1['Actor1Religion2'] = np.nan
    secrel1.drop('Actor1Religion2Code', axis=1, inplace=True)
    secrel2 = pd.merge(secrel1, CAMEO_RELIGION_DF,
                       left_on='Actor2Religion2Code',
                       right_on='CODE', how='left', left_index=True)
    if 'LABEL' in secrel2:
        secrel2['Actor2Religion2'] = secrel2['LABEL']
        secrel2.drop(['LABEL', 'CODE'], axis=1, inplace=True)
    else:
        secrel2['Actor2Religion2'] = np.nan
    secrel2.drop('Actor2Religion2Code', axis=1, inplace=True)
    del thirdtype2, secrel1

    # Match country code of actions
    countrycode = pd.merge(secrel2, FIPS_CODES,
                           left_on='ActionGeo_CountryCode',
                           right_on='Code', how='left', left_index=True)
    if 'Short-form name' in countrycode:
        countrycode['ActionCountry'] = countrycode['Short-form name']
        countrycode.drop(['Code', 'Short-form name', 'ActionGeo_CountryCode'],
                         axis=1, inplace=True)
    else:
        countrycode['ActionCountry'] = np.nan
    del secrel2

    # Match Event Code
    final = pd.merge(countrycode, CAMEO_DF, left_on='EventCode',
                     right_on='CAMEOEVENTCODE', how='left', left_index=True)
    if 'EVENTDESCRIPTION' in final:
        final.drop(['EventCode', 'CAMEOEVENTCODE'], axis=1, inplace=True)
    del countrycode

    return final


def country_name(code):
    """Simple function to understand a country code"""

    fips_codes = pd.read_csv('wikipedia-fips-codes.csv')
    iso_codes = pd.read_csv('wikipedia-iso-country-codes.csv')

    # print 'code:', code, 'type:', type(code)
    if type(code) == float and np.isnan(code):
        return np.nan

    if code == 'UK':
        code = 'GB'

    if code == 'VM':
        code = 'VN'

    if code == 'RB':
        code = 'RS'

    if code == 'OS':
        return 'Arafura Sea'

    if code == 'OC':
        return 'Indian Ocean'

    # if len(code) == 3:
    #     country = wiki_codes.loc[wiki_codes['Alpha-3 code'] == code,
    #                              'English short name (upper/lower case)']\
    #                              .iloc[0]
    # else:
    if len(iso_codes[iso_codes['Alpha-2 code'] == code]) > 0:
        country = iso_codes.loc[iso_codes['Alpha-2 code'] == code,
                                'English short name (upper/lower case)']\
                                .iloc[0]
    else:  # len(fips_codes[fips_codes['Code'] == code]) > 0:
        country = fips_codes.loc[fips_codes['Code'] == code,
                                 'Short-form name'].iloc[0]
    # else:
    #     return np.nan

    return country


def my_parser(in_file_name):
    """Load the infile, get the columns that I need, adds the Domain column,
    uses only the rows which have a URL from a source contained in SOURCE_DF,
    write an outfile, trash the infile"""

    colnames = pd.read_excel('CSV.header.fieldids.xlsx', sheetname='Sheet1',
                             index_col='Column ID', parse_cols=1)['Field Name']

    with open(in_file_name, 'r') as infile:
        full_df = pd.read_table(infile, names=colnames,
                                usecols=COLUMN_NAMES,
                                dtype=TYPESDICT)

    # Add country that produced the news
    full_df['DomainCountry'] = full_df['SOURCEURL'].apply(url_domain)
    full_df['DomainCountry'] = full_df['DomainCountry'].\
        apply(key_in_dict, args=(SOURCE_DICT, ))

    # Expand all the codes
    full_df = match_columns(full_df)
    full_df.set_index("GLOBALEVENTID", inplace=True)

    return full_df


def check_file(filename):
    """Check whether the zip or csv files already exist in tmp/ or data/"""

    if os.path.isfile(local_path + 'tmp/' + filename) or\
       os.path.isfile(local_path + 'data/' + filename.strip('.zip')):
        return True

    return False


def download_files(file_list):
    """download the files in filelist"""

    for compressed_file in file_list:
        print compressed_file,

        if check_file(compressed_file):
            continue

        print 'downloading,',
        urllib.urlretrieve(url=gdelt_base_url + compressed_file,
                           filename=local_path + 'tmp/' + compressed_file)

        # extract the contents of the compressed file to a temporary directory
        z = zipfile.ZipFile(file=local_path + 'tmp/' + compressed_file,
                            mode='r')
        for name in z.namelist():
            print 'extracting', name,
            z.extract(name, path=local_path + 'tmp/')

            # parse each of the csv files in the working directory,
            # THIS IS WHERE ALL THE CHOPPING, MERGING, ETC. HAPPENS
            print 'parsing,',
            infile_name = 'tmp/' + name
            df = my_parser(infile_name)

            print 'saving to data/',
            outfile_name = local_path+'data/' + name
            with open(outfile_name, 'w') as outfile:
                df.to_csv(outfile_name)

            print 'removing tmp files',
            os.remove(local_path + 'tmp/' + name)

        os.remove(local_path + 'tmp/' + compressed_file)
        print 'done'
