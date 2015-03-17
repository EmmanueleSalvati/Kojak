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

#  This is the index: 'GLOBALEVENTID',
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

gdelt_base_url = 'http://data.gdeltproject.org/events/'
local_path = '/Users/JerkFace/Metis/Projects/Kojak/'

# Load the table of countries' news
SOURCE_DF = pd.read_table('DOMAINSBYCOUNTRY-ENGLISH.TXT')
SOURCE_DF.drop(['FIPSCountryCode'], axis=1, inplace=True)


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


def my_parser(in_file_name):
    """Load the infile, get the columns that I need, adds the Domain column,
    uses only the rows which have a URL from a source contained in SOURCE_DF,
    write an outfile, trash the infile"""

    colnames = pd.read_excel('CSV.header.fieldids.xlsx', sheetname='Sheet1',
                             index_col='Column ID', parse_cols=1)['Field Name']

    with open(in_file_name, 'r') as infile:
        full_df = pd.read_table(infile, names=colnames,  # index_col=0,
                                usecols=COLUMN_NAMES)
    full_df['Domain'] = full_df['SOURCEURL'].apply(url_domain)
    full_df = pd.merge(full_df, SOURCE_DF,
                       on='Domain', how='inner', left_index=True)
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
        print 'extracting,\n',
        z = zipfile.ZipFile(file=local_path + 'tmp/' + compressed_file,
                            mode='r')
        print z.namelist(),
        for name in z.namelist():
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
