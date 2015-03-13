"""Data wrangler for GDELT data

Happily taken from:
http://nbviewer.ipython.org/github/JamesPHoughton/Published_Blog_Scripts/blob/master/GDELT%20Wrangler%20-%20Clean.ipynb

Thank you!
"""

import pandas as pd
import requests
import lxml.html as lh
import os.path
import urllib
import zipfile
import glob
import operator
import sys

#  'GLOBALEVENTID',
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

gdelt_base_url = 'http://data.gdeltproject.org/events/'
local_path = '/Users/JerkFace/Metis/Projects/Kojak/'


def filelist():
    """Get the file list to download"""

    # get the list of all the links on the gdelt file page
    page = requests.get(gdelt_base_url+'index.html')
    doc = lh.fromstring(page.content)
    link_list = doc.xpath("//*/ul/li/a/@href")

    # separate out those links that begin with four digits
    file_list = [x for x in link_list if str.isdigit(x[0:4]) and
                 x.startswith('201503')]

    return file_list


def my_parser(in_file_name):
    """Load the infile, get the columns that I need, write an outfile, trash
    the infile"""

    colnames = pd.read_excel('CSV.header.fieldids.xlsx', sheetname='Sheet1',
                             index_col='Column ID', parse_cols=1)['Field Name']

    with open(in_file_name, 'r') as infile:
        full_df = pd.read_table(infile, names=colnames, index_col=0,
                                usecols=COLUMN_NAMES)

    return full_df


def download_files(file_list):
    """download the files in filelist"""

    infilecounter = 0
    outfilecounter = 0

    for compressed_file in file_list[infilecounter:]:
        print compressed_file

        # if we dont have the compressed file stored locally, go get it.
        # Keep trying if necessary.
        while not os.path.isfile(local_path + 'tmp/' + compressed_file):
            print 'downloading,',
            urllib.urlretrieve(url=gdelt_base_url+compressed_file,
                               filename=local_path + 'tmp/' + compressed_file)

        # extract the contents of the compressed file to a temporary directory
        print 'extracting,',
        z = zipfile.ZipFile(file=local_path + 'tmp/' + compressed_file,
                            mode='r')
        for name in z.namelist():
            z.extract(name, path=local_path + 'tmp/')

            # parse each of the csv files in the working directory,
            print 'parsing,',
            infile_name = 'tmp/' + name
            df = my_parser(infile_name)

            print 'saving to data/'
            outfile_name = local_path+'data/' + name
            with open(outfile_name, 'w') as outfile:
                df.to_csv(outfile_name)

            for filetoremove in glob.glob(local_path + 'tmp/*'):
                os.remove(filetoremove)

        #     # open the infile and outfile
        #     with open(infile_name, mode='r') as infile, open(outfile_name, mode='w') as outfile:
        #         for line in infile:
        #             # extract lines with our interest country code
        #             if fips_country_code in operator.itemgetter(51, 37, 44)(line.split('\t')):
        #                 outfile.write(line)
        #         outfilecounter +=1

        #     # delete the temporary file
        #     os.remove(infile_name)
        # infilecounter +=1
        print 'done'
