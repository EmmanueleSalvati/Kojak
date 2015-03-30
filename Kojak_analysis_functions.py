"""Function for news analysis of the Kojak project"""

import pandas as pd
import GDELT_loader as loader
import numpy as np
import math
from collections import OrderedDict
pd.set_option('display.multi_sparse', False)

WEST_EUROPE = ('Belgium', 'Denmark', 'Finland', 'France', 'Germany', 'Greece',
               'Italy', 'Luxembourg', 'Netherlands', 'Norway', 'Portugal',
               'Spain', 'Sweden', 'United Kingdom')

NORTH_AMERICA = ('Canada', 'Mexico', 'United States')

CONTINENTS = {}
for country in WEST_EUROPE:
    CONTINENTS[country] = WEST_EUROPE
for country in NORTH_AMERICA:
    CONTINENTS[country] = NORTH_AMERICA


EVENTS = []
with open('events.txt', 'r') as f:
    for line in f:
        EVENTS.append(line.strip('\n'))


def filter_by_continent(df, continent_list):
    """Given list of countries, restrict the dataframe to the ones which have
    those countries in the DomainCountry column"""

    small_df = df.loc[df['DomainCountry'].isin(continent_list) == True, :]
    return small_df


def filter_by_event(df, event):
    """Given an event, restrict the dataframe to news which have that event"""

    small_df = df.loc[df['EVENTDESCRIPTION'] == event, :]

    return small_df


def filter_by_country_event(df, country, event):
    """Given an event and a country, restrict the dataframe to news which have
    'EVENTDESCRIPTION' == event & 'DomainCountry' == country"""

    cond1 = (df['DomainCountry'] == country)
    cond2 = (df['EVENTDESCRIPTION'] == event)

    return df.loc[cond1 & cond2, :]


def tf_idfs(df, continent):
    """For each country in the continent, create a dictionary of tf-idfs with
    all the 310 possible events
    supported continents so far: NORTH_AMERICA and WEST_EUROPE"""

    # idf 2
    alltopics_per_country = df.groupby('DomainCountry', sort=False).count().\
        to_dict()['EVENTDESCRIPTION']

    alltopics_world = df.groupby('EVENTDESCRIPTION').count().\
        to_dict()['DomainCountry']

    tfidfs_dict = {}
    for country in continent:
        print country
        # idf 2
        allevents = alltopics_per_country[country]

        country_dict = {}
        for event in EVENTS:

            tf = len(filter_by_country_event(df, country, event).index)

            if event not in alltopics_world:
                country_dict[event] = np.nan
            elif tf == 0:
                country_dict[event] = 0
            else:
                idf1 = alltopics_world[event]
                tfidf = float(tf) / idf1 / allevents
                country_dict[event] = tfidf

        print country_dict
        tfidfs_dict[country] = country_dict

    return tfidfs_dict


def tfidf_sort_dict(country_dict):
    """Takes a dictionary {'event1': count1, 'event2': count2} for a given
    country, gets rid of nan's and returns a dict sorted by count value"""

    sorted_dict = {}
    for k, v in country_dict.iteritems():
        if not math.isnan(v):
            sorted_dict[k] = country_dict[k]
    return OrderedDict(sorted(sorted_dict.items(),
                       key=lambda x: x[1], reverse=True))


if __name__ == '__main__':
    wtf = loader.load_csv("data", columns=loader.COLUMNS_AVGTONE)
    wtf.dropna(subset=['ActionCountry'], inplace=True)
    df = wtf.groupby(by='DomainCountry').agg([len, np.mean])
    df2 = df.xs('AvgTone', level=0, axis=1).sort('mean', ascending=False)
    df2.loc[filter(lambda x: x in funcs.WEST_EUROPE, df2.index), :]
