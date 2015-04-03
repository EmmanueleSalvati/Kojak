"""Function for news analysis of the Kojak project"""

import pandas as pd
import numpy as np
import GDELT_loader as loader
import os
from math import log10, floor

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
with open('eventcodes/events.txt', 'r') as f:
    for line in f:
        EVENTS.append(line.strip('\n'))


def filter_by_continent(df, continent_list):
    """Given list of countries, restrict the dataframe to the ones which have
    those countries in the DomainCountry column"""

    small_df = df.loc[df['DomainCountry'].isin(continent_list) == True, :]
    return small_df


def world_all(df):
    """Sum of all the topics in the current world
    (NORTH_AMERICA or WEST_EUROPE)"""

    alltopics_world = df.groupby('EVENTDESCRIPTION').count().\
        to_dict()['DomainCountry']

    sumworld = 0
    for v in alltopics_world.itervalues():
        sumworld += v

    return sumworld


def country_all(df, country):
    """Sum of all the topics in the country"""

    events_dict = df.groupby('DomainCountry', sort=False).count().\
        to_dict()['EVENTDESCRIPTION']
    return events_dict[country]


def split_df_by_country(df, continent_list):
    """Returns a dict of dataframes: one per country"""

    dfs = {}
    for country in continent_list:
        dfs[country] = df.loc[df['DomainCountry'] == country, :]

    return dfs


def generic_df_event_count(df):
    """Returns a dataframe whose index is the events,
    with the count per events as columns"""

    # return df.groupby(by='EVENTDESCRIPTION').count()
    return df.groupby(by='EVENTDESCRIPTION').agg({'DomainCountry':
                                                 pd.Series.count,
                                                 'AvgTone': np.mean})


def final_df(df, dfs):
    """Takes the world df, and the dictionary of countries dfs, combines them
    into one big dataframe of event counts"""

    column_names = ['world tone', 'world count']
    big_df = generic_df_event_count(df)
    for country in dfs.iterkeys():
        country_df = generic_df_event_count(dfs[country])
        big_df = pd.merge(big_df, country_df, left_index=True,
                          right_index=True, how='left')
        column_names.extend([country + ' tone', country + ' count'])
        # column_names.append(country)
    big_df.columns = column_names

    return big_df


def tfids_df(finaldf):
    """Creates a dataframe of tfidfs"""

    all_counts = finaldf.sum()
    tfidfs = pd.DataFrame()
    print 'world:', all_counts['world']

    for country in finaldf.columns[1:]:
        print country, all_counts[country]
        tfidfs[country] = (finaldf[country] * all_counts['world']) /\
            (finaldf['world'] * all_counts[country])

    return tfidfs


def round_sig(x, sig=2):
    return round(x, sig-int(floor(log10(x)))-1)


def tfs_to_tsv(tfs_df):
    """Sorts the tfs and writes out a tsv per country"""

    for country in tfs_df.columns:
        tmp = tfs_df[country].order(ascending=False)
        tmp.dropna(inplace=True)
        if country == "United States":
            tmp = tmp.apply(round_sig, args=(5,))
        else:
            tmp = tmp.apply(round_sig)
        tsv_name = str(country + '.tsv')
        tsv_name = tsv_name.replace(" ", "")
        os.system('echo "Event\tvalue" > %s' % tsv_name)
        tmp.to_csv(('tmp_%s' % tsv_name), sep='\t')
        os.system('cat tmp_%s >> %s' % (tsv_name, tsv_name))
        os.system('rm -f tmp_%s' % tsv_name)

if __name__ == '__main__':
    big_df = loader.load_csv("data", columns=loader.COLUMNS_TFIDF)
    world = filter_by_continent(big_df, NORTH_AMERICA)
    dfs = split_df_by_country(world, NORTH_AMERICA)
    tfs = tfids_df(final_df)
    tfs_to_tsv(tfs)
