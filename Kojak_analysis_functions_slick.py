"""Function for news analysis of the Kojak project"""

import pandas as pd
import numpy as np
pd.set_option('display.multi_sparse', False)

WEST_EUROPE = ('Belgium', 'Denmark', 'Finland', 'France', 'Germany', 'Greece',
               'Italy', 'Luxembourg', 'Netherlands', 'Norway', 'Portugal',
               'Spain', 'Sweden', 'United Kingdom')

# NORTH_AMERICA = ('Canada', 'Mexico', 'United States')
NORTH_AMERICA = ('Canada', 'United States')

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

    return df.groupby(by='EVENTDESCRIPTION').count()


def final_df(df, dfs):
    """Takes the world df, and the dictionary of countries dfs, combines them
    into one big dataframe of event counts"""

    column_names = ['world']
    big_df = generic_df_event_count(df)
    for country in dfs.iterkeys():
        country_df = generic_df_event_count(dfs[country])
        big_df = pd.merge(big_df, country_df, left_index=True,
                          right_index=True, how='left')
        column_names.append(country)
    big_df.columns = column_names

    return big_df


def tfids_df(finaldf):
    """Creates a dataframe of tfidfs"""

    all_counts = finaldf.sum()
    tfidfs = pd.DataFrame()
    for country in finaldf.columns[1:]:
        print 'world:', all_counts['world']
        print country, all_counts[country]
        tfidfs[country] = (finaldf[country] * all_counts['world']) /\
            (finaldf['world'] * all_counts[country])

    return tfidfs

