"""Function for news analysis of the Kojak project"""

import pandas as pd
import GDELT_loader as loader
import numpy as np
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


def event_tfidf(df, country, event):
    """First restrict by continent,
    then calculate tfidf for that continent,
    returns a number"""

    tf = filter_by_country_event(df, country, event)
    idf1 = filter_by_event(df, event)
    print 'tf:', len(tf), 'idf1:', len(idf1), 'idf2:', len(idf2)

    if len(idf1) == 0 or len(idf2) == 0:
        return np.nan
    elif len(tf) == 0:
        return 0
    return float(len(tf)) / len(idf1) / len(idf2)


def tf_idfs(df, continent):
    """For each country in the continent, create a dictionary of tf-idfs with
    all the 310 possible events
    supported continents so far: NORTH_AMERICA and WEST_EUROPE"""

    # idf 2
    allevents_per_country = df.groupby('DomainCountry', sort=False).count().\
        to_dict()['EVENTDESCRIPTION']

    # idf 1
    # events_dict_by_country {}
    # for name, group in df.groupby('DomainCountry'):
    #     events_dict_by_country[name] = group.groupby(by='EVENTDESCRIPTION').\
    #         count().to_dict()['DomainCountry']
    events_dict = df.groupby('EVENTDESCRIPTION').count().\
        to_dict()['DomainCountry']

    tfidfs_list = []
    for country in continent:
        # idf 2
        allevents = allevents_per_country[country]

        country_dict = {}
        for event in EVENTS:
            tf = len(filter_by_country_event(df, country, event))

            if allevents == 0 or event not in events_dict:
                print country, event, np.nan
                country_dict[event] = np.nan
            elif tf == 0:
                print country, event, 0
                country_dict[event] = 0
            else:
                idf1 = events_dict[event]
                print country, event, float(tf) / idf1 / allevents
                country_dict[event] = float(tf) / idf1 / allevents

            # event_tfidf(df, country, event, continent)
        tfidfs_list.append(country_dict)

    return tfidfs_list

if __name__ == '__main__':
    wtf = loader.load_csv("data", columns=loader.COLUMNS_AVGTONE)
    wtf.dropna(subset=['ActionCountry'], inplace=True)
    df = wtf.groupby(by='DomainCountry').agg([len, np.mean])
    df2 = df.xs('AvgTone', level=0, axis=1).sort('mean', ascending=False)
    df2.loc[filter(lambda x: x in funcs.WEST_EUROPE, df2.index), :]
