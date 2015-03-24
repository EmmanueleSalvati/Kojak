"""FIPS COUNTRY CODES SCRAPER"""

import csv
import urllib2
from BeautifulSoup import BeautifulSoup

opener = urllib2.build_opener()
opener.addheaders = [('User-agent', 'Mozilla/5.0')]

url = 'http://en.wikipedia.org/wiki/List_of_FIPS_country_codes'

page = opener.open(url)
soup = BeautifulSoup(page.read())

full_table = soup.findAll('table', {'class': 'wikitable sortable'})

fips_csv = csv.writer(open('wikipedia-fips-codes.csv', 'w'))
fips_csv.writerow(['Code', 'Short-form name'])

for table in full_table:
    # Iterate over the table pulling out the country table results.
    # Skip the first  row as it contains the already-parsed header information.
    for row in table.findAll("tr"):  # [1:]:
        tds = row.findAll('td')
        if len(tds) == 2:
            print tds
            # raw_cols = [td.findAll(text=True) for td in tds]
            # cols = []
            code = str(tds[0].find(text=True))
            country = str(tds[1].find('a').find(text=True))
            # country field contains differing numbers of elements, due to the flag --
            # only take the name
            # cols.append(raw_cols[0][-1:][0])
            # for all other columns, use the first result text
            # cols.extend([col[0] for col in raw_cols[1:]])
            fips_csv.writerow([code, country])
