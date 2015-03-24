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
    for row in table.findAll("tr"):  # [1:]:
        tds = row.findAll('td')
        if len(tds) == 2:
            code = str(tds[0].find(text=True))
            country = str(tds[1].find('a').find(text=True))
            fips_csv.writerow([code, country])
