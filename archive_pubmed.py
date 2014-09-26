#!/usr/bin/env python
import logging
import sys
import json

import futures
from Bio.Medline import parse as medline_record_generator
import requests
from bs4 import BeautifulSoup
import dateutil.parser
from internetarchive import get_item
import cStringIO


__title__ = 'pubmed arxivr'
__version__ = '0.0.1'
__author__ = 'Jake Johnson'
__license__ = 'AGPL 3'
__copyright__ = 'Copyright 2014 Internet Archive'


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

fh = logging.FileHandler('pubmed.log')
fh.setLevel(logging.INFO)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
log.addHandler(fh)
log.addHandler(ch)


def get_soup(url, params=None):
    r = requests.get(url, params=params)
    return BeautifulSoup(r.content)

def get_doi(record):
    if 'AID' in record:
        for aid in record['AID']:
            if 'doi' in aid:
                return aid.split()[0].split('/')[-1]
        for aid in record['AID']:
            if 'pii' in aid:
                return aid.split()[0].split('/')[-1]

def get_pdf_link(soup):
    for div in soup.find_all('div', 'format-menu'):
        for link in div.find_all('a'):
            if link.attrs.get('href', '').endswith('.pdf'):
                url = 'http://www.ncbi.nlm.nih.gov{0}'.format(link.attrs['href'])
                return url

def get_md(record, soup=None):
    def get_date():
        # First try to to use the "Date of Electronic Publication".
        raw_date = record.get('DEP')
        # If that doesn't exist, check the "Publication History Status".
        if not raw_date:
            try:
                raw_date = record.get('PHST', '')[-1].split()[0]
            except IndexError:
                raw_date = None

        # If we have a date string to work with at this point, chances
        # are it's a string that can be parsed into an iso date.
        if raw_date:
            try:
                parsed_date = dateutil.parser.parse(raw_date)
                return parsed_date.isoformat().split('T')[0]
            except ValueError:
                raw_date = None

        # Shoot, best chance we have left is to extract the year from
        # "Date of Electronic Publication" or "Date of Publication".
        if not raw_date:
            raw_date = record.get('DEP', '')[:4]
        if not raw_date:
            raw_date = record.get('DP', '').split()[0]
        return raw_date

    def get_language():
        language = record.get('LA', 'eng')
        if isinstance(language, list):
            language = language[0]
        return language

    def get_description():
        abstract = record.get('AB')
        description = ''
        if record.get('JT'):
            description += (
                'This article is from '
                '<a href="//archive.org/search.php?query=journaltitle%3A%28{0}%29">{0}'
                '</a>'.format(record['JT'])
            )
        if description and record.get('VI'):
            description += (
                ', <a href="//archive.org/search.php?query=journaltitle%3A%28{0}%29%20'
                'AND%20volume%3A%28{1}%29">volume {1}</a>.'.format(record['JT'], record['VI'])
            )
        elif description:
            description += '.'

        description += """<h2>Abstract</h2>{0}""".format(abstract)
        return description

    def get_external_identifiers():
        external_identifiers = []
        for id in record.get('AID', []):
            external_identifier = '{type}:{eid}'.format(type=id.split()[-1].strip('[]'),
                                                        eid=id.split()[0])
            external_identifiers.append(external_identifier)
        if not external_identifiers:
            log.warning('could not find external-identifiers')
        return external_identifiers

    def get_contributor():
        if not soup:
            return
        for div in soup.find_all('div', 'courtesy-note'):
            for strong in div.find_all('strong'):
                if isinstance(strong.contents, list):
                    return strong.contents[0]
                else:
                    return strong.contents
        log.warning('could not find contributor')

    md = {
        'mediatype': 'texts',
        'collection': 'pubmed',
        'identifier': 'pubmed-{0}'.format(record['PMC']),

        'title': record.get('TI'),
        'creator': record.get('FAU', record.get('AU')),
        'date': get_date(),
        'language': get_language(),
        'description': get_description(),
        'source': 'http://www.ncbi.nlm.nih.gov/pmc/articles/{0}'.format(record['PMC']),
        'external-identifier': get_external_identifiers(),
        'journaltitle': record.get('JT'),
        'issn': record.get('IS'),
        'volume': record.get('VI'),
        'contributor': get_contributor(),
    }
    md = dict((k,v) for (k,v) in md.items() if v)
    return md

def archive_article(record):
    pmc = record.get('PMC')
    doi = get_doi(record)

    url = 'http://www.ncbi.nlm.nih.gov/pmc/articles/{pmc}'.format(pmc=pmc)
    soup = get_soup(url)

    # Get PDF URL.
    if pmc and doi:
        pdf_url = ('http://www.ncbi.nlm.nih.gov/pmc/articles/'
                   '{pmc}/pdf/{doi}.pdf'.format(pmc=pmc, doi=doi))
    # If we can't get a doi/pii from the medline record,
    # let's go scraping.
    elif pmc:
        pdf_url = get_pdf_link(soup)
    else:
        log.error('skipping, cannot parse PDF link: {0}'.format(record['PMC']))
        sys.exit(1)

    md = get_md(record, soup)
    item = get_item(md['identifier'])
    if item.exists:
        item.info('skipping, already exists: {0}'.format(item.identifier))
        return

    r = requests.get(pdf_url)
    r.raise_for_status()
    log.info('downloaded: {0}'.format(pdf_url))

    # Define filename.
    fname = r.headers.get('content-disposition', '').split('=')[-1]
    if not fname:
        fname = url.split('/')[-1]
    fname = '{0}-{1}'.format(pmc, fname)
    pdf_fp = cStringIO.StringIO()
    pdf_fp.write(r.content)

    json_fp = cStringIO.StringIO()
    json.dump(record, json_fp)

    files = {
        fname: pdf_fp,
        '{0}_medline.json'.format(md['identifier']): json_fp
    }

    resps = item.upload(files, metadata=md, queue_derive=False, retries=100,
                        retries_sleep=20)

    log.info('successfully archived: pubmed-{0}'.format(item.identifier))

    return resps


if __name__ == '__main__':
    medline_records_file = sys.argv[-1]
    with open(medline_records_file) as fp:
        #with futures.ThreadPoolExecutor(max_workers=5) as executor:
        #    for result in executor.map(archive_article, medline_record_generator(fp)):
        #        pass
        for i, record in enumerate(medline_record_generator(fp)):
            archive_article(record)
            if i >= 100:
                sys.exit()
