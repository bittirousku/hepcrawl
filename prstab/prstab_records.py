# -*- coding: utf-8 -*-

"""
Get records from the APS API for the PRSTAB journal.

End product is the metadata for every journal as JSON files. 100 records per file.

Issues web interface: http://journals.aps.org/prab/issues/
API help page: http://harvest.aps.org/docs/harvest-api

Usage: 
    python prstab_records.py

TODO: make this more dynamic and flexible. Now everything is hardcoded.

"""
from __future__ import print_function

import sys

import requests
import json
import re
import time

#class PrstabScraper(object):
    
def get_no_of_result_pages(link_to_last):
    """Get pagination of the last page from the headers link.
    
    E.g. http://harvest.aps.org/v2/journals/articles?from=1998-05-14&page=24&per_page=100
    """
    search_pattern = re.compile(r'^.*&page=(\d*)&per_page.*$')
    result_no = search_pattern.search(link_to_last)
    try:
        return result_no.groups()[0]
    except AttributeError:
        raise

def find_records():
    # First record ever:
    # curl -D - -H 'Accept: application/vnd.tesseract.article+json' http://harvest.aps.org/v2/journals/articles/10.1103/PhysRevSTAB.1.012401

    # Get the starting point for search and the number of result pages:
    start_url = "http://harvest.aps.org/v2/journals/articles?from=1998-05-14&journals=PRSTAB&per_page=100"

    req = requests.get(start_url)
    # NOTE: the header next page link is wrong!! It's missing the journal.
    # API help says you shouldn't construct your own urls and strictly
    # use the one in the headers, BUT THEY ARE WRONG!
    link_to_last = req.links["last"]["url"]
    no_of_pages = get_no_of_result_pages(link_to_last)

    # Then go through all the result pages and write to file
    for res_page in range(1, int(no_of_pages) + 1):
        if not req.status_code == 200:
            import ipdb; ipdb.set_trace()
        records = req.content
        filename = "json/records" + str(res_page) + ".json"

        print("\nRequest url:" + req.url)
        print("Writing to file:" + filename)
        with open(filename, "w") as f:
            f.write(records)

        try:
            link_to_next = req.links["next"]["url"] + "&journals=PRSTAB"
            req = requests.get(link_to_next)
        except KeyError:
            # Because the last page does not have a "next" link.
            continue
        
        time.sleep(5)
            
       

find_records()

