# -*- coding: utf-8 -*-

"""
Get all the CPC records on inspire that do not have
a pdf associated with them.

There should be 1252 records total. The query will break the full list down
to 250 records per file. We have to continue the search with keyword `jrec`.
All the results are kept on separate files. Result format is MARCXML.

NOTE: right now this is very non-interactive. To change parameters, modify this
code.

TODO: make this more flexible and interactive.

inspire query:

https://inspirehep.net/search?wl=0&ln=en&p=j+%22Chin.Phys.%22+and+not+8564%3A%2Finspire.*pdf%2F&of=hb&action_search=Search&sf=earliestdate&so=d&rm=&rg=25&sc=0
(current, we don't have permission to get these)
OR:

https://inspirehep.net/search?wl=0&p=j+%22HEPNP%22+and+not+8564%3A%2Finspire.*pdf%2F&of=hb&action_search=Search&sf=earliestdate&so=d
(pre-2008)
"""

import sys
import re

from invenio_client import InvenioConnector
import getpass
import logging

from lxml import etree

def find_records(inspire_pattern=None, recids=None, rg=50):
    """Get records from Inspire with InvenioConnector."""
    uname = raw_input("Inspire login: ")
    pword = getpass.getpass()
    inspire = InvenioConnector("https://inspirehep.net", user=uname, password=pword)

    if recids:
        inspire_pattern = "recid " + " or ".join(recids)
    if inspire_pattern:
        print("Inspire search pattern: " + inspire_pattern)
        # print("Getting the first 50 records")
        records = inspire.search(p=inspire_pattern, of="xm", rg=rg, wl=0)
        # Have to add `wl=0` to make wildcards function properly! This requires authentication.
    else:
        print("No search pattern.")
        exit(1)

    n_records = get_number_of_records_in_batch(records)
    total_amount = get_total_number_of_records(records)
    print("Total amount of results: " + total_amount)

    filename = "inspire_xmls/records1.xml"
    print("Wrote " + str(n_records) + " records to file " + filename)
    with open(filename, "w") as f:
        f.write(records)
    startpoint = rg + 1

    while startpoint < int(total_amount):
        # XML files of 50 records will be written to directory "inspire_xmls/".
        # total_amount is the about the number of search results.
        # print("Getting records from startpoint " + str(startpoint))
        records = inspire.search(p=inspire_pattern, of="xm", rg=rg, jrec=startpoint, wl=0)

        n_records = get_number_of_records_in_batch(records)
        filename = "inspire_xmls/records"+str(startpoint)+".xml"
        print("Wrote " + str(n_records) + " records to file " + filename)
        with open(filename, "w") as f:
            f.write(records)
        startpoint += rg + 1


def get_number_of_records_in_batch(records_string):
    """Get the number of record nodes in a XML string."""
    collection = etree.fromstring(records_string)
    return len(collection.xpath("//*[local-name()='record']"))

def get_total_number_of_records(records_string):
    """Get the total number of search results."""
    collection = etree.fromstring(records_string)
    for comment in collection.xpath("//comment()"):
        if "Search-Engine-Total-Number-Of-Results" in comment.text:
            tot_num = re.search(r'.?Results:\s(\d+).?', comment.text).group(1)
            return tot_num

def main(argv):
    """Modify this to get different stuff."""
    #inspire_pattern = '773__p:"EPJ Web Conf." -980:proceedings and not 8564:/inspire.*epjconf.*pdf/'
    inspire_pattern = 'j "acta phys.pol.,b*,*" and not 8564:/Fulltext/ and not 980:proceedings'
    find_records(inspire_pattern, rg=50)
    #with open("recids.txt", "r") as f: recids = f.read().split()
    #find_records(recids=recids, rg=50, )

if __name__ == "__main__":
    main(sys.argv[1:])
