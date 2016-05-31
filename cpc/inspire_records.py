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

from invenio_client import InvenioConnector
import getpass
import logging


def find_records(inspire_pattern, total_amount):
    """Get records from Inspire with InvenioConnector."""
    uname = raw_input("Inspire login: ")
    pword = getpass.getpass()
    inspire = InvenioConnector("https://inspirehep.net", user=uname, password=pword)

    reclist = []
    print("Inspire search pattern: " + inspire_pattern)
    print("Getting the first 50 records")
    records = inspire.search(p=inspire_pattern, of="xm", rg=50, wl=0)
    # Have to add `wl=0` to make wildcards function properly! This requires authentication.

    filename = "inspire_xmls/records1.xml"
    print("Writing to file:" + filename)
    with open(filename, "w") as f:
        f.write(records)
    startpoint = 51

    while startpoint < total_amount:
        # XML files of 50 records will be written to directory "inspire_xmls/".
        # total_amount is the about the number of search results.
        print("Getting records from startpoint " + str(startpoint))
        records = inspire.search(p=inspire_pattern, of="xm", rg=50, jrec=startpoint, wl=0)

        filename = "inspire_xmls/records"+str(startpoint)+".xml"
        print("Writing to file:" + filename)
        with open(filename, "w") as f:
            f.write(records)
        startpoint += 51


def main(argv):
    """Modify this to get different stuff."""
    # inspire_pattern = 'j "Chin.Phys." and not 8564:/inspire.*pdf/'
    inspire_pattern = 'j "HEPNP" and not 8564:/inspire.*pdf/'
    find_records(inspire_pattern, 300)

if __name__ == "__main__":
    main(sys.argv[1:])
