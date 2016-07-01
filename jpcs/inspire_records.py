# -*- coding: utf-8 -*-

"""
Get all the JPCS records on inspire that do not have
a pdf associated with them.

There should be 6807 records total. The query will break the full list down
to 250 records per file. We have to continue the search with keyword `jrec`.
All the results are kept on separate files. Result format is MARCXML.

NOTE: right now this is very non-interactive. To change parameters, modify this
code.

TODO: make this more flexible and interactive.

inspire query:

https://inspirehep.net/search?wl=0&p=j+%22j.phys.conf.ser.%2C*%22+and+not+8564%3A%2Finspire.*pdf%2F&of=hb&action_search=Search&sf=earliestdate&so=d
(pre-2008)
"""


from invenio_client import InvenioConnector
import getpass

def find_records():
    """Get records from Inspire with InvenioConnector."""
    #inspire = InvenioConnector("http://inspirehep.net")  # Without authentication
    # With authentication:
    uname = raw_input("Inspire login: ")
    pword = getpass.getpass()
    inspire = InvenioConnector("https://inspirehep.net", user=uname, password=pword)

    #import time
    #time.sleep(10)

    # inspire_pattern = 'j "Chin.Phys." and not 8564:/inspire.*pdf/'
    inspire_pattern = 'j "j.phys.conf.ser.,*" and not 8564:/inspire.*pdf/'


    reclist = []
    print("Getting the first 250 records")
    print("Inspire search pattern: " + inspire_pattern)
    records = inspire.search(p=inspire_pattern, of="xm", rg=250, wl=0)
    # Have to add `wl=0` to make wildcards function properly! This requires authentication.


    filename = "inspire_xmls/records1.xml"  # TODO: modify this to match with the new directory structure
    print("Writing to file:" + filename)
    with open(filename, "w") as f:
        f.write(records)

    startpoint = 251
    while startpoint < 400:
        # Here it's hardcoded to go through all the 6000 records.
        # XML files of 250 records will be written to directory "inspire_xmls/".
        print("Getting records from startpoint " + str(startpoint))
        records = inspire.search(p=inspire_pattern, of="xm", rg=250, jrec=startpoint, wl=0)

        # TODO: modify this to match with the new directory structure
        filename = "inspire_xmls/records"+str(startpoint)+".xml"
        print("Writing to file:" + filename)
        with open(filename, "w") as f:
            f.write(records)

        startpoint += 251



find_records()
