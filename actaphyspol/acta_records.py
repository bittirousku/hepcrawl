# -*- coding: utf-8 -*-

"""
Get records from the acta physica polonica b web page. It is dynamically
generated with AJAX. This approach is using post requests.
TODO: How would this work with Selenium?
TODO: update this to match with the new directory structure.

End product is the metadata of the desired volumes as JSON files. One file per
volume.

Usage:
    python acta_records.py [-o <outdir>] <volumes>
e.g.:
    python acta_records.py 1
    python acta_records.py -o tmp 1-3
    python acta_records.py -o tmp 1-3, 5

outdir is optional, default is `tmp/json` (not /tmp/)

"""
from __future__ import print_function

import sys

import requests
import json

class ActaSeleniumScraper(object):
    pass

class ActaScraper(object):
    """This will scrape the acta physica polonica b web page one volume at
    a time.
    Made this as a class for fun. Quite unnecessary.
    """

    def __init__(self, vol_range=None, *args, **kwargs):
        self.vol_range = vol_range #not needed really

    def get_volume(self, vol):
        """Get all the issues of one volume.

        vol is volume number.
        Return value should be a list of 12 or 13 issues. Each issue should be a list
        of record dictionaries. If there are less than 12 issues, there will be an
        empty list (dict).
        """
        post_url = "http://www.actaphys.uj.edu.pl/modules/amfphp2/json.php?service=cms2.SiteService.publicGetPage"
        issues = []

        # There should be 12 issues per year plus possible special issues.
        for no in range(1,14):
            k0 = {
                "_explicitType":"cms2.site.vo.PageRequestVO",
                "daoClassName":"DAO",
                "daoMethodName":"ajaxGetArticles",
                "ids":"",
                "no" : no,
                "vol" : vol,
                "series" : "R",
                "lang":"pl"
            }

            data = {"k0": json.dumps(k0)}
            headers = {'X-Requested-With': 'XMLHttpRequest'}
            req = requests.post(url=post_url, data=data, headers=headers)
            reqdict = json.loads(req.content)

            records = json.loads(reqdict["content"])  # This is one issue
            issues.append(records)

        return issues

    def list_to_dict(self, volume, issues=False):
        """Converts issue lists (list of records) in a volume to a dict with
        firstpages as keys.

        issues=False means that there will be on dictionary of records with fpages as keys.
        issues=True means that there will be a nested dictionary of issues. Issues
        are dictionaries of records with fpages as keys.
        """
        voldict = {}

        if not issues:
            # This is what we presumably want
            for issue in volume:
                for rec in issue:
                    #Create volume dictionary of records
                    voldict[rec["Page"]] = rec

        else:
            # This creates a dict of dicts with issue numbers and fpages as keys:
            for i, issue in enumerate(volume):
                #Create volume dictionary of issues
                issuedict = {}
                for rec in issue:
                    #Create issue dictionary of records
                    issuedict[rec["Page"]] = rec
                iss_no = str(i+1)
                voldict[iss_no] = issuedict

        return voldict


    def write_to_file(self, outdir, volume, vol):
        """Write volume (list of issues) to a file.

        Result will be a JSON file of records with first pages as keys.
        There will be 1 volume per file.
        """
        filename = outdir + "/volume"+str(vol)+".json"

        with open(filename, "w") as f:
            f.write(json.dumps(volume, indent=4))


    def read_jsons(self, vol):
        """Read one volume JSON file to a dict.
        To access page 2047 of volume 45: volumes[45]["2047"].
        """
        volumes = {}
        infile = "json/volume"+str(vol)+".json"
        with open(infile, "r") as f:
            volumes[vol] = json.loads(f.read())

        return volumes


import getopt
import os

def main(argv=None):
    """Get data from actaphys web page and write to a JSON file.

    Arguments:
    : -o output JSON directory (optional, default is tmp/json/
    : volume number (can be single int or a multiple ints with comma separation
      or int range

    Usage:
    python acta_records.py 1
    python acta_records.py -o tmp 1-3
    python acta_records.py -o tmp 1-3, 5
    """
    if argv is None:
        argv = sys.argv
    outdir = "tmp/json/"

    #parse output dir from the arguments
    try:
        opts, args = getopt.getopt(argv,"ho:",["outdir="])
    except getopt.GetoptError:
        print('acta_records.py -o <outdir> <volumes>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('acta_records.py -o <outdir> <volumes>')
            sys.exit()
        elif opt in ("-o", "--ofile"):
            outdir = os.path.join(arg, '')
            argv = argv[2:]
    if not argv:
        print('acta_records.py [-o <outdir>] <volumes>')
        sys.exit()
    print('Output dir is ' + outdir)
    if not os.path.exists(outdir):
        os.makedirs(outdir)


    #parse vol numbers:
    all_vols = []
    for i in argv:
        vrange = i.strip(",")
        if "-" in vrange:
            start, end = vrange.split("-")
            start, end = int(start), int(end)
            vrange = range(start, end+1)
            all_vols.extend(vrange)
        else:
            vrange = int(vrange)
            all_vols.append(int(vrange))

    # Loop through the list of volumes, fetch the data and write to file
    for vol in all_vols:
        scraper = ActaScraper()

        print("Getting issues from the web...")
        volume = scraper.get_volume(vol)

        print("Converting to dictionary")
        voldict = scraper.list_to_dict(volume)
        print("Writing issues to " + outdir)
        writer = scraper.write_to_file(outdir, voldict, vol)





if __name__ == "__main__":
    main(sys.argv[1:])
