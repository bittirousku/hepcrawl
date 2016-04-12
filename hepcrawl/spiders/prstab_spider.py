# -*- coding: utf-8 -*-
#
# This file is part of hepcrawl.
# Copyright (C) 2016 CERN.
#
# hepcrawl is a free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""Spider for PRSTAB."""

from __future__ import absolute_import, print_function

import os

from urlparse import urljoin

import logging

from scrapy import Request
from scrapy.spiders import XMLFeedSpider

from ..items import HEPRecord
from ..loaders import HEPLoader
from ..utils import get_mime_type, parse_domain, split_fullname

import json

class PrstabSpider(XMLFeedSpider):

    """PRSTAB crawler


    This spider takes metadata records which are stored in a local XML file.

    0. First you should harvest the inspire files with `actaphyspol/inspire_records.py`,
       then actarecords with `actaphyspol/acta_records.py`

    1. Actavolumes in JSON format will be loaded to a class variable
       `self.actavolumes`. 
    2. For every record scrapy scrapes in the Inspire MARCXML file, a pdf link
       and abstract will be get from actavolumes. This actually checks if 
       abstract already exist  in the Inspire record before adding it.
    3. If some page cannot be found on the Acta JSON files, the whole record
       is skipped.
    4. End product is a MARCXML file. There is a dedicated pipeline 
       `XmlWriterPipeline` for that.

    If you wanto to scrape multiple volumes with one go, use the script
    `actaphyspol/scrape_acta_all.sh`.


    Example usage:
    scrapy crawl prstab -a source_file=file://`pwd`/tests/responses/prstab/xml/records1.xml -s "JSON_OUTPUT_DIR=tmp/"
    
    scrapy crawl prstab -a source_dir=`pwd`/tests/responses/prstab/xml/ -s "JSON_OUTPUT_DIR=tmp/"





    Happy crawling!
    """

    name = 'prstab'
    start_urls = []
    iterator = 'xml'  # Needed for proper namespace handling
    #itertag = 'xmlns:record'
    itertag = "//*[local-name()='record']"
    download_delay = 10

    #namespaces = [
        #("xmlns", "http://www.loc.gov/MARC21/slim"),
    #]

    custom_settings = {
        'ITEM_PIPELINES': {
            'hepcrawl.pipelines.XmlWriterPipeline': 300,
        },
    }

    # Set up a logger
    #logging.basicConfig(level=logging.DEBUG,
                        #filename="prstab.log",
                        #filemode="a+",
                        #format="%(asctime)-15s %(levelname)-8s %(message)s")
    #logger = logging.getLogger("PRSTAB")
    #console = logging.StreamHandler()
    #console.setLevel(logging.INFO)
    #formatter = logging.Formatter('%(asctime)-12s: %(levelname)-5s %(message)s')
    #console.setFormatter(formatter)
    #logging.getLogger('').addHandler(console)



    def __init__(self, source_file=None, source_dir=None, *args, **kwargs):
        """Construct PRSTAB spider."""
        super(PrstabSpider, self).__init__(*args, **kwargs)
        self.source_file = source_file
        self.source_dir = source_dir
        self.prstabrecords = self.read_json_volumes()


    def start_requests(self):
        """Default starting point for scraping shall be the local XML file."""
        # TODO: here are 24 inspire files total in the response directory. Should scrape
        # all of them.
        if self.source_file:
            yield Request(self.source_file)
        elif self.source_dir:
            for s_file in os.listdir(self.source_dir):
                source_file = "file://" + self.source_dir + s_file 
                yield Request(source_file)


    def read_json_volumes_with_doi_as_key(self):
        """Read all the 24 prstab json files on at a time. Return a dict where 
        every record are added with doi as a key.

        Input file content is a dict with one key, "data". Its value is a list 
        of record dicts.
        """
        records = {}
        for i in range(1, 25):
            infile = "tests/responses/prstab/json/records"+str(i)+".json"
            with open(infile, "r") as f:
                rec_dict = json.loads(f.read())
                for rec in rec_dict["data"]:
                    doi = rec["identifiers"]["doi"]
                    records[doi] = rec

        return records
    
    def read_json_volumes(self):
        """Read all the 24 prstab json files on at a time. Return a dict of dicts
        with volume number and fpage as keys.

        Input file content is a dict with one key, "data". Its value is a list 
        of record dicts.
        """
        records = {}
        for i in range(1, 25):
            infile = "tests/responses/prstab/json/records"+str(i)+".json"
            with open(infile, "r") as f:
                rec_dict = json.loads(f.read())
                for rec in rec_dict["data"]:
                    doi = rec["identifiers"]["doi"]
                    fpage= rec["pageStart"]
                    vol = rec["volume"]["number"]
                    #records[doi] = rec
                    if vol not in records:
                        records[vol] = {}
                    records[vol][fpage] = rec

        return records

    def add_fft_file(self, pdf_file, file_access, file_type):
        """Create a structured dictionary and add to 'files' item."""
        file_dict = {
            "access": file_access,
            "url": pdf_file,
            "type": file_type,
        }
        return file_dict

    def parse_node(self, response, node):
        """Get all the data for one Inspire record."""
        node.remove_namespaces()
        recid = node.xpath("./controlfield[@tag='001']/text()").extract()
        doi = node.xpath("./datafield[@tag='024']/subfield[@code='a']/text()").extract_first()
        vol = node.xpath("./datafield[@tag='773']/subfield[@code='v']/text()").extract_first()

        # NOTE: do we need fpage for PRSTAB?
        page_range = node.xpath("./datafield[@tag='773']/subfield[@code='c']/text()").extract_first()
        if "-" in page_range:
            fpage = page_range.split("-")[0]
        else:
            fpage = page_range
        fpage = filter(lambda x: x.isdigit(), fpage)

        # DOIs are not very reliable, not all records have them:
        # prstab_record = self.prstabrecords[doi]
        # Use vol and fpage instead:
        try:
            prstab_record = self.prstabrecords[vol][fpage]
        except KeyError:
            # No record found with current vol and fpage. Possible corrupt
            # Inspire data.
            return None

        pdf_link = "http://journals.aps.org/prab/pdf/" + prstab_record["id"]

        ## Check that the pdf link is valid
        if "pdf" not in get_mime_type(pdf_link):
            return None  # No pdf link available. Nothing to append to Inspire.
        
        abstract = []
        existing_abstract = node.xpath("./datafield[@tag='520']/subfield[@code='a']/text()").extract_first()
        if not existing_abstract:
            try:
                abstract = prstab_record["abstract"]["value"]
            except KeyError:
                # No abstract available
                pass

            
        
        response.meta["recid"] = recid
        response.meta["pdf_link"] = pdf_link
        response.meta["abstract"] = abstract

        return self.build_item(response)


    def build_item(self, response):
        """Build the final record."""
        node = response.selector
        record = HEPLoader(item=HEPRecord(), selector=node, response=response)

        abstract = response.meta.get("abstract")
        recid = response.meta.get("recid")
        pdf_link = response.meta.get("pdf_link")
        
        
        record.add_value("abstract", abstract)
        record.add_value("recid", recid)
        record.add_value('files', self.add_fft_file(pdf_link, "INSPIRE-PUBLIC", "fulltext"))
        
        return record.load_item()


