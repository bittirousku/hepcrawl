# -*- coding: utf-8 -*-
#
# This file is part of hepcrawl.
# Copyright (C) 2016 CERN.
#
# hepcrawl is a free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""Spider for Acta Physica Polonica B."""

from __future__ import absolute_import, print_function

import os

from urlparse import urljoin

import logging

from scrapy import Request
from scrapy.spiders import XMLFeedSpider

from ..items import HEPRecord
from ..loaders import HEPLoader
from ..utils import get_mime_type, parse_domain, split_fullname

from contextlib import closing
from selenium import webdriver
import selenium.webdriver.support.ui as ui

import json

from ..comparators import compare_titles

class ActaXMLSpider(XMLFeedSpider):

    """Acta Physica Polonica B crawler


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
    scrapy crawl actaphys -a source_file=file://`pwd`/tests/responses/actaphys/inspire_xml/records1.xml -s "JSON_OUTPUT_DIR=tmp/"

    scrapy crawl actaphys -a source_dir=`pwd`/tests/responses/actaphys/inspire_xml/ -s "JSON_OUTPUT_DIR=tmp/" -s "LOG_FILE=actaphys.log"

    Happy crawling!
    """

    name = 'actaphys'
    start_urls = []
    iterator = 'xml'
    itertag = "//*[local-name()='record']"
    download_delay = 10

    custom_settings = {
        'ITEM_PIPELINES': {
            'hepcrawl.pipelines.XmlWriterPipeline': 300,
        },
    }

    ### Set up a logger
    #logging.basicConfig(level=logging.DEBUG,
                        #filename="actaphys.log",
                        #filemode="a+",
                        #format="%(asctime)-15s %(levelname)-8s %(message)s")
    #logger = logging.getLogger("acta physica polonica")
    #console = logging.StreamHandler()
    #console.setLevel(logging.INFO)
    #formatter = logging.Formatter('%(asctime)-12s: %(levelname)-5s %(message)s')
    #console.setFormatter(formatter)
    #logging.getLogger('').addHandler(console)



    def __init__(self, source_file=None, source_dir=None, *args, **kwargs):
        """Construct Acta spider."""
        super(ActaXMLSpider, self).__init__(*args, **kwargs)
        self.source_file = source_file
        self.source_dir = source_dir
        self.actavolumes = self.read_json_volumes()
        self.flat_acta_records = self.flatten_dict(self.actavolumes)



    def start_requests(self):
        """Default starting point for scraping shall be the local XML file."""
        if self.source_file:
            yield Request(self.source_file)
        elif self.source_dir:
            for s_file in self.listdir_fullpath(self.source_dir):
                source_file = "file://" + s_file
                yield Request(source_file)

    def listdir_fullpath(self, directory):
        """List full paths of files in a directory."""
        return [os.path.join(directory, f) for f in os.listdir(directory)]

    def flatten_dict(self, nested_dict):
        """Flatten a triple nested dict and return a list of dicts.
        Discard the outer keys (because we have them inside also!).
        """
        flat_acta_records = []
        for key, vol in nested_dict.iteritems():
            for fpage, record in vol.iteritems():
                flat_acta_records.append(record)

        return flat_acta_records


    def read_json_volumes(self):
        """Read all the 47 actaphys json files (volumes) to one dict.
        To access page 2047 of volume 45: volumes[47]["997"].
        """
        volumes = {}
        for i in range(1, 48):
            infile = "tests/responses/actaphys/json/volume"+str(i)+".json"
            with open(infile, "r") as f:
                volumes[str(i)] = json.loads(f.read())

        return volumes

    def add_fft_file(self, pdf_file, file_access, file_type):
        """Create a structured dictionary and add to 'files' item."""
        file_dict = {
            "access": file_access,
            "url": pdf_file,
            "type": file_type,
        }
        return file_dict

    def marc_to_dict(self, node, tag):
        """Convert a MARCXML node to a dictionary."""
        marcdict = {}
        marc_node = node.xpath("./datafield[@tag='"+ tag +"']")
        subfields = marc_node.xpath("subfield")
        for subfield in subfields:
            dkey = subfield.xpath("@code").extract_first()
            dvalue = subfield.xpath("text()").extract_first()
            marcdict[dkey] = dvalue

        return marcdict

    def parse_node(self, response, node):
        """Build the final record."""
        # NOTE: We can do it here just with the urls. Check missing records'
        # fpage and fetch the corresponding fulltext url

        node.remove_namespaces()
        recid = node.xpath("./controlfield[@tag='001']/text()").extract_first()
        dois = node.xpath("./datafield[@tag='024']/subfield[@code='a']/text()").extract()
        vol_raws = node.xpath("./datafield[@tag='773']/subfield[@code='v']/text()").extract()
        title = node.xpath("./datafield[@tag='245']/subfield[@code='a']/text()").extract_first()
        marc_773 = self.marc_to_dict(node, "773")
        for vr in vol_raws:
            if "B" in vr:
                vol = vr[1:]
        #else:
            #???

        page_range = node.xpath("./datafield[@tag='773']/subfield[@code='c']/text()").extract_first()

        if "-" in page_range:
            fpage = page_range.split("-")[0]
        else:
            fpage = page_range
        fpage = filter(lambda x: x.isdigit(), fpage)

        pubinfo = {
            "vol": vol,
            "fpage": fpage,
            "recid": recid,
            #"abstract": abstract,
            "title": title,
            "marc_773": marc_773
        }



        # HACK
        #if recid == "3151":
            #fpage = "5"
        #if recid == "5589": # has an erratum OK
            #fpage = "459"
            #vol = "9"
        #if recid == "71542":
            #fpage = "57"
            #vol = "2"
        #if recid == "201076":
            #fpage = "747"
            #vol = "14"
        #if recid == "220517": # THIS has an erratum! missing erratum
            #fpage = "1151"
            #vol = "15"
        #if recid == "286731": # THIS has an erratum! OK
            #fpage = "801"
            #vol = "20"


        link = "http://www.actaphys.uj.edu.pl/findarticle?series=Reg&vol="+vol+"&page="+fpage
        pdf_link = "http://www.actaphys.uj.edu.pl/vol"+vol+"/fulltext/v"+vol+"p"+fpage.zfill(4)

        self.only_update_773 = False
        if self.only_update_773:
            # Here try to match title like with CPC
            # Caught some errors, but nothing serious
            correct_record = self.compare_with_dict_metadata(pubinfo)
            if not correct_record:
                self.logger.warning("Titles don't match. Could not find splash metadata for recid " + recid)
                return None
            pubinfo["marc_773"]["n"] = correct_record["No"]
            pubinfo["marc_773"]["v"] = correct_record["Vol"]
            pubinfo["marc_773"]["c"] = correct_record["Page"]

        abstract = ""
        existing_abstract = node.xpath("./datafield[@tag='520']/subfield[@code='a']/text()").extract_first()
        if not existing_abstract:
            try:
                abstract = self.actavolumes[vol][fpage].get("AbstrTeX")
            except KeyError:
                self.logger.warning("No fpage "+ fpage + " found on volume " + vol + ", recid: " + recid)
                return None


        response.meta["fpage"] = fpage
        response.meta["recid"] = recid
        response.meta["dois"] = dois
        response.meta["pdf_link"] = pdf_link
        response.meta["abstract"] = abstract
        response.meta["marc_773"] =  pubinfo.get("marc_773")


        return self.build_item(response)

    def compare_with_dict_metadata(self, pubinfo):
        """Loop the dictionary of try to find a title that matches
        with the one in Inspire metadata.
        """
        # NOTE: should use the flattened list of dicts here.
        for record in self.flat_acta_records:
            splash_title = record["Title"]
            inspire_title = pubinfo["title"]
            jaccard_similarity, same_titles = compare_titles(inspire_title,
                                                            splash_title,
                                                            method="jaccard",
                                                            required=0.4)
            vol_diff = abs(int(record["Vol"]) - int(pubinfo["vol"]))
            if pubinfo["fpage"] == record["Page"]:
                if same_titles:
                    return record
            elif same_titles and vol_diff <= 2:
                print("\n")
                print("Inspire: {0}, {1} {2}".format(inspire_title, pubinfo["vol"], pubinfo["marc_773"].get("c")))

                print("Splash: {0}, {1} {2}".format(
                    splash_title, record["Vol"],
                    record["Page"]))

                print("\n")
                print("http://inspirehep.net/record/" + pubinfo["recid"])

                really_same = raw_input("Are they really the same? (y/n)") or "y"
                if really_same == "y":
                    return record



    def build_item(self, response):
        node = response.selector
        record = HEPLoader(item=HEPRecord(), selector=node, response=response)
        pdf_link = response.meta.get("pdf_link")

        record.add_value("recid", response.meta.get("recid"))
        if self.only_update_773:
            record.add_value("marc_773", response.meta.get("marc_773"))
        else:
            record.add_value("dois", response.meta.get("dois"))
            record.add_value("abstract", response.meta.get("abstract"))
            record.add_value('additional_files', self.add_fft_file(pdf_link, "INSPIRE-PUBLIC", "Fulltext"))
        return record.load_item()


