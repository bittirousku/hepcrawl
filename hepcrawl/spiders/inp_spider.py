# -*- coding: utf-8 -*-
#
# This file is part of hepcrawl.
# Copyright (C) 2016 CERN.
#
# hepcrawl is a free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""Spider for CPC."""

from __future__ import absolute_import, print_function

import os
import re

import time

from urlparse import urljoin

from w3lib.html import (
    remove_tags,
    remove_tags_with_content,
)

import logging

import requests

import nltk

from scrapy import Request
from scrapy.spiders import XMLFeedSpider

from ..items import HEPRecord
from ..loaders import HEPLoader
from ..utils import get_mime_type, parse_domain, split_fullname

import json


class INPSpider(XMLFeedSpider):

    """INP crawler

    Inspire query: r budker-inp-* and not 035:arxiv and not 8564:/inspire.*pdf/

    This spider takes metadata records which are stored in a local XML file.

    Write more here

    Example usage:
    scrapy crawl inp -a source_file=file://`pwd`/tests/responses/inp/inspire_xml/test_record.xml -s "JSON_OUTPUT_DIR=tmp/"

    TODO: continue from here! Just scrape the files, this spider is now working.
    Happy crawling!
    """

    name = 'inp'
    start_urls = []
    iterator = 'xml'  # Needed for proper namespace handling
    itertag = "//*[local-name()='record']"
    #download_delay = 10
    base_url = "http://www.inp.nsk.su/activity/preprints/index.ru.shtml?year="
    local_url = "file:///home/henrikv/.virtualenvs/hepcrawl/src/hepcrawl/tmp/inp/"

    custom_settings = {
        'ITEM_PIPELINES': {
            'hepcrawl.pipelines.XmlWriterPipeline': 300,
        },
    }

    def __init__(self, source_file=None, source_dir=None, *args, **kwargs):
        """Construct CPC spider."""
        super(INPSpider, self).__init__(*args, **kwargs)
        self.source_file = source_file
        self.source_dir = source_dir

    def start_requests(self):
        """Default starting point for scraping shall be the local XML file."""
        if self.source_file:
            yield Request(self.source_file)
        elif self.source_dir:
            for s_file in self.listdir_fullpath(self.source_dir):
                source_file = "file://" + s_file
                yield Request(source_file)
        # else:
            # self.get_splash_pages()


    def listdir_fullpath(self, d):
        return [os.path.join(d, f) for f in os.listdir(d)]

    def add_fft_file(self, pdf_file, file_access, file_type):
        """Create a structured dictionary and add to 'files' item."""
        file_dict = {
            "access": file_access,
            "url": pdf_file,
            "type": file_type,
        }
        return file_dict

    def get_splash_pages(self):

        def get_splash_page(link, year):
            page_stream = requests.get(link)
            # filenames format: year_vol_issue_page_range.xml
            filename = "{}_INP.html".format(year)
            # NOTE: filepath creation is very clunky at the moment:
            filepath = "/home/henrikv/.virtualenvs/hepcrawl/src/hepcrawl/tmp/inp/"
            full_path = filepath + filename

            with open(full_path, "w") as f:
                f.write(page_stream.content)

        years = range(1962, 2017)
        for year in years:
            link = self.base_url + str(year)
            get_splash_page(link, year)
            time.sleep(10)



    def parse_node(self, response, node):
        """Get all the data from an Inspire record."""
        node.remove_namespaces()
        recid = node.xpath("./controlfield[@tag='001']/text()").extract_first()
        report = node.xpath("./datafield[@tag='037']/subfield[@code='a']/text()").extract_first()


        try:
            year, report_no = re.search(r'(\d+)-(\d+)$', report).groups()
        except AttributeError:
            import ipdb; ipdb.set_trace()
            return None

        if len(year) == 2:
            year = "19{}".format(year)

        link = self.local_url + year + "_INP.html"
        request = Request(link, callback=self.scrape_splash)
        request.meta["recid"] = recid
        request.meta["year"] = year
        request.meta["report_no"] = report_no.zfill(3)  # Make sure to use leading zeros.

        return request


    def scrape_splash(self, response):
        """Scrape the splash page for PDF link."""
        node = response.selector
        year = response.meta["year"]
        report_no = response.meta["report_no"]
        report_no = "{}_{}".format(year, report_no)  # format in the file name

        pdf_link = node.xpath("//a[contains(@href, '" + report_no + "')]/@href").extract_first()

        if pdf_link:
            response.meta["pdf_link"] = pdf_link
            return self.build_item(response)
        else:
            self.log("No PDF link found for report" + report_no)


    def build_item(self, response):
        """Build the final record."""
        node = response.selector
        record = HEPLoader(item=HEPRecord(), selector=node, response=response)

        pdf_link = response.meta.get("pdf_link")

        record.add_value("recid", response.meta.get("recid"))
        record.add_value(
            'additional_files',
            self.add_fft_file(pdf_link, "INSPIRE-PUBLIC", "Fulltext")
        )

        return record.load_item()
