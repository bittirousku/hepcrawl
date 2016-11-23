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
from scrapy.spiders import XMLFeedSpider, CrawlSpider

from ..items import HEPRecord
from ..loaders import HEPLoader
from ..utils import get_mime_type, parse_domain, split_fullname

import json


class JINRSpider(XMLFeedSpider):

    """INP crawler

    Inspire query: r budker-inp-* and not 035:arxiv and not 8564:/inspire.*pdf/

    This spider takes metadata records which are stored in a local XML file.

    Write more here

    Example usage:
    scrapy crawl jinr -a source_file=file://`pwd`/tests/responses/jinr/inspire_xmls/test_record.xml -s "JSON_OUTPUT_DIR=tmp/"
    scrapy crawl jinr -a source_dir=`pwd`/tests/responses/jinr/inspire_xmls/ -s "JSON_OUTPUT_DIR=tmp/"

    Happy crawling!
    """

    name = 'jinr'
    start_urls = []
    iterator = 'xml'
    itertag = "//*[local-name()='record']"
    domain = "http://inis.jinr.ru/sl/NTBLIB/"

    custom_settings = {
        'ITEM_PIPELINES': {
            'hepcrawl.pipelines.XmlWriterPipeline': 300,
        },
    }

    def __init__(self, source_file=None, source_dir=None, *args, **kwargs):
        """Construct JINR spider."""
        super(JINRSpider, self).__init__(*args, **kwargs)
        self.source_file = source_file
        self.source_dir = source_dir
        self.pdf_links = self.load_pdf_links()

    def start_requests(self):
        """Default starting point for scraping shall be the local XML file."""
        if self.source_file:
            yield Request(self.source_file)
        elif self.source_dir:
            for s_file in self.listdir_fullpath(self.source_dir):
                source_file = "file://" + s_file
                yield Request(source_file)

    def listdir_fullpath(self, d):
        return [os.path.join(d, f) for f in os.listdir(d)]

    def load_pdf_links(self):
        """Load the contents of the file url file to a list."""
        pdf_url_file = (
            "/home/henrikv/.virtualenvs/hepcrawl/src/hepcrawl/tests/"
            "responses/jinr/JINR_report_fulltext_urls"
        )
        with open(pdf_url_file, "r") as f:
            pdf_links = [line.rstrip('\n') for line in f]

        return pdf_links


    def add_fft_file(self, pdf_file, file_access, file_type):
        """Create a structured dictionary and add to 'files' item."""
        file_dict = {
            "access": file_access,
            "url": pdf_file,
            "type": file_type,
        }
        return file_dict

    def parse_node(self, response, node):
        """Get all the data from an Inspire record."""
        node.remove_namespaces()
        record = HEPLoader(item=HEPRecord(), selector=node, response=response)
        recid = node.xpath("./controlfield[@tag='001']/text()").extract_first()
        report = node.xpath("./datafield[@tag='037']/subfield[@code='a']/text()").extract_first()


        if report:
            report = report.replace(u"ОИЯИ", "JINR")
            report = report.replace(u"\u0415", "E")
            report = report.replace(u"\u0420", "P")
            try:
                mock_url = "http://inis.jinr.ru/sl/NTBLIB/{}.pdf".format(report)
            except UnicodeEncodeError as err:
                # import ipdb; ipdb.set_trace()
                return None
        else:
            # import ipdb; ipdb.set_trace()
            return None

        if mock_url in self.pdf_links:
            record.add_value("recid", recid)
            record.add_value(
                'additional_files',
                self.add_fft_file(mock_url, "INSPIRE-PUBLIC", "Fulltext")
            )

            return record.load_item()


