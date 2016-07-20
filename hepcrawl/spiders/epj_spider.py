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

from scrapy import Request
from scrapy.spiders import XMLFeedSpider

from ..items import HEPRecord
from ..loaders import HEPLoader
from ..utils import get_mime_type, parse_domain, split_fullname

import json



class EPJSpider(XMLFeedSpider):

    """EPJ crawler


    This spider takes metadata records which are stored in a local XML file.

    Example usage:
    scrapy crawl epj -a source_file=file://`pwd`/tests/responses/epj/inspire_xml/records1.xml -s "JSON_OUTPUT_DIR=tmp/" -s "LOG_FILE=epj.log"
    scrapy crawl epj -a source_dir=`pwd`/tests/responses/epj/inspire_xml/ -s "JSON_OUTPUT_DIR=tmp/" -s "LOG_FILE=epj.log"

    Happy crawling!
    """

    name = 'epj'
    start_urls = []
    iterator = 'xml'
    itertag = "//*[local-name()='record']"
    #download_delay = 30

    custom_settings = {
        'ITEM_PIPELINES': {
            'hepcrawl.pipelines.XmlWriterPipeline': 300,
        },
        #'AUTOTHROTTLE_ENABLED': 'True',   # FIXME: y dis no wörk? :( Changing settings.py works
    }




    #def _monkey_patching_HTTPClientParser_statusReceived(self):
        #"""
        #monkey patching for scrapy.xlib.tx._newclient.HTTPClientParser.statusReceived
        #From https://github.com/scrapy/scrapy/issues/345
        #because unnecessary twisted errors
        #"""
        ## FIXME: this is not really working...
        ##from scrapy.xlib.tx._newclient import HTTPClientParser, ParseError
        #from twisted.web._newclient import HTTPClientParser, ParseError
        #old_sr = HTTPClientParser.statusReceived
        #def statusReceived(self, status):
            #try:
                #return old_sr(self, status)
            #except ParseError, e:
                #if e.args[0] == 'wrong number of parts':
                    #return old_sr(self, status + ' OK')
            #raise
        #statusReceived.__doc__ == old_sr.__doc__
        #HTTPClientParser.statusReceived = statusReceived

    def __init__(self, source_file=None, source_dir=None, *args, **kwargs):
        """Construct EPJ spider."""
        super(EPJSpider, self).__init__(*args, **kwargs)
        self.source_file = source_file
        self.source_dir = source_dir
        #self._monkey_patching_HTTPClientParser_statusReceived()


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
        recid = node.xpath("./controlfield[@tag='001']/text()").extract_first()
        doi = node.xpath("./datafield[@tag='024']/subfield[@code='a']/text()").extract_first()
        if not doi:
            self.logger.warning("No DOI for recid " + recid)
            # Probably a duplicate preprint record.
            return None
        dxlink = "http://dx.doi.org/" + doi
        #test_splash = "file:///home/henrikv/.virtualenvs/hepcrawl/src/hepcrawl/tests/responses/epj/test_splash.html"

        time.sleep(30)
        request = Request(dxlink, callback=self.scrape_for_pdf_link)
        request.meta["recid"] = recid
        request.meta["doi"] = doi
        return request


    def scrape_for_pdf_link(self, response):
        """Get pdf link from the EPJ splash page
        E.g. http://www.epj-conferences.org/articles/epjconf/abs/2016/16/
        epjconf-RICAP-14_06010/epjconf-RICAP-14_06010.html
        """
        all_links = response.xpath("//a[contains(@href, 'pdf')]/@href").extract()

        if all_links:
            response.meta["pdf_link"] = all_links[0]
            return self.build_item(response)
        else:
            print("NO LINK FOUND")


    def build_item(self, response):
        """Build the final record."""
        node = response.selector
        record = HEPLoader(item=HEPRecord(), selector=node, response=response)

        pdf_link = response.meta.get("pdf_link")

        record.add_value("recid", response.meta.get("recid"))
        record.add_value('additional_files', self.add_fft_file(pdf_link, "INSPIRE-PUBLIC", "Fulltext"))

        return record.load_item()
