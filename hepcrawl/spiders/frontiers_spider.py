# -*- coding: utf-8 -*-
#
# This file is part of hepcrawl.
# Copyright (C) 2016 CERN.
#
# hepcrawl is a free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""CUSTOM Spider for Frontiers in... journals."""


from __future__ import absolute_import, print_function

import re

import os

from scrapy import Request
from scrapy.spiders import XMLFeedSpider

from ..items import HEPRecord
from ..loaders import HEPLoader
from ..mappings import OA_LICENSES


class FrontiersSpider(XMLFeedSpider):

    """Frontiers crawler

    Scrape through all inspire xmls
    create link to the fulltext pdf with DOI information
    hope that the link is correct :>


    Example usage:
    .. code-block:: console

        scrapy crawl frontiers -a source_file=file://`pwd`/tests/responses/frontiers/inspire_xml/records1.xml -s "JSON_OUTPUT_DIR=tmp/" -s "LOG_FILE=frontiers.log"

    Happy crawling!
    """

    name = 'frontiers'
    start_urls = []
    iterator = 'xml'
    itertag = "//*[local-name()='record']"

    custom_settings = {
        'ITEM_PIPELINES': {
            'hepcrawl.pipelines.XmlWriterPipeline': 300,
        },
    }

    def __init__(self, source_file=None, *args, **kwargs):
        """Construct Frontiers spider."""
        super(FrontiersSpider, self).__init__(*args, **kwargs)
        self.source_file = source_file

    def start_requests(self):
        """Default starting point for scraping shall be the local XML file."""
        yield Request(self.source_file)


    def create_fft_file(self, file_path, file_access, file_type):
        """Create a structured dictionary to add to 'files' item."""
        file_dict = {
            "access": file_access,
            "description": self.name.upper(),
            "url": file_path,
            "type": file_type,
        }
        return file_dict

    def listdir_fullpath(self, d):
        return [os.path.join(d, f) for f in os.listdir(d)]

    def parse_node(self, response, node):
        """Iterate all the record nodes in the XML and build the HEPRecord."""
        node.remove_namespaces()
        record = HEPLoader(item=HEPRecord(), selector=node, response=response)
        recid = node.xpath("./controlfield[@tag='001']/text()").extract_first()
        doi = node.xpath("./datafield[@tag='024']/subfield[@code='a']/text()").extract_first()

        # FIXME: aargh the direct links give access denied errors
        # Scraping is forbidden in terms!
        pdf_link = "http://journal.frontiersin.org/article/" + doi + "/pdf"

        record.add_value("recid", recid)
        record.add_value('additional_files', self.create_fft_file(pdf_link, "INSPIRE-PUBLIC", "Fulltext"))

        return record.load_item()
