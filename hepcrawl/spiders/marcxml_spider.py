# -*- coding: utf-8 -*-
#
# This file is part of hepcrawl.
# Copyright (C) 2016 CERN.
#
# hepcrawl is a free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""Generic spider for MARCXML and OAI MARCXML."""

from __future__ import absolute_import, print_function

from scrapy import Request
from scrapy.spiders import XMLFeedSpider

from ..items import HEPRecord
from ..loaders import HEPLoader
from ..utils import get_mime_type, parse_domain, split_fullname


class MarcXMLSpider(XMLFeedSpider):

    """Generic MARCXML and OAI MARCXML crawler


    This spider takes metadata records which are stored in a local XML file.

    1. The spider will parse the local MARCXML or OAI MARCXML format file for record data

    2. 


    Example usage:
    scrapy crawl marcxml -a source_file=file://`pwd`/tests/responses/marcxml/marcxml.xml -s "JSON_OUTPUT_DIR=tmp/"
    scrapy crawl marcxml -a source_file=file://`pwd`/tests/responses/marcxml/oai_marcxml.xml -s "JSON_OUTPUT_DIR=tmp/"




    Happy crawling!
    """

    name = 'marcxml'
    start_urls = []
    iterator = 'xml'  # Needed for proper namespace handling
    #itertag = 'xmlns:record'
    itertag = "//*[local-name()='record']"
    download_delay = 5

    #namespaces = [
        #("xmlns", "http://www.loc.gov/MARC21/slim"),
    #]

    def __init__(self, source_file=None, *args, **kwargs):
        """Construct BASE spider."""
        super(MarcXMLSpider, self).__init__(*args, **kwargs)
        self.source_file = source_file

    def start_requests(self):
        """Default starting point for scraping shall be the local XML file."""
        yield Request(self.source_file)

    #def get_affiliations(self, node):
        #""" Cleans the affiliation element."""

        #affiliations_raw = node.xpath(
            #"./slim:datafield[@tag='502']/slim:subfield[@code='a']/text()").extract()
        #affiliations = []
        #for aff_raw in affiliations_raw:
            #arlist = aff_raw.split(",")
            #aff = ",".join([i for i in arlist if not
                            #("diss" in i.lower() or i.strip().isdigit())])
            #affiliations.append(aff)

        #return affiliations

    def get_authors(self, node):
        """Gets the authors."""
        authors_raw = node.xpath("./datafield[@tag='100']/subfield[@code='a']/text()").extract()
        affiliations = node.xpath("./datafield[@tag='100']/subfield[@code='u']/text()").extract()

        authors = []
        for author, affiliation in zip(authors_raw, affiliations):
            surname, given_names = split_fullname(author)
            authors.append({
                'surname': surname,
                'given_names': given_names,
                #'full_name': author,
                'affiliation': affiliation,
            })

        return authors

    def parse_node(self, response, node):
        """Build the final record."""
        #import ipdb; ipdb.set_trace()
        node.remove_namespaces()
        record = HEPLoader(item=HEPRecord(), selector=node, response=response)

        
        title = node.xpath("./datafield[@tag='245']/subfield[@code='a']/text()").extract()
        collections = node.xpath("./datafield[@tag='980']/subfield[@code='a']/text()").extract()
        files = node.xpath("./datafield[@tag='856']/subfield[@code='u']/text()").extract()
        page_nr = node.xpath("./datafield[@tag='300']/subfield[@code='a']/text()").extract_first()
        date_published = node.xpath("./datafield[@tag='269']/subfield[@code='c']/text()").extract_first()
    

        record.add_value('authors', self.get_authors(node))
        record.add_value('title', title)
        record.add_value('collections', collections)
        record.add_value('files', files)
        record.add_value('page_nr', page_nr)
        record.add_value('date_published', date_published)

        
        return record.load_item()

