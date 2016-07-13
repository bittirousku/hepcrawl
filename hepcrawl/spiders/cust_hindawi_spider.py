# -*- coding: utf-8 -*-
#
# This file is part of hepcrawl.
# Copyright (C) 2016 CERN.
#
# hepcrawl is a free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""CUSTOM Spider for AHEP and other Hindawi journals."""


from __future__ import absolute_import, print_function

import re

import os

from scrapy import Request
from scrapy.spiders import XMLFeedSpider

from ..items import HEPRecord
from ..loaders import HEPLoader
from ..mappings import OA_LICENSES


class CustHindawiSpider(XMLFeedSpider):

    """Hindawi crawler

    OAI interface: http://www.hindawi.com/oai-pmh/
    Example:
    http://www.hindawi.com/oai-pmh/oai.aspx?verb=listrecords&set=HINDAWI.AHEP&metadataprefix=marc21&from=2010-01-01

    http://www.hindawi.com/oai-pmh/oai.aspx?verb=listrecords&resumptionToken=2010-01-01||HINDAWI.AHEP|marc21|200|2016-07-08T12:52:12Z

    Sets to use:
    HINDAWI.AHEP (Advances in High Energy Physics)

    http://www.hindawi.com/oai-pmh/oai.aspx?verb=getrecord&identifier=oai:hindawi.com:10.1155/2013/910419&metadataprefix=marc21

    Scrapes Hindawi metadata XML files one at a time.
    The actual files should be retrieved from Hindawi via its OAI interface.
    The file can contain multiple records.

    1. The spider will parse the local MARC21XML format file for record data
       (DOI).

    3. A request to scrape the Hindawi XML for the record with this DOI will be
       returned. The XML is accessed via OAI interface.

    2. Finally a HEPRecord will be created.

    Example usage:
    .. code-block:: console

        scrapy crawl custhindawi -a source_file=file://`pwd`/tests/responses/ahep/inspire_xml/records1.xml -s "JSON_OUTPUT_DIR=tmp/" -s "LOG_FILE=custhindawi.log"

    Happy crawling!
    """

    name = 'custhindawi'
    start_urls = []
    iterator = 'xml'
    itertag = "//*[local-name()='record']"


    custom_settings = {
        'ITEM_PIPELINES': {
            'hepcrawl.pipelines.XmlWriterPipeline': 300,
        },
    }

    def __init__(self, source_file=None, *args, **kwargs):
        """Construct AHEP spider."""
        super(CustHindawiSpider, self).__init__(*args, **kwargs)
        self.source_file = source_file

    def start_requests(self):
        """Default starting point for scraping shall be the local XML file."""
        yield Request(self.source_file)


    def get_urls_in_record(self, node):
        """Return all the different urls in the xml."""
        marc_856 = node.xpath(".//datafield[@tag='856']/subfield[@code='u']/text()").extract()
        marc_FFT = node.xpath(
            ".//datafield[@tag='FFT']/subfield[@code='a']/text()").extract()
        all_links = list(set(marc_856 + marc_FFT))

        return self.differentiate_urls(all_links)

    @staticmethod
    def differentiate_urls(urls_in_record):
        """Determine what kind of urls the record has."""
        pdf_links = []
        xml_links = []
        splash_links = []
        for link in urls_in_record:
            if "pdf" in link.lower():
                pdf_links.append(link)
            elif "xml" in link.lower():
                xml_links.append(link)
            elif "dx.doi.org" in link.lower():
                splash_links.append(link)

        return (
            pdf_links,
            xml_links,
            splash_links,
        )

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
        recid = node.xpath("./controlfield[@tag='001']/text()").extract_first()
        doi = node.xpath("./datafield[@tag='024']/subfield[@code='a']/text()").extract_first()

        local_hindawi = False
        if local_hindawi:
            xml_file_path = "/home/henrikv/.virtualenvs/hepcrawl/src/hepcrawl/tests/responses/custom_hindawi/hindawi_xml/"
            for xml_file in self.listdir_fullpath(xml_file_path):
                if "xml" in xml_file:
                    link = "file://" + xml_file
                    request = Request(link, callback=self.scrape_for_pdf)
                    request.meta["recid"] = recid
                    request.meta["doi"] = doi

                    yield request
        else:
            try:
                link = "http://www.hindawi.com/oai-pmh/oai.aspx?verb=getrecord&identifier=oai:hindawi.com:" + doi + "&metadataprefix=marc21"
            except TypeError:
                # Mostly if not DOI in inspire record
                import ipdb; ipdb.set_trace()
            request = Request(link, callback=self.scrape_for_pdf)
            request.meta["recid"] = recid
            request.meta["doi"] = doi
            yield request

    def scrape_for_pdf(self, response):
        """Scrape the hindawi xmls for pdf links."""
        node = response.selector
        # Here find the correct record
        node.remove_namespaces()
        records = node.xpath("//record")
        pdf_links = []

        for record in records:
            hindawi_doi = record.xpath(".//datafield[@tag='024'][subfield[@code='2'][contains(text(), 'DOI')]]/subfield[@code='a']/text()").extract_first()
            inspire_doi = response.meta.get("doi")
            if hindawi_doi == inspire_doi:
                pdf_links, xml_links, splash_links = self.get_urls_in_record(record)
                break

        if not pdf_links:
            self.logger.warning("Wrong DOI for recid: " + response.meta["recid"])
            return None

        response.meta["pdf_links"] = pdf_links

        return self.build_item(response)


    def build_item(self, response):
        node = response.selector
        node.remove_namespaces()
        record = HEPLoader(item=HEPRecord(), selector=node, response=response)

        pdf_link = response.meta.get("pdf_links")[0]
        record.add_value("recid", response.meta.get("recid"))
        record.add_value('additional_files', self.create_fft_file(pdf_link, "INSPIRE-PUBLIC", "Fulltext"))


        return record.load_item()
