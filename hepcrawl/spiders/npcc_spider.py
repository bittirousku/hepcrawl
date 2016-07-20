# -*- coding: utf-8 -*-
#
# This file is part of hepcrawl.
# Copyright (C) 2016 CERN.
#
# hepcrawl is a free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""Custom spider for NPCC (Elsevier)."""

from __future__ import absolute_import, print_function

import os
import re

from tempfile import mkdtemp

import dateutil.parser as dparser

import requests

from scrapy import Request
from scrapy.spiders import XMLFeedSpider

from ..items import HEPRecord
from ..loaders import HEPLoader
from ..utils import (
    unzip_xml_files,
    get_first,
    has_numbers,
    range_as_string,
)

from ..dateutils import format_year


class NPCCSpider(XMLFeedSpider):
    """NPCC crawler.

    # NOTE WARNING TODO FIXME HACK This is an elsevier harvest, so be E X T R A Careful!

    Scrape the Inspire XMLs and from the metadata there construct paths to
    Elsevier XMLs for scraping. Must get out the `LICENSE TO KILL`.

    1. Start scraping the Inspire XMLs.

    3. Load Elsevier records to a dict. Take DOIs as keys, and preserve everything.
    FIXME: this is yet to be done! Please flatten dir structure.

    4. Compare. With every Inspire record check if DOI exists and try to find
       an Elsevier journal with the same DOI.

    5. If the matching record can be found, check if it has an OPEN ACCESS license.

    6. If yes, build_item with recid and possibly sd_url (or sumthng else?).

    7. Enjoy.

    NOTE: Florian did this.



    Example usage:
    .. code-block:: console

        scrapy crawl NPCC -a atom_feed=file://`pwd`/tests/responses/npcc/test_feed.xml -s "JSON_OUTPUT_DIR=tmp/"
        scrapy crawl NPCC -a zip_file=file://`pwd`/tests/responses/npcc/nima.zip -s "JSON_OUTPUT_DIR=tmp/"
        scrapy crawl NPCC -a xml_file=file://`pwd`/tests/responses/npcc/sample_consyn_record.xml -s "JSON_OUTPUT_DIR=tmp/"

        scrapy crawl NPCC -a xml_file=file://`pwd`/tests/responses/npcc/records1.xml -s "JSON_OUTPUT_DIR=tmp/"

    for logging, add -s "LOG_FILE=elsevier.log"

    * This is useful: https://www.elsevier.com/__data/assets/pdf_file/0006/58407/ja50_tagbytag5.pdf

    Happy crawling!
    """

    name = 'NPCC'
    start_urls = []
    iterator = 'xml'
    itertag = "//*[local-name()='record']"

    custom_settings = {
        'ITEM_PIPELINES': {
            'hepcrawl.pipelines.XmlWriterPipeline': 300,
        },
        'AUTOTHROTTLE_ENABLED': 'True',
    }

    namespaces = [
        ("doc", "http://www.elsevier.com/xml/document/schema"),
        ("dp", "http://www.elsevier.com/xml/common/doc-properties/schema"),
        ("cps", "http://www.elsevier.com/xml/common/consyn-properties/schema"),
        ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
        ("dct", "http://purl.org/dc/terms/"),
        ("prism", "http://prismstandard.org/namespaces/basic/2.0/"),
        ("oa", "http://vtw.elsevier.com/data/ns/properties/OpenAccess-1/"),
        ("cp", "http://vtw.elsevier.com/data/ns/properties/Copyright-1/"),
        ("cja", "http://www.elsevier.com/xml/cja/schema"),
        ("ja", "http://www.elsevier.com/xml/ja/schema"),
        ("bk", "http://www.elsevier.com/xml/bk/schema"),
        ("ce", "http://www.elsevier.com/xml/common/schema"),
        ("mml", "http://www.w3.org/1998/Math/MathML"),
        ("cals", "http://www.elsevier.com/xml/common/cals/schema"),
        ("tb", "http://www.elsevier.com/xml/common/table/schema"),
        ("sa", "http://www.elsevier.com/xml/common/struct-aff/schema"),
        ("sb", "http://www.elsevier.com/xml/common/struct-bib/schema"),
        ("xlink", "http://www.w3.org/1999/xlink"),
    ]

    def __init__(self, atom_feed=None, zip_file=None, xml_file=None, *args, **kwargs):
        """Construct Elsevier spider."""
        super(NPCCSpider, self).__init__(*args, **kwargs)
        self.atom_feed = atom_feed
        self.zip_file = zip_file
        self.xml_file = xml_file

    def start_requests(self):
        """Spider can be run on atom feed, zip file, or individual record xml"""
        if self.atom_feed:
            yield Request(self.atom_feed, callback=self.handle_feed)
        elif self.zip_file:
            yield Request(self.zip_file, callback=self.handle_package)
        elif self.xml_file:
            yield Request(
                self.xml_file,
                meta={"xml_url": self.xml_file},
            )

    def handle_feed(self, response):
        """Handle the feed and yield a request for every zip package found."""
        node = response.selector
        node.remove_namespaces()
        entry = node.xpath(".//entry")
        for ent in entry:
            self.zip_file = ent.xpath("./link/@href").extract()[0]
            yield Request(self.zip_file, callback=self.handle_package)

    def handle_package(self, response):
        """Handle the zip package and yield a request for every XML found."""
        self.log("Visited %s" % response.url)
        filename = os.path.basename(response.url).rstrip(".zip")
        # TMP dir to extract zip packages:
        target_folder = mkdtemp(prefix="elsevier_" + filename + "_", dir="/tmp/")

        zip_filepath = response.url.replace("file://", "")
        xml_files = unzip_xml_files(zip_filepath, target_folder)
        # The xml files shouldn't be removed after processing; they will
        # be later uploaded to Inspire. So don't remove any tmp files here.
        for xml_file in xml_files:
            xml_url = u"file://{0}".format(os.path.abspath(xml_file))
            yield Request(
                xml_url,
                meta={"package_path": zip_filepath,
                      "xml_url": xml_url},
            )

    @staticmethod
    def get_dois(node):
        """Get the dois."""
        dois = node.xpath(".//ja:item-info/ce:doi/text()")
        if not dois:
            dois = node.xpath(".//prism:doi/text()")
        if dois:
            return dois.extract()

    def get_title(self, node):
        """Get article title."""
        title = node.xpath(".//ce:title/text()")
        if not title:
            title = node.xpath(".//dct:title/text()")
        if title:
            return self._fix_node_text(title.extract())


    #def get_copyright(self, node):
        #"""Get copyright information."""
        #cr_holder = node.xpath(".//ce:copyright/text()")
        #cr_year = node.xpath(".//ce:copyright/@year")
        #cr_statement = node.xpath(".//ce:copyright/@type").extract()
        #if not (cr_statement or cr_holder) or "unknown" in " ".join(cr_statement).lower():
            #cr_statement = node.xpath(".//prism:copyright/text()").extract()
            #if len(cr_statement) > 1:
                #cr_statement = [
                    #st for st in cr_statement if "unknown" not in st.lower()]

        #copyrights = {}
        #if cr_holder:
            #copyrights["cr_holder"] = self._fix_node_text(cr_holder.extract())
        #if cr_year:
            #copyrights["cr_year"] = cr_year.extract_first()
        #if cr_statement:
            #copyrights["cr_statement"] = get_first(cr_statement)

        #return copyrights


    #def get_doctype(self, node):
        #"""Return a doctype mapped from abbreviation."""
        #abbrv_doctype = node.xpath(".//@docsubtype").extract()
        #doctype = ''
        #if abbrv_doctype:
            #doctype = self.DOCTYPE_MAPPING[get_first(abbrv_doctype)]
        #elif node.xpath(".//ja:article"):
            #doctype = "article"
        #elif node.xpath(".//ja:simple-article"):
            #doctype = "article"
        #elif node.xpath(".//ja:book-review"):
            #doctype = "book-review"
        #elif node.xpath(".//ja:exam"):
            #doctype = "exam"
        ## A scientific article in a conference proceedings is not cnf.
        #if node.xpath(".//conference-info"):
            #doctype = "conference_paper"
        #if doctype:
            #return doctype


    #@staticmethod
    #def _get_sd_url(xml_file):
        #"""Construct a sciencedirect url from the xml filename."""
        #try:
            #basename = os.path.basename(xml_file)
            #elsevier_id = os.path.splitext(basename)[0]
            #url = u"http://www.sciencedirect.com/science/article/pii/" + elsevier_id
        #except AttributeError:
            #url = ''
        #return url

    def parse_node(self, response, node):
        """Parse the Inspire records XML."""
        node.remove_namespaces()
        recid = node.xpath("./controlfield[@tag='001']/text()").extract_first()
        doi = node.xpath("./datafield[@tag='024']/subfield[@code='a']/text()").extract_first()
        pass
        # then request to somewhere

    #def parse_elsevier_xml(self, response, node):
        #"""Get information about the journal."""
        #info = {}
        #xml_file = response.meta.get("xml_url")
        #dois = self.get_dois(node)
        #fpage = node.xpath('.//prism:startingPage/text()').extract_first()
        #lpage = node.xpath('.//prism:endingPage/text()').extract_first()
        #issn = node.xpath('.//prism:issn/text()').extract_first()
        #volume = node.xpath('.//prism:volume/text()').extract_first()
        #issue = node.xpath('.//prism:number/text()').extract_first()
        #journal_title, section = self.get_journal_and_section(
            #self._get_publication(node))
        #year, date_published = self.get_date(node)
        #conference = node.xpath(".//conference-info").extract_first()

        #if section and volume:
            #volume = section + volume
        #if volume:
            #info["volume"] = volume
        #if journal_title:
            #info["journal_title"] = journal_title
        #if issn:
            #info["issn"] = issn
        #if issue:
            #info["issue"] = issue
        #if fpage and lpage:
            #info["fpage"] = fpage
            #info["lpage"] = lpage
            #info["page_nr"] = int(lpage) - int(fpage) + 1
        #elif fpage:
            #info["fpage"] = fpage
        #if year:
            #info["year"] = year
        #if date_published:
            #info["date_published"] = date_published
        #if dois:
            #info["dois"] = dois
        #if conference:
            #info["conference"] = conference

        ## Test if need to scrape additional info:
        #keys_wanted = set([
            #"journal_title", "volume", "issue", "fpage", "lpage", "year",
            #"date_published", "dois", "page_nr",
        #])
        #keys_existing = set(info.keys())
        #keys_missing = keys_wanted - keys_existing

        #if len(keys_missing) > 0:
            #sd_url = self._get_sd_url(xml_file)
            #if sd_url:
                #request = Request(sd_url, callback=self.scrape_sciencedirect)
                #request.meta["info"] = info
                #request.meta["keys_missing"] = keys_missing
                #request.meta["node"] = node
                #request.meta["xml_url"] = xml_file
                #request.meta["handle_httpstatus_list"] = self.ERROR_CODES
                #return request

        #response.meta["info"] = info
        #response.meta["node"] = node
        #return self.build_item(response)


    #@staticmethod
    #def get_license(node):
        #"""Get the license."""
        #pub_license_url = node.xpath(
            #".//oa:userLicense/text()").extract_first()
        #lic_text = 'http://creativecommons.org/licenses/by/3.0'
        #if pub_license_url and pub_license_url.startswith(lic_text):
            #pub_license = u'CC-BY-3.0'
            #return pub_license, pub_license_url
        #else:
            #return '', ''

    #def add_fft_file(self, file_path, file_access, file_type):
        #"""Create a structured dictionary and add to 'files' item."""
        #file_dict = {
            #"access": file_access,
            #"description": self.name.title(),
            #"url": file_path,
            #"type": file_type,
        #}
        #return file_dict

    def build_item(self, response):
        """Parse an Elsevier XML file into a HEP record."""
        node = response.meta.get("node")
        record = HEPLoader(
            item=HEPRecord(), selector=node, response=response)
        #doctype = self.get_doctype(node)

        license = pub_license_url = node.xpath(".//oa:userLicense/text()").extract_first()
        #xml_file = response.meta.get("xml_url")
        #if xml_file:
            #record.add_value('additional_files', self.add_fft_file(xml_file, "HIDDEN", "Fulltext"))
            #sd_url = self._get_sd_url(xml_file)
            #if requests.head(sd_url).status_code == 200:  # Test if valid url
                #record.add_value("urls", sd_url)

        #pub_license, pub_license_url = self.get_license(node)
        #if pub_license:
            #record.add_value('license', pub_license)
            #record.add_value('license_url', pub_license_url)
            #record.add_value('license_type', "Open access")

        ## record.add_xpath("urls", "//prism:url/text()")  # We don't want dx.doi urls

        #copyrights = self.get_copyright(node)
        #record.add_value('copyright_holder', copyrights.get("cr_holder"))
        #record.add_value('copyright_year', copyrights.get("cr_year"))
        #record.add_value('copyright_statement', copyrights.get("cr_statement"))


        return record.load_item()
