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

from ..comparators import compare_titles


class CPCSpider(XMLFeedSpider):

    """CPC crawler


    This spider takes metadata records which are stored in a local XML file.

    Write more here

    Example usage:
    scrapy crawl cpc
    scrapy crawl cpc -a source_dir=`pwd`/tests/responses/cpc/xml/ -s "JSON_OUTPUT_DIR=tmp/"
    scrapy crawl cpc -a source_file=file://`pwd`/tests/responses/cpc/inspire_xml/records1.xml -s "JSON_OUTPUT_DIR=tmp/" -s "LOG_FILE=cpc.log"
    scrapy crawl cpc -a source_file=file://`pwd`/tests/responses/cpc/inspire_xml/test1.xml -s "JSON_OUTPUT_DIR=tmp/" -s "LOG_FILE=cpc.log"

    scrapy crawl cpc -a source_dir=`pwd`/tests/responses/cpc/inspire_xml/ -s "JSON_OUTPUT_DIR=tmp/" -s "LOG_FILE=cpc.log"

    Happy crawling!
    """

    name = 'cpc'
    start_urls = []
    iterator = 'xml'  # Needed for proper namespace handling
    itertag = "//*[local-name()='record']"
    download_delay = 10
    domain = "http://cpc-hepnp.ihep.ac.cn:8080/Jwk_cpc/EN"
    fixed_metadata = True  # True: assume correct marc_773
    only_update_773 = False  # True: correct, False: append
    testing = False  # True: don't make get requests to CPC server

    custom_settings = {
        'ITEM_PIPELINES': {
            'hepcrawl.pipelines.XmlWriterPipeline': 300,
        },
    }

    def __init__(self, source_file=None, source_dir=None, *args, **kwargs):
        """Construct CPC spider."""
        super(CPCSpider, self).__init__(*args, **kwargs)
        self.source_file = source_file
        self.source_dir = source_dir
        self.start_urls = self.get_start_urls()
        self.splash_files = self.load_splash_directory()
        #self.splash_files = None
        self.full_metadata = self.load_full_metadata()


    def start_requests(self):
        """Default starting point for scraping shall be the local XML file."""
        if self.source_file:
            yield Request(self.source_file)
        elif self.source_dir:
            for s_file in self.listdir_fullpath(self.source_dir):
                source_file = "file://" + s_file
                yield Request(source_file)
        else:
            for url in self.start_urls:
                yield Request(url, callback=self.scrape_issue_listing)

    def listdir_fullpath(self, d):
        return [os.path.join(d, f) for f in os.listdir(d)]

    def get_start_urls(self):
        """Construct all the start urls for splash page getting."""
        # years = range(1977, 2008)
        return [] # NOTE: We have everything now!
        urls = []

        for year in years:
            vol_url = "http://cpc-hepnp.ihep.ac.cn:8080/Jwk_cpc/EN/article/showTenYearVolumnDetail.do?nian=" + str(year)
            urls.append(vol_url)

        return urls


    def scrape_issue_listing(self, response):
        """Scrape issue listing of one volume.

        Create all the files inside the loop one at a time.
        """
        node = response.selector
        # Be sure to ignore the top banner advertising newest issues, use not(img)!
        issues = node.xpath("//td/a[contains(@href, 'volumn_')][not(img)]")

        for issue in issues:
            raw_link = issue.xpath("@href").extract_first().lstrip("..").rstrip(".shtml")
            link = self.domain + raw_link + "_abs.shtml"
            raw_text = issue.xpath("text()").extract_first()
            pubinfo = self.dig_pubinfo_from_issue_listing(raw_text)

            self.get_issue_splash_pages(link, pubinfo)
            time.sleep(10)  # We don't want to get banned!


    def dig_pubinfo_from_issue_listing(self, raw_text):
        """Dig pubinfo from CPC volumn (issue listing) web page."""
        raw_text = " ".join(raw_text.split())
        # NOTE: Issue can be a special issue like "S1", i.e. non-digit.
        if "1999" in raw_text:
            # 1999 actually doesnt' contain page_ranges
            search_pattern = re.compile(r'(\d\d\d\d) Vol\.(\d+) No\.(..) pp\. (\d+-\d+).?')
        else:
            search_pattern = re.compile(r'.?(\d\d\d\d).?Vol\.(\d+).?No\.(..).?pp\.(\d+-\d+).?')
        search_result = search_pattern.search(raw_text)
        try:
            if search_result:
                year = search_result.group(1)
                vol = search_result.group(2)
                issue = search_result.group(3)
                page_range = search_result.group(4)
        except IndexError:
            # NOTE: I wonder if there really are any IndexErrors
            raise

        return {
            "year": year,
            "vol": vol,
            "issue": issue,
            "page_range": page_range,
        }

    def get_issue_splash_pages(self, link, pubinfo):
        """Get one issue splash page and write to file."""
        page_stream = requests.get(link)
        # filenames format: year_vol_issue_page_range.xml
        filename = "{}_{}_{}_{}_CPC.html".format(
                                        pubinfo["year"],
                                        pubinfo["vol"],
                                        pubinfo["issue"],
                                        pubinfo["page_range"]
                                        )
        # NOTE: filepath creation is very clunky at the moment:
        filepath = "/home/henrikv/.virtualenvs/hepcrawl/src/hepcrawl/tmp/cpc/"
        full_path = filepath + filename

        with open(full_path, "w") as f:
            f.write(page_stream.content)


    def load_splash_directory(self):
        """Load the splash page directory and set up a dictionary for easy access."""
        # NOTE: The actual metadata is not in the dict...just pointers SLOW
        splashdir = "/home/henrikv/.virtualenvs/hepcrawl/src/hepcrawl/tests/responses/cpc/cpc_splash/"
        from os import listdir
        from os.path import isfile, join

        splash_dict = {}
        all_html_files = sorted([f for f in listdir(splashdir) if "html" in f])
        for filename in all_html_files:
            full_path = "file://" + splashdir + filename
            filename = filename.split("_")
            year = filename[0]
            vol = filename[1]
            issue = filename[2]
            page_range = filename[3]

            if year not in splash_dict:
                splash_dict[year] = []

            if issue[0] == "0" and len(issue) > 1:
                issue = issue[1:]

            splash_dict[year].append({
                "vol": vol,
                "issue": issue,
                "page_range": page_range,
                "filename": full_path
                })

        return splash_dict

    def load_full_metadata(self):
        """Load the CSV file provided my CPC staff.

        This contains all the metadata. Maybe handling this is easier than scraping
        multiple html pages. Return a list of dicts.
        """
        collection = []
        import csv
        metafile = "/home/henrikv/.virtualenvs/hepcrawl/src/hepcrawl/tests/responses/cpc/full_metadata/cpc_full_metadata.csv"
        with open(metafile, "r") as csvfile:
            meta_reader = csv.reader(csvfile, delimiter=';', quotechar='"')
            for i, row in enumerate(meta_reader):
                if i==0:
                    # First row is the header
                    keys = row
                else:
                    collection.append({k: v for k, v in zip(keys, row)})

        return collection


    def add_fft_file(self, pdf_file, file_access, file_type):
        """Create a structured dictionary and add to 'files' item."""
        file_dict = {
            "access": file_access,
            "url": pdf_file,
            "type": file_type,
        }
        return file_dict

    def fix_node_text(self, text_nodes):
        """Join text split to multiple elements.
        Also clean unwantend whitespaces. Input must be a list.
        Returns a string.
        """
        # FIXME: is this causing the problem with strange missing whitespaces?
        title = " ".join(" ".join(text_nodes).split())
        return title

    def fix_punctuation_whitespace(self, text):
        """Add whitespace after punctuation."""
        text = re.sub(r'\.([a-zA-Z])', r'. \1', text)
        text = re.sub(r'\,([a-zA-Z])', r', \1', text)

        return text

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

    def find_issue_file(self, pubinfo):
        """Find the correct splash page file from local directory.

        Also return the correct issue if it wasn't in the Inspire metadata.
        WARNING: Inspire metadata is completely messed up, page numbers and issues
        are wrong for many records!
        """
        fpage = pubinfo["fpage"]
        issue = pubinfo["issue"]
        year = pubinfo["year"]
        recid = pubinfo["recid"]
        filename = ""
        correct_issue = ""

        for issue_file in self.splash_files[year]:
            if not issue:
                if self.test_string_in_range(fpage, issue_file["page_range"]):
                    filename = issue_file["filename"]
                    correct_issue = issue_file["issue"]
                    break
            else:
                if issue_file["issue"] == issue:
                    filename = issue_file["filename"]
                    break

        return filename, correct_issue


    def test_string_in_range(self, test_string, range_string):
        """Test if a int string is in range of string range.
        E.g. test if "5" is in "1-50".
        """
        srange = range_string.split("-")
        if int(srange[0]) <= int(test_string) <= int(srange[1]):
            return True
        else:
            return False
        #NOTE: Something will probably go wrong here somewhere?


    def parse_node(self, response, node):
        """Get all the data from an Inspire record."""
        node.remove_namespaces()
        recid = node.xpath("./controlfield[@tag='001']/text()").extract_first()
        vol = node.xpath("./datafield[@tag='773']/subfield[@code='v']/text()").extract_first()
        year = node.xpath("./datafield[@tag='773']/subfield[@code='y']/text()").extract_first()
        issue = node.xpath("./datafield[@tag='773']/subfield[@code='n']/text()").extract_first()
        page_range = node.xpath("./datafield[@tag='773']/subfield[@code='c']/text()").extract_first()
        abstract = node.xpath("./datafield[@tag='520']/subfield[@code='a']/text()").extract_first()
        title = node.xpath("./datafield[@tag='245']/subfield[@code='a']/text()").extract_first()
        marc_773 = self.marc_to_dict(node, "773")

        if not year:
            self.logger.warning("No year found for recid " + recid)
            year = ""
            yield
            return
        # NOTE: we have permission to download only up to year 2008.
        if int(year) >= 2008:
            self.logger.info("Year is greater than 2008 for " + recid + ". Skipping.")
            yield
            return
        if not issue:
            issue = ""

        if not page_range:
            self.logger.warning("No page range found for recid " + recid)
            page_range = ""
            fpage = ""
        else:
            if "-" in page_range:
                fpage = page_range.split("-")[0]
            else:
                fpage = page_range
            fpage = filter(lambda x: x.isdigit(), fpage)

        pubinfo = {
            "year": year,
            "vol": vol,
            "issue": issue,
            "fpage": fpage,
            "recid": recid,
            "abstract": abstract,
            "title": title,
            "marc_773": marc_773
        }

        if not self.only_update_773 and self.splash_files:
            # If you want to use the individual CPC splash pages as source
            # This is the original approach, use this for appending stuff
            self.fixed_metadata = True  # HACK, True if the fpages are correct
            if self.fixed_metadata:
                splash_issuefile, issue = self.find_issue_file(pubinfo)
                marc_773["n"] = issue
                pubinfo["marc_773"] = marc_773

                if not splash_issuefile:
                    if pubinfo["year"] == "1999":
                        self.logger.info("Record from year 1999 skipped, recid " + recid)
                    else:
                        self.logger.warning("Didn't find splash record for recid " + recid + ". Skipping.")
                    yield
                    return

                request = Request(splash_issuefile, callback=self.parse_issue_file)
                request.meta["pubinfo"] = pubinfo
                yield request
            # else:
                # # Here should go through all the issues because the issue numbers
                # # and fpages are mostly wrong in Inspire metadata.
                # # NOTE: This takes forever. Use only if no full metadata available
                # for splash_issuefile in self.splash_files[year]:
                    # self.logger.info("Parsing vol. " + splash_issuefile["vol"] + ", issue " + splash_issuefile["issue"])
                    # request = Request(splash_issuefile["filename"], callback=self.parse_issue_file)
                    # marc_773["n"] = splash_issuefile["issue"]
                    # pubinfo["marc_773"] = marc_773
                    # request.meta["pubinfo"] = pubinfo
                    # yield request
        elif self.only_update_773 and self.full_metadata:
            # We got the metadata as one CSV file which is loaded to a dictionary.
            # NOTE: with this it's not possible to get PDFs, so only correct run
            correct_record = self.compare_with_dict_metadata(pubinfo)
            if not correct_record:
                self.logger.warning("Titles don't match. Could not find splash metadata for recid " + recid)
                yield
                return
            fpage = correct_record["Start Page"]
            lpage = correct_record["End Page"]

            pubinfo["marc_773"]["n"] = correct_record["Issue"]
            pubinfo["marc_773"]["v"] = correct_record["Volume"]
            pubinfo["marc_773"]["y"] = correct_record["Year"]
            pubinfo["marc_773"]["c"] = "{}-{}".format(fpage, lpage)

            response.meta["recid"] = recid
            response.meta["pdf_link"] = pubinfo.get("afs_filepath")
            response.meta["marc_773"] =  pubinfo.get("marc_773")

            yield self.build_item(response)

    def compare_with_dict_metadata(self, pubinfo):
        """Loop the dictionary of try to find a title that matches
        with the one in Inspire metadata.

        This is an better and faster alternative to scraping the splash pages
        with `parse_issue_file`. But with this it's not possible to get PDFs :(
        """
        for record in self.full_metadata:
            splash_title = record["English Title"]
            inspire_title = pubinfo["title"]
            jaccard_similarity, same_titles = compare_titles(inspire_title,
                                                            splash_title,
                                                            method="jaccard",
                                                            required=0.4)
            year_diff = abs(int(record["Year"]) - int(pubinfo["year"]))
            if pubinfo["fpage"] == record["Start Page"]:
                if same_titles:
                    return record
            elif same_titles and year_diff <= 2:
                print("\n")
                print("Inspire: {0}, {1}({2}) {3}".format(inspire_title, pubinfo["vol"], pubinfo["year"], pubinfo["marc_773"].get("c")))

                print("Splash: {0}, {1}({2}) {3}-{4}".format(
                    splash_title, record["Volume"], record["Year"],
                    record["Start Page"], record["End Page"]))

                print("\n")
                print("http://inspirehep.net/record/" + pubinfo["recid"])

                really_same = raw_input("Are they really the same? (y/n)") or "y"
                if really_same == "y":
                    return record



    def parse_issue_file(self, response):
        """Parse the issue file and fetch metadata for a record.

        Finally call build_item.
        """
        node = response.selector
        pubinfo = response.meta["pubinfo"]
        records = node.xpath("//table[tr/td/a[contains(@onclick, 'PDF')]]")
        recid = pubinfo["recid"]
        correct_record = ""
        inspire_title = pubinfo["title"]

        for record in records:
            splash_pubinfo_line = record.xpath("tr/td[a[contains(@onclick, 'PDF')]]/text()").extract_first()
            splash_pubinfo = self.dig_pubinfo_from_record_listing(splash_pubinfo_line)
            splash_title = record.xpath("tr[td/a[contains(@onclick, 'PDF')]]/preceding-sibling::tr/td/a/b").extract_first()
            if not self.fixed_metadata:
                # Check if INSPIRE title matches with splash page title:
                jaccard_similarity, same_titles = compare_titles(inspire_title, splash_title, method="jaccard", required=0.6)
                # We can check both fpage and title similarity:
                if pubinfo["fpage"] == splash_pubinfo["fpage"]:
                    if same_titles:
                        correct_record = record
                        break
            else:
                if pubinfo["fpage"] == splash_pubinfo["fpage"]:
                    correct_record = record
                    break
            # FIXME: this is commented to fasten up the process; now we have some
            # correct fpages so don't want extra title matching.
            #elif same_titles:
                #self.logger.warning("Wrong fpage, but name matches for recid " + recid)
                #correct_record = record
                ## NOTE: Get new (correct?) page range. These should NOT be
                ## appended, but corrected!
                #pubinfo["marc_773"]["c"] = splash_pubinfo["page_range"]
                #break

        if not correct_record:
            self.logger.warning("Titles don't match. Could not find splash metadata for recid " + recid)
            #self.logger.info("Highest similarity " + str(highest_sim) + " with issue " + pubinfo["marc_773"]["n"])
            return None

        #self.logger.info("jaccard distance: " + str(highest_sim) + " for recid "  + recid)

        if self.only_update_773:
            response.meta["recid"] = recid
            response.meta["marc_773"] =  pubinfo.get("marc_773")
            return self.build_item(response)
        else:
            if not pubinfo.get("abstract"):
                abs_element = correct_record.xpath(
                    "tr[td/a[contains(@onclick, 'PDF')]]/preceding-sibling::tr/td")[-1]
                abstract = self.fix_node_text(abs_element.xpath("child::node()").extract())
                abstract = self.fix_punctuation_whitespace(abstract)
                response.meta["abstract"] = abstract
            try:
                pdf_id = self.get_pdf_id(correct_record)
            except AttributeError:
                self.logger.warning("PDF not available for recid " + recid)
                return None

            pubinfo["afs_filepath"] = self.write_pdf_file(
                splash_pubinfo,
                pdf_id,
                recid,
                testing=self.testing  # NOTE: this is to avoid unnecessary requests when developing
            )

            response.meta["recid"] = recid
            response.meta["pdf_link"] = pubinfo.get("afs_filepath")
            response.meta["marc_773"] =  pubinfo.get("marc_773")

            return self.build_item(response)

    def get_pdf_id(self, correct_record):
        """Parse the pdf_click attribute for the PDF id."""
        pdf_click = correct_record.xpath("tr/td/a[contains(@onclick, 'PDF')]/@onclick").extract_first()
        search_pattern = re.compile(r'.?(\d+).?')
        try:
            search_result = search_pattern.search(pdf_click).group(1)
        except AttributeError:
            raise
        return search_result


    def write_pdf_file(self, splash_pubinfo, pdf_id, recid, testing=True):
        """Write a local copy of the PDF file and return AFS file path.
        NOTE: Remember to upload the files to the AFS directory before using
        Batchupload.
        """
        # We have to download the pdf locally first, because they don't have
        # file extensions so Inspire won't accept them.
        url_to_get = "http://cpc-hepnp.ihep.ac.cn:8080/Jwk_cpc/EN/article/downloadArticleFile.do?attachType=PDF&id=" + pdf_id

        localpath = "/home/henrikv/.virtualenvs/hepcrawl/src/hepcrawl/tmp/cpc_pdf/"
        afspath = "/afs/cern.ch/project/inspire/uploads/library/CPC/"
        filename = "{}_{}_{}_{}.pdf".format(
            splash_pubinfo["year"],
            splash_pubinfo["vol"],
            splash_pubinfo["page_range"],
            recid)

        if not testing:
            pdf_stream = requests.get(url_to_get)
            with open(localpath + filename, "w") as f:
                f.write(pdf_stream.content)
            time.sleep(5)  # We don't want to get banned!

        return afspath + filename


    def dig_pubinfo_from_record_listing(self, pubinfo_line):
        """Dig pubinfo from the line on the issue (record listing) splash page."""
        raw_pubinfo = " ".join(pubinfo_line.split())
        search_pattern = re.compile(r'(\d\d\d\d)\sVol\.\s(\d+)\s\(\d+\): (\d+-\d+)')
        search_result = search_pattern.search(raw_pubinfo)
        if not search_result:
            search_pattern = re.compile(r'(\d\d\d\d)\sVol\.\s(\d+)\s\(..\): (\d+-\d+)')
            search_result = search_pattern.search(raw_pubinfo)
        if search_result:
            year = search_result.group(1)
            vol = search_result.group(2)
            page_range = search_result.group(3)

        try:
            assert page_range
        except UnboundLocalError:
            # FIXME: here should handle non-standard messy strings?
            # Problematic e.g.: 2002 Vol. 26 (S1): 0-03
            import ipdb; ipdb.set_trace()

        # Create also fpage
        if "-" in page_range:
            fpage = page_range.split("-")[0]
        else:
            fpage = page_range
        fpage = filter(lambda x: x.isdigit(), fpage)

        return {
            "year": year,
            "vol": vol,
            "page_range": page_range,
            "fpage": fpage,
        }


    def build_item(self, response):
        """Build the final record."""
        node = response.selector
        record = HEPLoader(item=HEPRecord(), selector=node, response=response)

        pdf_link = response.meta.get("pdf_link")

        record.add_value("recid", response.meta.get("recid"))
        # NOTE: if you want to add issues, do it with a separate run
        # because they must be corrected, not appended:
        #
        if self.only_update_773:
            record.add_value("marc_773", response.meta.get("marc_773"))
        else:
            record.add_value("abstract", response.meta.get("abstract"))
            record.add_value('additional_files', self.add_fft_file(pdf_link, "INSPIRE-PUBLIC", "Fulltext"))

        return record.load_item()
