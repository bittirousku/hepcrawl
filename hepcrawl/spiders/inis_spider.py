# -*- coding: utf-8 -*-
#
# This file is part of hepcrawl.
# Copyright (C) 2015, 2016 CERN.
#
# hepcrawl is a free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""Spider for ALPHA."""

from __future__ import absolute_import, print_function

import re

import dateutil
from urlparse import urljoin

from scrapy import Request
from scrapy.spiders import XMLFeedSpider, CrawlSpider

from ..items import HEPRecord
from ..loaders import HEPLoader
from ..utils import has_numbers

# We have a pdf file for these report numbers:
pdf_numbers = {
    19105948, 19105949, 19105950, 19105951, 19105952, 19105953, 19105954,
    19105955, 19105956, 19105957, 19105958, 19105959, 19105960, 19105961,
    19105962, 19105963, 19105964, 19105965, 19105966, 19105967, 19105991,
    19105992, 19105993, 19105994, 19105995, 19105996, 19105997, 19105998,
    19105999, 19106000, 19106001, 19106002, 19106003, 19106004, 19106005,
    19106006, 19106007, 19106008, 19106009, 19106028, 19106053, 19106054,
    19106055, 19106056, 19106057, 19106058, 19106064, 19106065, 19106066,
    19106067, 19106068, 19106069, 19106070, 19106071, 19106072, 19106073,
    19106074, 19106075, 19106076, 19106077, 19106078, 19106079, 19106080,
    19106081, 19106082, 19106083, 19106084, 19106085, 19106086, 19106087,
    19106088, 19106089, 19106090, 19106091, 19106092, 19106093, 19106094,
    19106095, 19106096, 19106132, 19106133, 19106134, 19106135, 19106136,
    19106137, 19106138, 19106139, 19106140, 19106141, 19106142, 19106143,
    19106144, 19106145, 19106146, 19106167, 19106168, 19106169, 19106170,
    19106171, 19106172, 19106173, 19106174, 19106175, 19106176, 19106177,
    19106178, 20012342
}

proceedings_fulltext_attached_on_inis = {
    11506523,
    11570979,
    12582846,
    17065215,
    17085748,
    19102901,
    19100851,
}

class INISSpider(CrawlSpider):

    """INIS crawler
    Scrapes theses metadata from INIS Excel file.
    https://inis.iaea.org/search/search.aspx?q=source%3A%226.+All-union+conference+on+charged+particle+accelerators%3B+Dubna%2C+USSR%3B+11+-+13+Oct+1978%22&src=inws

    example usage:
    scrapy crawl INIS_excel -a source_file=file://`pwd`/tests/responses/inis/test.csv -s "JSON_OUTPUT_DIR=tmp/"
    scrapy crawl INIS_excel -a source_file=file://`pwd`/tests/responses/inis/dubna_proceedings.csv -s "JSON_OUTPUT_DIR=tmp/"

    Happy crawling!
    """

    name = 'INIS_excel'

    custom_settings = {
        'ITEM_PIPELINES': {
            'hepcrawl.pipelines.XmlInsertPipeline': 300,
        },
    }

    def __init__(self, source_file=None, *args, **kwargs):
        """Construct INIS spider"""
        super(INISSpider, self).__init__(*args, **kwargs)
        self.source_file = source_file

    def start_requests(self):
        """You can also run the spider on local test files"""
        if self.source_file:
           yield Request(self.source_file)

    def parse_csv_string(self, response):
        """Convert the CVS file to JSON."""
        # FIXME: other option is to use csv module
        # Actually the best thing would be not to use HEPcrawl to do this..?

        lines = response.body.strip("\n").split("\n")

        header = lines.pop(0).replace("\"", "")  # I converted the XLSX with stupid quotes on strings....
        header_keys = header.split("\t")
        list_of_dicts = []

        for line in lines:
            metadata_dict = {}
            line = line.replace("\"", "")
            record = line.split("\t")
            assert len(header_keys) == len(record)
            for index, key in enumerate(header_keys):
                metadata_dict[key] = record[index] if record[index] != "NULL" else ''
            list_of_dicts.append(metadata_dict)

        return list_of_dicts

    def get_authors(self, jsonrecord):
        """Get authors."""
        authors = []
        authors_raw = jsonrecord["Authors"]
        # First should remove the affiliations, otherwise the regex won't
        # behave nicely.
        authors_raw = re.sub("\(.*?\)", "", authors_raw).rstrip(".").rstrip()
        authorlist = re.findall(r'(\w+\'*, \w*\.\w*\.*)', authors_raw)
        # import ipdb; ipdb.set_trace()

        for author in authorlist:
            authors.append({
                "raw_name": author,
            })

        if not authors:
            # This should not happen
            import ipdb; ipdb.set_trace()
        return authors

    def get_page_nrs(self, jsonrecord):
        """Get the page range and page numbers."""
        pages_raw = jsonrecord["Pages"]

        if "-" in pages_raw:
            fpage, lpage = re.search(r'(\d+)-(\d+)', pages_raw).groups()
            page_range = '{}-{}'.format(fpage, lpage)
            page_nr = str(int(lpage) - int(fpage) + 1)
        else:
            page_range = re.search(r'(\d+)', pages_raw).group(0)
            page_nr = ''


        return page_range, page_nr

    def get_cnum(self, jsonrecord):
        """Get CNUM."""
        date = jsonrecord["Cond Date"]

        try:
            fday, lday, month, year = re.search(r'^(\d+)\s*-\s*(\d+)\s(\w+)\s(\d+).$', date).groups()
        except AttributeError:
            # Don't want this to happen
            import ipdb; ipdb.set_trace()


        month = dateutil.parser.parse(month).month

        # FIXME: be careful, the cnum might be different if there are
        # multiple conferences on the same day!
        cnum = 'C{}-{}-{}'.format(year[-2:], month, fday)

        return cnum

    def create_reportnr_dicts(self, jsonrecord):
        """Create structured dictionaries to add to 'report_numbers' item."""
        report_no = jsonrecord["RN"]
        if report_no:
            return {
                    'value': report_no,
                    'source': "INIS",
                }

    def create_public_notes_dicts(self, jsonrecord):
        """Create structured dictionaries to add to 'report_numbers' item."""
        public_note = jsonrecord["Physical Description"]
        if public_note:
            return {
                    'value': public_note.strip(),
                    'source': "INIS",
                }

    def get_classification_dicts(self, term, scheme):
        """Get content classification."""
        return {
                'value': term,
                'scheme': scheme,
            }

    def get_controlled_keywords(self, jsonrecord):
        keywords_raw = jsonrecord["Descriptors"].lower().rstrip(".")
        kwlist =  [keyw.capitalize() for keyw in keywords_raw.split("; ")]
        keywords = []

        for keyw in kwlist:
            keywords.append(
                {
                'value': keyw,
                'scheme': 'INIS',
                }
            )

        return keywords


    def create_fft_dicts(self, file_path):
        """Create a structured dictionary and add to 'files' item."""
        file_dict = {
            "access": "INSPIRE-PUBLIC",
            "description": "INIS",
            "url": file_path,
            "type": "Fulltext",
        }

        return file_dict




    def parse(self, response):
        """Parse INIS CSV string into a HEP record."""
        json_responses = self.parse_csv_string(response)

        for jsonrecord in json_responses:
            #import ipdb; ipdb.set_trace()
            language = "Russian"
            report_no = self.create_reportnr_dicts(jsonrecord)
            title = jsonrecord["Original Title"]
            title_translated = jsonrecord["Title"]
            authors = self.get_authors(jsonrecord)
            page_range, page_nr = self.get_page_nrs(jsonrecord)
            date_published = jsonrecord["Publ Year"]
            abstract = jsonrecord["Abstract"]
            description = jsonrecord["Physical Description"]
            collections = ["HEP", "ConferencePaper"]
            controlled_keywords = self.get_controlled_keywords(jsonrecord)
            cnum = self.get_cnum(jsonrecord)
            content_classification = self.get_classification_dicts(
                "Accelerators", "INSPIRE"
            )
            reportno = jsonrecord["RN"]

            record = HEPLoader(item=HEPRecord(), selector=jsonrecord, response=response)

            # NOTE: Watch out for incorrect titles. If the original
            # is in English, the "Original Title" will be the title of the whole
            # proceedings in Russian (Trudy). Also note that the language of all the
            # records will be either "Russian" or "NULL". In these cases
            # there should be no translated title and the title should be "Title"
            # No language field should be present.
            if jsonrecord["Language"] == "":
            # if "Trudy devyatogo" in  jsonrecord["Original Title"]:
                record.add_value("title", jsonrecord["Title"].rstrip("."))
            else:
                record.add_value("title", jsonrecord["Original Title"].rstrip("."))
                record.add_value("title_trans", jsonrecord["Title"].rstrip("."))
                record.add_value("language", language)

            if abstract:
                record.add_value("abstract", jsonrecord["Abstract"])
            record.add_value("authors", self.get_authors(jsonrecord))
            record.add_value("controlled_keywords", self.get_controlled_keywords(jsonrecord))
            record.add_value("public_notes", self.create_public_notes_dicts(jsonrecord))
            record.add_value("content_classification", content_classification)
            record.add_value("date_published", jsonrecord["Publ Year"])
            if page_nr:
                record.add_value("page_nr", page_nr)
            record.add_value("collections", collections)
            record.add_value("report_numbers", self.create_reportnr_dicts(jsonrecord))


            marc_773 = {
                "c": page_range,
                "w": cnum,
            }
            record.add_value("marc_773", marc_773)

            if int(reportno) in pdf_numbers:
                afs_path = "/afs/cern.ch/project/inspire/uploads/library/dubna/" + reportno + ".pdf"
                record.add_value("additional_files", self.create_fft_dicts(afs_path))


            # Theses fields we want:
            ## 100/700 for the authors
            ## 041__a:Russian
            ## 245__a:Russian title
            ## 242__a:translated title
            ## 035__a:<RN> with $$9:INIS
            ## 773__c:Pages $$w<CNUM>
            ## 260__c:Publ Year
            ## 500__a:Physical description 9:INIS
            ## 520__a:abstract 9:INIS
            ## 980__a:HEP + ConferencePaper
            ## 65017a:Accelerators 2:INSPIRE
            ## 6531a: keywords 9: publisher



            yield record.load_item()
