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

from urlparse import urljoin

from scrapy import Request
from scrapy.spiders import XMLFeedSpider, CrawlSpider

from ..items import HEPRecord
from ..loaders import HEPLoader
from ..utils import has_numbers


class INISSpider(CrawlSpider):
#class INISSpider(XMLFeedSpider):

    """INIS crawler
    Scrapes theses metadata from INIS experiment web page.
    https://inis.iaea.org/search/search.aspx?q=source%3A%226.+All-union+conference+on+charged+particle+accelerators%3B+Dubna%2C+USSR%3B+11+-+13+Oct+1978%22&src=inws

    example usage:
    scrapy crawl INIS -a source_file=file://`pwd`/tests/responses/inis/records_1.htm -s "JSON_OUTPUT_DIR=tmp/"

    Happy crawling!
    """

    name = 'INIS'
    start_urls = ["https://inis.iaea.org/search/search.aspx?q=source%3A%226.+All-union+conference+on+charged+particle+accelerators%3B+Dubna%2C+USSR%3B+11+-+13+Oct+1978%22&src=inws"]
    itertag = '//div[@id="main"]//div[@class="rightNav"]/div[@class="g1"]'

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
        elif self.start_urls:
            i = 0
            while i < 242:
                url = self.start_urls[0] + "&start" + i
                i += 10
                yield Request(url)


    def fix_affiliation_line(self, affiliations):
        """Remove possible brackets from beginning and end.

        Note that strip("()") removes all brackets from end, which is not
        desirable: we might have something like "(Moskva (USSR))"
        """
        fixed_affs = []
        for aff in affiliations:
            aff = aff.strip()
            if aff[0] == "(" and aff[-1] == ")":
                fixed_affs.append(aff[1:-1])

        return fixed_affs


    def get_authors(self, record):
        """Get authors."""
        authors = []
        authors_raw = record.xpath('.//span[@class="aut-cc author"]/a[@class="author-name"]')
        for author in authors_raw:
            author_name = author.xpath('./text()').extract_first()
            # FIXME: affiliations could be wrong!
            # Now we are just outputting all the affs without concern to which is which
            affiliations = author.xpath('./@data-affiliation').extract()
            affiliations = self.fix_affiliation_line(affiliations)
            authors.append({
                "raw_name": author_name,
                "affiliations": [{'value': aff} for aff in affiliations],
            })

        return authors

    def clean_text(self, text):
        """Remove unwanted whitespaces and newlines from a string."""
        return " ".join(text.split())


    def parse(self, response):
        """Parse Alpha web page into a HEP record."""

        node = response.selector
        publications = node.xpath(self.itertag)

        for pub in publications:
            record = HEPLoader(item=HEPRecord(), selector=node, response=response)
            en_title = pub.xpath('.//div[@class="g1-title"]//span[@class="englishtitle"]/text()').extract_first()
            authors = self.get_authors(pub)  # Remember: first one marc 100, rest marc 700
            conference_title = "All-union conference on charged particle accelerators; Dubna, USSR; 11 - 13 Oct 1978"

            more_info = pub.xpath('./div[@class="expandable"]')
            original_title = more_info.xpath('.//span[@class="cc originaltitle"]/text()').extract_first()
            keywords = more_info.xpath('.//span[@class="cc primarysubject"]//text()').extract()
            doctype = more_info.xpath('.//span[@class="cc recordtype"]//text()').extract_first()
            report_no = more_info.xpath('.//span[@class="cc reportnumber"]//text()').extract_first()
            language = more_info.xpath('.//span[@class="cc language"]//text()').extract_first()
            year = more_info.xpath('.//span[@class="cc year"]//text()').extract_first()
            collections = ['HEP', 'ConferencePaper']
            # license?

            # FIXME: try to extract something from the pubnote:
            raw_pubnote = more_info.xpath('.//span[@class="cc source"]/text()').extract_first()

            record.add_value("title", self.clean_text(en_title))
            record.add_value("orig_title", self.clean_text(original_title))
            record.add_value("authors", authors)
            record.add_value("free_keywords", keywords)

            record.add_value("journal_doctype", doctype)  # FIXME: this is a report, so no journal_?
            record.add_value("journal_year", year)  # FIXME: same
            record.add_value("date_published", year)

            record.add_value("report_numbers", report_no)
            if "english" not in language.lower():
                record.add_value("language", language)

            record.add_value("collections", collections)

            cnum = "C78-10-11"  # NOTE: CNUM is hardcoded here!
            marc_773 = {
                # "c": fpage,  # these are in raw_pubnote but they are wrong!
                "w": cnum,
                # "x":,
                "y": year,
            }

            record.add_value("marc_773", marc_773)



            yield record.load_item()
