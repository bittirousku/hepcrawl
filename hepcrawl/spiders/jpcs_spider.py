# -*- coding: utf-8 -*-
#
# This file is part of hepcrawl.
# Copyright (C) 2016 CERN.
#
# hepcrawl is a free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""Spider for IOP."""

from __future__ import absolute_import, print_function

import os

import tarfile

from tempfile import mkdtemp

from scrapy import Request
from scrapy.spiders import XMLFeedSpider
from ..extractors.nlm import NLM

from ..items import HEPRecord
from ..loaders import HEPLoader


class JPCSSpider(XMLFeedSpider, NLM):
    """IOPSpider crawler.

    This spider goes through Inspire metadata of records with missing pdfs, and
    constructs MARCXML with paths pointing to the missing pdfs.

    1. Extract DOI from the Inspire metadata.

    2. Construct path with the DOI. The paths are taken directly from the DOI.
       I.e. with DOI 10.1088/1742-6596/726/1/012027, the path is
       `pwd`/tmp/jpcs/1742-6596/726/1/012027/JPCS_726_1_012027.pdf

    3. Create MARCXML for appending.

    Example usage:
    .. code-block:: console

        scrapy crawl jpcs -a xml_file=file://`pwd`/custom_harvest/jpcs/inspire_xmls/records1.xml

        scrapy crawl jpcs -a xml_file=file://`pwd`/custom_harvest/jpcs/inspire_xmls/records1.xml -s "JSON_OUTPUT_DIR=tmp/" -s "LOG_FILE=jpcs.log"

    Happy crawling!
    """

    name = 'jpcs'
    start_urls = []
    iterator = 'xml'
    itertag = "//*[local-name()='record']"

    OPEN_ACCESS_JOURNALS = {
        "J. Phys.: Conf. Ser.",
        # FIXME: add more
    }

    custom_settings = {
        'ITEM_PIPELINES': {
            'hepcrawl.pipelines.XmlWriterPipeline': 300,
        },
    }

    def __init__(self, zip_file=None, xml_file=None, pdf_files=None, *args, **kwargs):
        """Construct JPCS spider."""
        super(JPCSSpider, self).__init__(*args, **kwargs)
        self.zip_file = zip_file
        self.xml_file = xml_file
        self.pdf_files = pdf_files

    def start_requests(self):
        """Spider can be run on a record XML file."""
        if self.xml_file:
            request = Request(self.xml_file)
            # if self.pdf_files:
                # request.meta["pdf_files"] = self.pdf_files
            yield request


    def get_pdf_path(self, issn, vol, issue, fpage, recid, year):
        """Get path for the correct pdf."""

        localpath = "/home/henrikv/.virtualenvs/hepcrawl/src/hepcrawl/tmp/jpcs/"
        afspath = "/afs/cern.ch/project/inspire/uploads/library/jpcs/"

        # format year for filename use:
        if year and len(year) == 4:
            year = year[2:]
            if "0" in year and "10" not in year:
                year = year[-1]
        else:
            self.logger.warning("No proper year for recid " + recid)
            return None

        # pdf_filename = "jpconf{}_{}_{}.pdf".format(year, vol, fpage)
        # pdf_filename_alt = "JPCS_{}_{}_{}.pdf".format(vol, issue, fpage)
        container_path = "{}/{}/{}/{}/".format(issn, vol, issue, fpage)


        path_to_pdfs = localpath + container_path
        if os.path.isdir(path_to_pdfs):
            for fname in os.listdir(path_to_pdfs):
                if "pdf" in fname.lower():
                    pdf_filename = fname
        else:
            self.logger.warning("No file found for recid " + recid + " (" + container_path + year + ")")
            raise ValueError
            # return None

        # NOTE: please don't rename random files manually...
        full_path = localpath + container_path + pdf_filename
        afs_full_path = afspath + container_path + pdf_filename

        if os.path.isfile(full_path):
            return afs_full_path
        else:
            self.logger.warning("No file found for recid " + recid + " (" + container_path + pdf_filename + ")")
            return None

    def add_fft_file(self, file_path, file_access, file_type):
        """Create a structured dictionary and add to 'files' item."""
        file_dict = {
            "access": file_access,
            "description": self.name.upper(),
            "url": file_path,
            "type": file_type,
        }
        return file_dict

    def parse_node(self, response, node):
        """Parse the record XML and create a HEPRecord."""
        node.remove_namespaces()
        record = HEPLoader(item=HEPRecord(), selector=node, response=response)
        recid = node.xpath("./controlfield[@tag='001']/text()").extract_first()

        # There are some strange temporary records:
        # marc500 = node.xpath("./datafield[@tag='500']/subfield[@code='a']/text()").extract_first()
        # if marc500 and "temporary" in marc500.lower():
            # self.logger.info("Temporary record, recid: " + recid)
            # return None  # Don't skip these after all

        doi = node.xpath("./datafield[@tag='024']/subfield[@code='a']/text()").extract()
        if doi and len(doi) == 1:  # HACK: there might be multiple DOIs
            doi = doi[0]
        elif doi and len(doi) == 2:
            doi = doi[1]

        if doi == "10.1088/1742-6596/349/1/02008":  # HACK
            doi = "10.1088/1742-6596/349/1/012008"  # HACK

        proceedings = node.xpath("./datafield[@tag='245']/subfield[@code='a']/text()").extract_first()
        if "proceedings" in proceedings.lower():
            self.logger.info("Recid " + recid + " is proceedings. Skipping.")
            return None

        year = node.xpath("./datafield[@tag='773']/subfield[@code='y']/text()").extract_first()
        if not year:
            pub_date = node.xpath("./datafield[@tag='260']/subfield[@code='c']/text()").extract_first()
            year = pub_date.split("-")[0]

        try:
            issn, vol, issue, fpage = doi.split("/")[1:]
        except (AttributeError, IndexError, ValueError):
            try:
                vol = node.xpath("./datafield[@tag='773']/subfield[@code='v']/text()").extract_first()
                issue = node.xpath("./datafield[@tag='773']/subfield[@code='n']/text()").extract_first()
                page_range = node.xpath("./datafield[@tag='773']/subfield[@code='c']/text()").extract_first()
                if "-" in page_range:
                    fpage = page_range.split("-")[0]
                else:
                    fpage = page_range
                fpage = filter(lambda x: x.isdigit(), fpage)

                issn = "1742-6596"
            except TypeError:
                self.logger.warning("No DOI found for recid " + recid + ". Are these proceedings?")
                return None

        pdf_path = self.get_pdf_path(issn, vol, issue, fpage, recid, year)
        if not pdf_path:
            return None

        record.add_value("recid", recid)
        file_type = "Fulltext"
        file_access = "INSPIRE-PUBLIC"
        record.add_value("additional_files",
                            self.add_fft_file(pdf_path, file_access, file_type))

        return record.load_item()
