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
from netrc import netrc
from tempfile import mkdtemp

import mechanize
from scrapy import Request, FormRequest
from scrapy.spiders import XMLFeedSpider
from ..extractors.nlm import NLM

from ..items import HEPRecord
from ..loaders import HEPLoader
from ..utils import get_temporary_file


class IOPSpider(XMLFeedSpider, NLM):
    """IOPSpider crawler.

    This spider should first be able to harvest files from IOP STACKS
    (http://stacks.iop.org/Member/). Then it should scrape through the files
    and get the things we want.

    XML files are in NLM PubMed format:
    http://www.ncbi.nlm.nih.gov/books/NBK3828/#publisherhelp.XML_Tag_Descriptions
    Examples:
    http://www.ncbi.nlm.nih.gov/books/NBK3828/#publisherhelp.Example_of_a_Standard_XML

    1. Fetch gzipped data packages from STACKS

    2. Scrape the XML files inside.

    3. Return valid JSON records.

    You can also call this spider directly on gzip package or an XML file. If called
    without arguments, it will attempt to fetch files from STACKS.

    Example usage:
    .. code-block:: console

        scrapy crawl IOP
        scrapy crawl IOP -a xml_file=file://`pwd`/tests/responses/iop/xml/test_standard.xml
        scrapy crawl IOP -a zip_file=file://`pwd`/tests/responses/iop/packages/test.tar.gz -a xml_file=file://`pwd`/tests/responses/iop/xml/test_standard.xml
        scrapy crawl IOP -a pdf_files=`pwd`/tests/responses/iop/pdf/ -a xml_file=file://`pwd`/tests/responses/iop/xml/test_standard.xml

    for JSON output, add -s "JSON_OUTPUT_DIR=tmp/"
    for logging, add -s "LOG_FILE=iop.log"

    Happy crawling!
    """

    name = 'mechanize'
    start_urls = []
    iterator = 'xml'
    itertag = 'Article'
    http_user = ''  # to enable HTTP basic authentication
    http_pass = ''
    stacks_url = "http://stacks.iop.org/Member/extract"

    OPEN_ACCESS_JOURNALS = {
        "J. Phys.: Conf. Ser.",
        # FIXME: add more
    }

    issns = [
        "1751-8121", "0954-3899", "1742-6596", "1367-2630", "1538-3881",
        "0004-637X", "2041-8205", "0067-0049", "1674-1056", "1674-1137",
        "0256-307X", "0264-9381", "0253-6102", "0143-0807", "0295-5075",
        "1475-7516", "1748-0221", "0957-0233", "0951-7715", "1402-4896",
        "1402-4896-topical", "0031-9120", "1063-7869", "0034-4885", "1674-4527"
    ]

    def __init__(self, zip_file=None, xml_file=None, pdf_files=None,
                 http_netrc=None, journal=None, *args, **kwargs):
        """Construct IOP spider."""
        super(IOPSpider, self).__init__(*args, **kwargs)
        self.zip_file = zip_file
        self.xml_file = xml_file
        self.pdf_files = pdf_files
        self.http_netrc = "tmp/stacks_netrc"
        self.http_user, _, self.http_pass = self.get_authentications()
        self.journal = journal  # if you want to take only one specific journal
        if journal:
            self.issns = [journal]

    def get_authentications(self):
        """Get username and password form netrc file."""
        auths = netrc(self.http_netrc)
        return auths.authenticators(self.stacks_url)


    def start_requests(self):
        """Spider can be run on a record XML file. In addition, a gzipped package
        containing PDF files or the path to the pdf files can be given.

        If no arguments are given, it should try to get the package from STACKS.
        """
        if self.xml_file:
            if not self.pdf_files and self.zip_file:
                self.pdf_files = self.handle_pdf_package(self.zip_file)
            elif not self.pdf_files:
                self.pdf_files = self.fetch_newest_pdf_packages_from_stacks()
            request = Request(self.xml_file)
            if self.pdf_files:
                request.meta["pdf_files"] = self.pdf_files
            yield request
        else:
            yield Request(self.stacks_url, callback=self.scrape_for_available_issues)

    def fetch_newest_pdf_packages_from_stacks(self):
        """Get the newest PDF package from STACKS."""
        # FIXME: make sure this works
        package = requests.get(
            "http://stacks.iop.org/Member/lload.tar.gz",
            auth=('user', 'pass')
        )
        target_file = get_temporary_file(
            prefix="IOP_", suffix="_pdf", dir="/tmp"
        )
        with open(target_file, "w") as tf:
            tf.write(package.content)

        return self.handle_pdf_package(target_file)

    def handle_pdf_package(self, zip_file):
        """Extract all the pdf files in the gzip package."""
        filename = os.path.basename(zip_file).rstrip(".tar.gz")
        # TMP dir to extract zip packages:
        # FIXME: should the files be permanently stored somewhere
        target_folder = mkdtemp(
            prefix="iop" + filename + "_", dir="/tmp/")
        zip_filepath = zip_file.replace("file://", "")
        self.untar_files(zip_filepath, target_folder)

        return target_folder

    def scrape_for_available_issues(self, response):
        """Scrape the STACKS page for missing issues."""
        node = response.selector

        existing_issues = self.get_existing_issues()
        #existing_issues = {
            #"1367-2630": [
                #u'18/1', u'18/2', u'18/3', u'18/4', u'18/5', u'18/6', u'18/7',
                #u'18/8', u'18/9', u'18/10']
        #}  # NOTE: This is a test dictionary.
        # issns = existing_issues.keys()  # this is also a test

        available_issues = {}  # All the issues availabe on STACKS
        for issn in self.issns:
            journal = node.xpath('.//select[@name="' + issn +'/from"]')
            issues = journal.xpath('.//option/text()').extract()
            available_issues[issn] = issues

        missing_issues = {}  # All the issues we don't have but could have
        for journal in available_issues:
            if journal in existing_issues:
                difference = list(set(available_issues[journal]) - set(existing_issues[journal]))
                missing_issues[journal] = difference
            else:
                # Take the whole journal if we don't have it
                missing_issues[journal] = available_issues[journal]

        # Note that we could have existing issues which are not available any more

        #missing_issues = {"1674-4527": ["16/1"]}  # Getting AmbiguityError
        #missing_issues = {"1402-4896": ["91/1"]}  # Getting AmbiguityError
        # now we know what issues we are missing.
        for journal_issn in missing_issues:
            for issue in missing_issues[journal_issn]:
                import time; time.sleep(10)  # NOTE: be careful or be banned
                browser = mechanize.Browser()
                browser.add_password(self.stacks_url, self.http_user, self.http_pass, "STACKS")
                browser.open(self.stacks_url)
                browser.select_form(nr=0)  # There is only one form, but it's multipart
                form = browser.form

                # Try setting the values:
                #try:
                    #browser.form.set_value_by_label([issue], name=journal_issn + "/from")
                    #browser.form.set_value_by_label([issue], name=journal_issn + "/to")
                    ## Hey you are supposed to select, not set the values!
                #except Exception as err:
                    ## We are getting AmbiguityError here, error message: "16/1"
                    ## but does it matter at all??
                    ## This error is very strange
                    #import ipdb; ipdb.set_trace()
                # Try selecting the values:
                journal_from = form.find_control(name=journal_issn + "/from")
                journal_to = form.find_control(name=journal_issn + "/to")
                journal_from.get_items(label=issue)[0].selected = True
                journal_to.get_items(label=issue)[0].selected = True


                # These below work
                checkboxes = browser.form.find_control(name="issn")
                desired_journal = checkboxes.get_items(name=journal_issn)
                desired_journal[0].selected = True
                # To check whether something is selected or not:
                # The asterisk tells if something's selected
                # for control in form.controls:
                    # print(control)
                resp = browser.submit()  # Click the button


                metadata_directory = "tmp/IOP/{}/{}".format(journal_issn, issue)
                # FIXME: something wacky happening  here, the dir structure is not right?
                if not os.path.exists(metadata_directory):
                    os.makedirs(metadata_directory)
                path_to_new_file = metadata_directory + "/extract.pubmed"
                with open(path_to_new_file, "w") as f:
                    f.write(resp.get_data())

                abs_path = "file://{}".format(os.path.abspath(path_to_new_file))

                yield Request(abs_path)


    def get_existing_issues(self):
        """Get the issues we already have based on dir structure.

        e.g. tmp/IOP/issn/vol/issue
        """
        existing_issns = os.listdir("tmp/IOP/")
        existing_issues = {}

        for issn in existing_issns:
            issues = []
            for vol in os.listdir("tmp/IOP/" + issn):
                for issue in os.listdir("tmp/IOP/" + issn + "/" + vol):
                    issues.append(vol + "/" + issue)
            existing_issues[issn] = issues

        #FIXME: this is not working?
        #import ipdb; ipdb.set_trace()
        return existing_issues


    @staticmethod
    def untar_files(zip_filepath, target_folder):
        """Unpack a tar.gz package while flattening the dir structure.

        Return list of pdf paths.
        """
        pdf_files = []
        with tarfile.open(zip_filepath, "r:gz") as tar:
            for filename in tar.getmembers():
                if filename.path.endswith(".pdf"):
                    filename.name = os.path.basename(filename.name)
                    absolute_path = os.path.join(target_folder, filename.path)
                    if not os.path.exists(absolute_path):
                        tar.extract(filename, path=target_folder)
                    pdf_files.append(absolute_path)

        return pdf_files

    def get_pdf_path(self, vol, issue, fpage):
        """Get path for the correct pdf."""
        pattern = "{}_{}_{}.pdf".format(vol, issue, fpage)
        for pdf_path in os.listdir(self.pdf_files):
            if pattern in pdf_path:
                return os.path.join(self.pdf_files, pdf_path)

    def add_fft_file(self, file_path, file_access, file_type):
        """Create a structured dictionary and add to 'files' item."""
        file_dict = {
            "access": file_access,
            "description": self.name,
            "url": file_path,
            "type": file_type,
        }
        return file_dict

    def parse_node(self, response, node):
        """Parse the record XML and create a HEPRecord."""
        record = HEPLoader(item=HEPRecord(), selector=node, response=response)

        #import ipdb; ipdb.set_trace()

        pub_status = self.get_pub_status(node)
        if pub_status in {"aheadofprint", "received"}:
            return None

        fpage, lpage, page_nr = self.get_page_numbers(node)
        volume = node.xpath(".//Journal/Volume/text()").extract_first()
        issue = node.xpath(".//Journal/Issue/text()").extract_first()

        record.add_value("journal_fpage", fpage)
        record.add_value("journal_lpage", lpage)
        record.add_value("page_nr", page_nr)
        record.add_xpath('abstract', ".//Abstract")
        record.add_xpath("title", ".//ArticleTitle")
        record.add_value('authors', self.get_authors(node))
        journal_title = node.xpath(
            ".//Journal/JournalTitle/text()").extract_first()
        record.add_value("journal_title", journal_title)
        record.add_value("journal_issue", issue)
        record.add_value("journal_volume", volume)
        record.add_xpath("journal_issn", ".//Journal/Issn/text()")
        record.add_value("dois", self.get_dois(node))

        journal_year = node.xpath(".//Journal/PubDate/Year/text()").extract()
        if journal_year:
            record.add_value("journal_year", int(journal_year[0]))

        record.add_xpath("language", ".//Language/text()")
        record.add_value('date_published', self.get_date_published(node))
        record.add_xpath('copyright_statement',
                         "./CopyrightInformation/text()")
        record.add_xpath('copyright_holder', "//Journal/PublisherName/text()")
        record.add_xpath(
            'free_keywords', "ObjectList/Object[@Type='keyword']/Param[@Name='value']/text()")

        record.add_xpath("related_article_doi", "//Replaces[@IdType='doi']/text()")
        doctype = self.get_doctype(node)  # FIXME: should these be mapped?
        record.add_value("journal_doctype", doctype)
        record.add_value('collections', self.get_collections(doctype))

        # xml_file_path = response.url  # FIXME: Do we want to store the XML?
        # record.add_value("additional_files",
                         #self.add_fft_file(xml_file_path, "INSPIRE-HIDDEN", "Fulltext"))
        if self.pdf_files:
            pdf_file_path = self.get_pdf_path(volume, issue, fpage)
            if pdf_file_path:
                if doctype and "erratum" in doctype.lower():
                    file_type = "Erratum"
                else:
                    file_type = "Fulltext"
                if journal_title in self.OPEN_ACCESS_JOURNALS:
                    file_access = "INSPIRE-PUBLIC"  # FIXME: right?
                else:
                    file_access = "INSPIRE-HIDDEN"
                record.add_value("additional_files",
                                 self.add_fft_file(pdf_file_path, file_access, file_type))

        return record.load_item()
