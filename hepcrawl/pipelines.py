# -*- coding: utf-8 -*-
#
# This file is part of hepcrawl.
# Copyright (C) 2015, 2016 CERN.
#
# hepcrawl is a free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""Pipelines for saving extracted items are defined here.

Don't forget to add pipelines to the ITEM_PIPELINES setting
See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
"""

import os
import datetime
import json
import requests

from .utils import get_temporary_file


def has_publication_info(item):
    """If any publication info."""
    return item.get('pubinfo_freetext') or item.get('journal_volume') or \
        item.get('journal_title') or \
        item.get('journal_year') or \
        item.get('journal_issue') or \
        item.get('journal_fpage') or \
        item.get('journal_lpage') or \
        item.get('journal_artid') or \
        item.get('journal_doctype')


def filter_fields(item, keys):
    """Filter away keys."""
    for key in keys:
        item.pop(key, None)


class JsonWriterPipeline(object):
    """Pipeline for outputting items in JSON lines format."""

    def __init__(self, output_uri=None):
        self.output_uri = output_uri
        self.count = 0

    @classmethod
    def from_crawler(cls, crawler):
        if crawler.spider is not None:
            prefix = "{0}_".format(crawler.spider.name)
        else:
            prefix = "hepcrawl"

        output_uri = get_temporary_file(
            prefix=prefix,
            suffix=".json",
            directory=crawler.settings.get("JSON_OUTPUT_DIR")
        )
        return cls(
            output_uri=output_uri,
        )

    def open_spider(self, spider):
        self.file = open(self.output_uri, "wb")
        self.file.write("[")

    def close_spider(self, spider):
        self.file.write("]\n")
        self.file.close()
        spider.logger.info("Wrote {0} records to {1}".format(
            self.count,
            self.output_uri,
        ))

    def process_item(self, item, spider):
        line = ""
        if self.count > 0:
            line = "\n,"
        line += json.dumps(dict(item), indent=4)
        self.file.write(line)
        self.count += 1
        return item

class XmlWriterPipeline(JsonWriterPipeline):
    """This is my own custom pipeline to write MARCXML files."""
    @classmethod
    def from_crawler(cls, crawler):
        if crawler.spider is not None:
            prefix = "{0}_".format(crawler.spider.name)
        else:
            prefix = "hepcrawl"

        output_uri = get_temporary_file(
            prefix=prefix,
            suffix=".xml",
            directory=crawler.settings.get("JSON_OUTPUT_DIR")
        )
        return cls(
            output_uri=output_uri,
        )

    def open_spider(self, spider):
        self.file = open(self.output_uri, "wb")
        self.file.write("<collection>\n")

    def close_spider(self, spider):
        self.file.write("</collection>\n")
        self.file.close()
        spider.logger.info("Wrote {0} records to {1}".format(
            self.count,
            self.output_uri,
        ))

    def process_item(self, item, spider):
        recid = item.get("recid")[0]
        if "additional_files" in item:
            path_file = item.get("additional_files")[0]["url"]
            description = item.get("additional_files")[0]["type"]
            file_type = item.get("additional_files")[0]["access"]
        abstract = item.get("abstract")
        issue = item.get("journal_issue")
        marc_773 = item.get("marc_773")

        line = \
        '<record>\n'\
        '  <controlfield tag="001">%s</controlfield>\n' % (recid)

        try:
            if "additional_files" in item:
                line += \
                '  <datafield tag="FFT" ind1=" " ind2=" ">\n'\
                '    <subfield code="a">%s</subfield>\n'\
                '    <subfield code="d">%s</subfield>\n'\
                '    <subfield code="t">%s</subfield>\n'\
                '  </datafield>\n' % (path_file, description, file_type)
        except TypeError:
            import ipdb; ipdb.set_trace()

        if abstract:
            line += \
            '  <datafield tag="520" ind1=" " ind2=" ">\n'\
            '    <subfield code="a">%s</subfield>\n'\
            '  </datafield>\n' %(abstract)

        if issue:
            line += \
            '  <datafield tag="773" ind1=" " ind2=" ">\n'\
            '    <subfield code="n">%s</subfield>\n'\
            '  </datafield>\n' %(issue)

        if marc_773:
            line += \
            '  <datafield tag="773" ind1=" " ind2=" ">\n'
            for code in sorted(marc_773[0]):
                line += \
                '    <subfield code="%s">%s</subfield>\n' %(code, marc_773[0][code])
            line +=\
            '  </datafield>\n'

        line += '</record>\n'

        self.file.write(line.encode("utf8"))
        self.count += 1
        return item

class InspireAPIPushPipeline(object):
    """Push to INSPIRE API via tasks API."""

    def __init__(self):
        self.count = 0

    def process_item(self, item, spider):
        """Convert internal format to INSPIRE data model."""
        self.count += 1
        if 'related_article_doi' in item:
            item['dois'] += item.pop('related_article_doi', [])

        source = item.pop('source', spider.name)
        item['acquisition_source'] = {
            'source': source,
            # NOTE: Keeps method same as source to conform with INSPIRE
            # submissions which add `submissions` to this field.
            'method': source,
            'date': datetime.datetime.now().isoformat(),
            'submission_number': os.environ.get('SCRAPY_JOB'),
        }

        item['titles'] = [{
            'title': item.pop('title', ''),
            'subtitle': item.pop('subtitle', ''),
            'source': source,
        }]
        item['field_categories'] = [
            {"term": term, "source": "publisher", "scheme": source}
            for term in item.get('field_categories', [])
        ]
        item['abstracts'] = [{
            'value': item.pop('abstract', ''),
            'source': source,
        }]
        item['report_numbers'] = [
            {"value": rn, "source": source}
            for rn in item.get('report_numbers', [])
        ]
        item['imprints'] = [{
            'date': item.pop('date_published', ''),
        }]
        item['license'] = [{
            'license': item.pop('license', ''),
            'url': item.pop('license_url', ''),
            'material': item.pop('license_type', ''),
        }]
        item['copyright'] = [{
            'holder': item.pop('copyright_holder', ''),
            'year': item.pop('copyright_year', ''),
            'statement': item.pop('copyright_statement', ''),
            'material': item.pop('copyright_material', ''),
        }]
        if not item.get('publication_info'):
            if has_publication_info(item):
                item['publication_info'] = [{
                    'journal_title': item.pop('journal_title', ''),
                    'journal_volume': item.pop('journal_volume', ''),
                    'year': item.pop('journal_year', ''),
                    'journal_issue': item.pop('journal_issue', ''),
                    'artid': item.pop('journal_artid', ''),
                    'page_start': item.pop('journal_fpage', ''),
                    'page_end': item.pop('journal_lpage', ''),
                    'note': item.pop('journal_doctype', ''),
                    'pubinfo_freetext': item.pop('pubinfo_freetext', ''),
                }]

        # Remove any fields
        filter_fields(item, [
            'journal_title',
            'journal_volume',
            'journal_year',
            'journal_issue',
            'journal_fpage',
            'journal_lpage',
            'journal_doctype',
            'journal_artid',
            'pubinfo_freetext',
        ])
        return item

    def _prepare_payload(self, spider):
        """Return payload for push."""
        payload = dict(
            job_id=os.environ['SCRAPY_JOB'],
            results_uri=os.environ['SCRAPY_FEED_URI'],
            log_file=os.environ['SCRAPY_LOG_FILE'],
        )
        payload['errors'] = [
            (str(err['exception']), str(err['sender']))
            for err in spider.state.get('errors', [])
        ]
        return payload

    def _cleanup(self, spider):
        """Run cleanup."""
        # Cleanup errors
        if 'errors' in spider.state:
            del spider.state['errors']

    def close_spider(self, spider):
        """Post results to HTTP API."""
        task_endpoint = spider.settings['API_PIPELINE_TASK_ENDPOINT_MAPPING'].get(
            spider.name, spider.settings['API_PIPELINE_TASK_ENDPOINT_DEFAULT']
        )
        api_url = os.path.join(
            spider.settings['API_PIPELINE_URL'],
            task_endpoint
        )
        if api_url and 'SCRAPY_JOB' in os.environ:
            requests.post(api_url, json={
                "kwargs": self._prepare_payload(spider)
            })

        self._cleanup(spider)


class InspireCeleryPushPipeline(InspireAPIPushPipeline):
    """Push to INSPIRE API via Celery."""

    def __init__(self):
        from celery import Celery

        super(InspireCeleryPushPipeline, self).__init__()
        self.celery = Celery()

    def open_spider(self, spider):
        self.celery.conf.update(dict(
            BROKER_URL=spider.settings['BROKER_URL'],
            CELERY_RESULT_BACKEND=spider.settings['CELERY_RESULT_BACKEND'],
            CELERY_ACCEPT_CONTENT=spider.settings['CELERY_ACCEPT_CONTENT'],
            CELERY_TIMEZONE=spider.settings['CELERY_TIMEZONE'],
            CELERY_DISABLE_RATE_LIMITS=spider.settings['CELERY_DISABLE_RATE_LIMITS'],
            CELERY_TASK_SERIALIZER='json',
            CELERY_RESULT_SERIALIZER='json',
        ))

    def close_spider(self, spider):
        """Post results to BROKER API."""
        if 'SCRAPY_JOB' in os.environ and self.count > 0:
            task_endpoint = spider.settings['API_PIPELINE_TASK_ENDPOINT_MAPPING'].get(
                spider.name, spider.settings['API_PIPELINE_TASK_ENDPOINT_DEFAULT']
            )
            self.celery.send_task(
                task_endpoint,
                kwargs=self._prepare_payload(spider),
            )

        self._cleanup(spider)
