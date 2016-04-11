#!/bin/bash
# This script will go through every file in the source file directory and
# scrape them. Better way might be to give this list to scrapy and let it 
# loop them? Whatever, this works.



FILEDIR=/home/henrikv/.virtualenvs/hepcrawl/src/hepcrawl/tests/responses/actaphys/xml/
HEPCRAWLDIR=/home/henrikv/.virtualenvs/hepcrawl/src/hepcrawl

cd $HEPCRAWLDIR
for marcfile in $FILEDIR*
    do
        echo FILE = $marcfile
        scrapy crawl actaphys -a source_file=file://$marcfile -s "JSON_OUTPUT_DIR=tmp/"
    done