# -*- coding: utf-8 -*-

"""
Script to add spaces after punctuation to abstracts inside Inspire records.
"""


import re
import os
from lxml import etree

#filename = "cpc_2qWFxR_204.xml"
#outfile = "fixed_" + filename

for xml_file in os.listdir("."):
    if xml_file.endswith(".xml"):
        outfile = "fixed_" + xml_file
        with open(xml_file, "r") as f:
            collection = etree.parse(f)

        for record in collection.xpath("record"):
            abstract = record.xpath("./datafield[@tag='520']/subfield[@code='a']")
            if len(abstract) == 1:
                abstract[0].text = re.sub(r'\.([a-zA-Z])', r'. \1', abstract[0].text)
                abstract[0].text = re.sub(r'\,([a-zA-Z])', r', \1', abstract[0].text)
            else:
                print("No abstract here?")

        #with open(outfile, 'w') as f:
            #f.write(etree.tostring(collection, encoding='UTF-8', pretty_print=True))
