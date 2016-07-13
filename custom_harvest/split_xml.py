# -*- coding: utf-8 -*-

"""
Split one big XML file of record (or whatever) nodes to smaller files
of 50 records each.

TODO: make this a bit more modular and flexible. Now it's more or less hard-
coded to work only on PRSTAB marcxml files of 50 records each.

"""

from __future__ import print_function

from lxml import etree


# Load xml records file:
num_of_records = 50
tree = etree.parse("records.xml")
root = tree.getroot()
records = root.xpath("//record")


# Why do we have this test function??
def print_to_file_test(records):
    """Split big XML to chunks of 50 Inspire records and print to files.
    NOTE: Test version with numbers only.
    """
    # This is now actually working!

    previous_num = 1
    for num, record in enumerate(records):
        if num == 0:
            # First 50 records
            filename = "records_1-50"
            previous_num = num + 1
            f = open("tmp/" + filename, "a")
            print("<collection>", file=f)
            print(filename)

        elif (num % num_of_records == 0):
            print("</collection>", file=f)
            f.close()
            filename = "records_" + str(previous_num+50) + "-" + str(num+50)
            previous_num = num + 1

            if (len(records) - previous_num < num_of_records):
                # Last less than 50 records, to have a correct filename
                filename = "records_" + str(previous_num) + "-" + str(len(records))

            f = open("tmp/" + filename, "a")
            print("<collection>", file=f)
            print(filename)


        print(num+1, file=f)

    print("</collection>", file=f)
    f.close()



def print_to_file(records, prefix):
    """Split big XML to chunks of 50 records and print to files."""
    previous_num = 1

    for num, record in enumerate(records):
        if num == 0:
            # First 50 records
            filename = prefix + "_records_1-50.xml"
            previous_num = num + 1
            f = open("tmp/" + filename, "a")
            print("<collection>", file=f)
            print(filename)

        elif (num % num_of_records == 0):
            print("</collection>", file=f)
            f.close()
            filename = prefix + "_records_" + str(previous_num+50) + "-" + str(num+50) + ".xml"
            previous_num = num + 1

            if (len(records) - previous_num < num_of_records):
                # Last less than 50 records, to have a correct filename
                filename = prefix + "_records_" + str(previous_num) + "-" + str(len(records)) + ".xml"

            f = open("tmp/" + filename, "a")
            print("<collection>", file=f)
            print(filename)

        # HACK: this will skip the two newlines at the end. Why there are newlines??
        print(etree.tostring(record, pretty_print=True)[:-2], file=f)

    print("</collection>", file=f)
    f.close()



#Finally use the function:
print_to_file(records, "EPJ")
