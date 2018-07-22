"""Module specifically designed to parse large xml files, like StackOverflow data dump and write to
a mongo collection in a memory-friendly manner.
"""
import sys
import xml.etree.ElementTree as ET

from pymongo import MongoClient


def parse_xml(filepath):
    """Parse XML files line-by-line and returns a generator."""
    tree = ET.iterparse(filepath)

    for event, elem in tree:
        row_data = {}
        for attr in elem.keys():
            value = elem.get(attr)
            row_data[attr] = value
        elem.clear()
        yield row_data


if __name__ == '__main__':
    filepath = sys.argv[1]  # input file to parse
    collection = sys.argv[2]  # collection name

    # initialization
    client = MongoClient()
    col = client['qa_matching'][collection]
    batch_size = 50000  # write batch to mongodb at once
    batch = []

    for i, record in enumerate(parse_xml(filepath)):
        if i % batch_size == 0 and batch:
            # write to mongodb
            sys.stdout.flush()
            sys.stdout.write("\rProcessed %i records" % i)
            col.insert_many(batch)
            batch = []
        else:
            batch.append(record)

    # insert the remaining elements
    col.insert_many(batch)
