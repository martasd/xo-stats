#!/usr/bin/env python
"""
This script processes backups of XO Journal metadata and produces
statistics in JSON for further processing.

Usage:
  process_journal_stats.py NUMBER-OF-JOURNALS [-o FILE]

Options:
  -h --help  show this help message
  -o FILE    specify output file
  --version  show version

"""

__author__ = "Martin Dluhos"
__email__ = "martin@gnu.org"
__version__ = "0.1"


import os
import re
import csv
import json
from docopt import docopt


def _get_metadata(filepath):
    '''
    Process the Journal metadata and output them in the specified format
    '''

    with open(filepath, "r") as fp:
        data = fp.read()
    return json.loads(data)


def _process_metadata_files(metadata_dir_path):
    '''
    Outputs stats from one Journal
    '''

    compiled_stats = []
    metadata_file = re.compile(r'.*\.metadata$')

    for file in os.listdir(metadata_dir_path):
        if metadata_file.match(file) is not None:
            print "Processing file: %s" % file
            activity_metadata = _get_metadata(metadata_dir_path + '/' + file)

            if len(activity_metadata) > 0:
                compiled_stats.append(activity_metadata)

    return compiled_stats


def _get_metadata_path(root_dir, serial_dir):
    '''
    Determine the path to metadata directory, which varies in different
    versions of Sugar.

    Sugar 0.84 - 0.88: [serial]/datastore-[current,latest]/[store]
    '''

    datastore_name = re.compile('^datastore-*')
    serial_num = re.compile('^[A-Z]{2}')

    if serial_num.match(serial_dir):
        path = root_dir + '/' + serial_dir
    else:
        # Not a directory with metadata
        return None

    datastore_dir = None
    for dir in os.listdir(path):
        if datastore_name.match(dir):
            datastore_dir = dir

    if datastore_dir:
        path = path + '/' + datastore_dir
    else:
        # No datastore dir
        return None

    store_dir = path + '/store'
    if os.path.isdir(store_dir):
        path = store_dir

    print "Found valid journal dir: %s" % path
    return path


def _process_journals(root_dir):
    '''
    Output stats from all specified journals
    '''

    serial_dirs = os.listdir(root_dir)
    serial_dir = serial_dirs.pop()
    metadata_dir_path = _get_metadata_path(root_dir, serial_dir)
    if metadata_dir_path:
            all_journals_stats = _process_metadata_files(metadata_dir_path)

    for serial_dir in serial_dirs:
        metadata_dir_path = _get_metadata_path(root_dir, serial_dir)
        if metadata_dir_path:
            all_journals_stats.append(
                _process_metadata_files(metadata_dir_path))

    return all_journals_stats


def main():
    arguments = docopt(__doc__, version=__version__)

    current_dir = os.getcwd()
    # TODO: process only journals selected by the user
    collected_stats = _process_journals(current_dir + '/users')

    outfile = arguments['-o']
    if outfile:
        with open(current_dir + '/' + outfile, 'w') as fp:
            if re.search(r'\.json', outfile):
                json.dump(collected_stats, fp)
            elif re.search(r'\.csv', outfile):
                keys = {}
                for i in collected_stats:
                    for k in i.keys():
                        keys[k] = 1
                csv_writer = csv.DictWriter(fp,
                                            fieldnames=keys.keys(),
                                            quoting=csv.QUOTE_MINIMAL)
                csv_writer.writeheader()
                for row in collected_stats:
                    csv_writer.writerow(row)
            else:
                print "Unsupported format output file format."

if __name__ == "__main__":
    main()
