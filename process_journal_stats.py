#!/usr/bin/env python
"""
This script processes backups of XO Journal metadata and produces
statistics in a specified output file for further processing. Supported output
format currently include CSV and JSON.

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
    metadata_json = json.loads(data)

    # TODO: Select only relevant metadata
    activity_name = metadata_json['activity']
    metadata_json['activity'] = re.split(r'\.', activity_name)[-1]
    return metadata_json


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
    Output stats from all specified journals in JSON
    '''

    all_journals_stats = []
    for serial_dir in os.listdir(root_dir):
        metadata_dir_path = _get_metadata_path(root_dir, serial_dir)
        if metadata_dir_path:
            curr_journal_stats = _process_metadata_files(metadata_dir_path)
            all_journals_stats += curr_journal_stats

    return all_journals_stats


def _activity_count(collected_stats):

    activity_counts = {}

    # count the number of times activities have been launched
    for record in collected_stats:
        activity_name = record['activity']

        if activity_name == '':
            continue

        if activity_name in activity_counts:
            activity_counts[activity_name] += 1
        else:
            activity_counts[activity_name] = 1

    return activity_counts


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
                # Collect all field names for header
                keys = {}
                for instance_stats in collected_stats:
                    for k in instance_stats.keys():
                        keys[k] = 1

                # Write one activity instance per row
                csv_writer = csv.DictWriter(fp,
                                            fieldnames=keys.keys(),
                                            quoting=csv.QUOTE_MINIMAL)
                csv_writer.writeheader()
                for row in collected_stats:
                    csv_writer.writerow(row)

                activity_counts = _activity_count(collected_stats)

                with open(current_dir + '/activity' + outfile, 'w') as fp:
                    for key, val in activity_counts.items():
                        fp.write(key + ', ' + str(val) + '\n')

            else:
                print "Unsupported format output file format."

if __name__ == "__main__":
    main()
