#!/usr/bin/env python
"""
This script processes backups of XO Journal metadata and produces
statistics in a specified output file for further processing. Supported output
format currently include CSV and JSON.

Usage:
  process_journal_stats.py [-o FILE] [-d DIRECTORY]

Options:
  -h --help     show this help message
  -o FILE       output file [default: ./journal_stats.csv]
  -d DIRECTORY  users directory with journal backups [default: ./users]
  --version    show version

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

    # TODO: Process all datastore backups, not just the latest one
    datastore_dir = None
    for dir in os.listdir(path):
        if datastore_name.match(dir) and os.path.islink(dir) is False:
            datastore_dir = dir
            break

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
    backup_dir = arguments['-d']
    outfile = arguments['-o']

    # TODO: process only journals selected by the user
    collected_stats = _process_journals(backup_dir)

    with open(outfile, 'w') as fp:
        outfile_ext = os.path.splitext(outfile)[1]

        if outfile_ext == '.json':
            json.dump(collected_stats, fp)
        elif outfile_ext == '.csv':
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

                # we need to convert to ASCII for csv writer
                for key, value in row.items():
                    if isinstance(value, unicode):
                        row[key] = value.encode('ascii', errors='ignore')
                csv_writer.writerow(row)

            # Output activity statistics
            activity_counts = _activity_count(collected_stats)

            with open(os.path.splitext(outfile)[0] +
                      '_activity.csv', 'w') as fp:
                for key, val in activity_counts.items():
                    fp.write(key + ', ' + str(val) + '\n')
        else:
            print "Unsupported format output file format."

if __name__ == "__main__":
    main()
