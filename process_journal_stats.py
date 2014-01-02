#!/usr/bin/env python
"""
This script processes backups of XO Journal metadata and produces
statistics in a specified output file for further processing. Supported output
format currently include CSV and JSON.

Usage:
  process_journal_stats.py all [-m METADATA] [options]
  process_journal_stats.py activity [-s STATS] [options]

Options:
  -h --help     show this help message
  -o FILE       output file [default: ./journal_stats.csv]
  -d DIRECTORY  users directory with journal backups [default: ./users]
  -m METADATA   list of metadata to include in the output
                [default: ['activity', 'uid', 'title_set_by_user', 'title', 'tags', 'share-scope', 'keep', 'mime_type', 'mtime']]
  -s STATS      list of metadata to include with activity statistics
                [default: share-scope keep mime_type]
  --version     show version

"""

__author__ = "Martin Dluhos"
__email__ = "martin@gnu.org"
__version__ = "0.2"


import os
import re
import csv
import json
import filecmp
from docopt import docopt


def _get_metadata(filepath):
    '''
    Process the Journal metadata and output them in the specified format
    '''

    with open(filepath, "r") as fp:
        data = fp.read()
    metadata_in = json.loads(data)
    global metadata

    # Select only relevant metadata
    metadata_out = {}
    activity_name = metadata_in.pop('activity')

    if activity_name:
        metadata_out['activity'] = re.split(r'\.', activity_name)[-1]

        for key, val in metadata_in.items():
            if key in metadata:
                metadata_out[key] = val

    return metadata_out


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
    Determine the path to metadata directories, the paths vary for different
    versions of Sugar.

    Sugar 0.84 - 0.88: [serial]/datastore-[current,latest]/[store]
    '''

    datastore_name = re.compile('^datastore-*')
    serial_num = re.compile('^[A-Z]{2}')

    metadata_dirs = []

    if serial_num.match(serial_dir):
        path_to_serial = root_dir + '/' + serial_dir
    else:
        # Not a directory with metadata
        return []

    # iterate over all datastore backups for one serial number
    for dir in os.listdir(path_to_serial):
        path = path_to_serial + '/' + dir
        if datastore_name.match(dir) and os.path.islink(path) is False:
            store_dir = path + '/store'
            if os.path.isdir(store_dir):
                path = store_dir

            # make sure to include only unique directories
            diff = True
            for orig_dir in metadata_dirs:
                cmp = filecmp.dircmp(orig_dir, path)
                diff = diff and (cmp.left_only or cmp.right_only or cmp.diff_files)

            if diff:
                print "Found valid journal dir: %s" % path
                metadata_dirs.append(path)

    return metadata_dirs


def _process_journals(root_dir):
    '''
    Output stats from all specified journals in JSON
    '''

    all_journals_stats = []
    for serial_dir in os.listdir(root_dir):
        metadata_dirs = _get_metadata_path(root_dir, serial_dir)

        # process each datastore backup per serial number
        for metadata_dir in metadata_dirs:
            curr_journal_stats = _process_metadata_files(metadata_dir)
            all_journals_stats += curr_journal_stats

    return all_journals_stats


def _activity_stats(collected_stats):
    '''
    Calculate the number of times each activity was launched.
    '''

    activity_stats = {}
    global metadata

    try:
        metadata.remove('share-scope')
    except ValueError:
        pass
    else:
        metadata += ['public', 'private']

    # count the number of times activities have been launched
    for record in collected_stats:

        activity = record['activity']

        # pre-processing:
        # convert all metadata values to booleans
        try:
            record.pop('mime_type')
        except KeyError:
            record['mime_type'] = 0
        else:
            record['mime_type'] = 1

        try:
            keep = record['keep']
        except KeyError:
            record['keep'] = 0
        else:
            record['keep'] = int(keep)

        scope_vals = ['public', 'private']
        try:
            active_scope = record.pop('share-scope')
        except KeyError:
            record[scope_vals[0]] = 0
            record[scope_vals[1]] = 0
        else:
            scope_vals.remove(active_scope)
            inactive_scope = scope_vals.pop()
            record[active_scope] = 1
            record[inactive_scope] = 0

        if activity in activity_stats:
            # update
            for key in metadata:
                val = record[key]
                activity_stats[activity][key] += val
        else:
            # initialize
            activity_stats[activity] = {'activity': activity}

            # process the remaining metadata
            for key in metadata:
                val = record[key]
                activity_stats[activity][key] = val

    metadata.append('activity')
    return activity_stats, metadata


def _print_activity_stats(collected_stats, outfile):
    '''
    Output activity counts
    '''

    activity_stats, metadata = _activity_stats(collected_stats)

    with open(os.path.splitext(outfile)[0] +
              '_activity.csv', 'w') as fp:

        csv_writer = csv.DictWriter(fp,
                                    fieldnames=metadata,
                                    quoting=csv.QUOTE_MINIMAL)

        csv_writer.writeheader()
        for activity, metadata_dict in activity_stats.items():
            csv_writer.writerow(metadata_dict)


def main():
    arguments = docopt(__doc__, version=__version__)
    backup_dir = arguments['-d']
    outfile = arguments['-o']
    global metadata

    if arguments['all']:
        metadata = arguments['-m']
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
            else:
                print "Unsupported format output file format."

    elif arguments['activity']:
        metadata = arguments['-s'].split()
        collected_stats = _process_journals(backup_dir)
        _print_activity_stats(collected_stats, outfile)

if __name__ == "__main__":
    main()
