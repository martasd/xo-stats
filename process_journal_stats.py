#!/usr/bin/env python
"""
This script processes backups of XO Journal metadata and produces
statistics in a specified output file for further processing. Supported output
format currently include CSV and JSON.

Usage:
  process_journal_stats.py all [-m METADATA] [options]
  process_journal_stats.py dbinsert DB_NAME [-m METADATA] [options]
  process_journal_stats.py activity [-s STATS] [options]

Options:
  -h --help     show this help message
  -o FILE       output file [default: ./journal_stats.csv]
  -d DIRECTORY  users directory with journal backups [default: ./users]
  -m METADATA   list of metadata to include in the output
                [default: ['activity', 'activity_id', 'uid', 'title_set_by_user', 'title', 'tags', 'share-scope', 'keep', 'mime_type', 'mtime']]
  -s STATS      list of metadata to include with activity statistics (e.g. count share-scope keep mime_type)
  --server URL  the database server [default: http://127.0.0.1:5984]
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
import couchdb
from couchdb.http import ResourceConflict, PreconditionFailed
from uuid import uuid4
from docopt import docopt


def _get_metadata(filepath):
    '''
    Process the Journal metadata and output them in the specified format
    '''

    global metadata
    with open(filepath, "r") as fp:
        data = fp.read()

    metadata_out = {}

    try:
        metadata_in = json.loads(data)
    except ValueError:
        # TODO: Deal with invalid escape characters
        print "Could not read metadata from %s",  filepath
    else:
        # Select only relevant metadata
        activity_name = metadata_in.pop('activity')

        if activity_name:
            activity_name = re.split(r'\.', activity_name)[-1]
            metadata_out['activity'] = re.sub(r'Activity', '', activity_name)

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
            activity_metadata = _get_metadata(metadata_dir_path + '/' + file)

            if len(activity_metadata) > 0:
                compiled_stats.append(activity_metadata)

    return compiled_stats


def _get_metadata_path(root_dir, serial_dir):
    '''
    Determine the path to metadata directories, the paths vary for different
    versions of Sugar.

    Sugar 0.82 - 0.88: [serial]/datastore-[current,latest]/[store]
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


def _preprocess_record(record):
    '''
        Convert all metadata values to booleans
    '''

    record['count'] = 1

    try:
        if record.pop('mime_type'):
            record['mime_type'] = 1
        else:
            record['mime_type'] = 0
    except KeyError:
            record['mime_type'] = 0

    try:
        keep = record['keep']
    except KeyError:
        record['keep'] = 0
    else:
        record['keep'] = int(keep)

    try:
        active_scope = record.pop('share-scope')
    except KeyError:
        # when no scope present, default to private
        record['private'] = 1
    else:
        record['private'] = 1 if active_scope == 'private' else 0

    return record


def _activity_stats(collected_stats):
    '''
    Calculate the specified statistic for each activity.
    '''

    activity_stats = {}
    global metadata

    try:
        metadata.remove('share-scope')
    except ValueError:
        pass
    else:
        metadata += ['private']

    # count the number of times activities have been launched
    for record in collected_stats:

        _preprocess_record(record)

        activity = record['activity']

        if activity in activity_stats:
            # update
            for key in metadata:
                val = record[key]
                activity_stats[activity][key] += val
        else:
            # initialize
            activity_stats[activity] = {}

            # process the remaining metadata
            for key in metadata:
                val = record[key]
                activity_stats[activity][key] = val

    metadata.append('activity')
    return activity_stats, metadata


def _print_activity_stats(collected_stats, outfile, format):
    '''
    Output activity counts
    '''

    activity_stats, metadata = _activity_stats(collected_stats)

    with open(outfile, 'w') as fp:

        if format == '.json':
            json.dump(activity_stats, fp, indent=4)
        elif format == '.csv':
            csv_writer = csv.DictWriter(fp,
                                        fieldnames=metadata,
                                        quoting=csv.QUOTE_MINIMAL)

            csv_writer.writeheader()
            for activity, metadata_dict in activity_stats.items():
                csv_writer.writerow(metadata_dict)
        else:
            print "Unsupported output file format."


def insert_into_db(collected_stats, db_name, server_url):
    '''Insert collected statistics into CouchDB one activity instance per
    document
    '''

    couch = couchdb.Server(url=server_url)

    # create a new database if it doesn't exist
    try:
        db = couch.create(db_name)
    except PreconditionFailed:
        db = couch[db_name]
        print "Importing documents into existing database"

    count = 0
    for instance_stats in collected_stats:
        # activity_id is unique per activity instance
        try:
            instance_id = instance_stats.pop('activity_id')
        except KeyError:
            instance_id = uuid4().hex
        instance_stats['_id'] = instance_id
        try:
            db.save(instance_stats)
            count += 1
        except ResourceConflict:
            print "Document %s already exists in database %s." % (instance_id, db_name)
        except PreconditionFailed:
            # not clear why db.save can raise this exception, but it does when
            # the document already exists
            pass

    print "%s Journal records successfuly inserted into db: %s" % (count, db_name)


def main():
    arguments = docopt(__doc__, version=__version__)
    backup_dir = arguments['-d']
    outfile = arguments['-o']
    format = os.path.splitext(outfile)[1]
    global metadata

    if arguments['all']:
        metadata = arguments['-m']
        # TODO: process only journals selected by the user
        collected_stats = _process_journals(backup_dir)

        with open(outfile, 'w') as fp:

            if format == '.json':
                json.dump(collected_stats, fp)
                print "Output file: %s" % outfile
            elif format == '.csv':
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
                print "Output file: %s" % outfile
            else:
                print "Unsupported output file format."

    elif arguments['dbinsert']:
        metadata = arguments['-m']
        db_name = arguments['DB_NAME']
        server_url = arguments['--server']
        collected_stats = _process_journals(backup_dir)
        # put collected stats into CouchDB
        insert_into_db(collected_stats, db_name, server_url)

    elif arguments['activity']:
        metadata = arguments['-s']
        metadata = metadata.split(',') if metadata else []
        collected_stats = _process_journals(backup_dir)
        _print_activity_stats(collected_stats, outfile, format)
        print "Output file: %s" % outfile


if __name__ == "__main__":
    main()
