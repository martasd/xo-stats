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
  -h --help          show this help message
  -o FILE            output file [default: ./journal_stats.csv]
  -d DIRECTORY       users directory with journal backups [default: /library/users]
  -m METADATA        list of metadata to include in the output
                     [default: ['activity', 'activity_id', 'uid', 'title_set_by_user', 'title', 'tags', 'share-scope', 'keep', 'mime_type', 'mtime']]
  -s STATS           list of metadata to include with activity statistics (e.g. count share-scope keep mime_type)
  --server URL       the database server [default: http://127.0.0.1:5984]
  --deployment NAME  the deployment site
  --version          show version

"""

__author__ = "Martin Dluhos"
__email__ = "martin@gnu.org"
__version__ = "0.2"


import os
import re
import csv
import ast
import json
import filecmp
import couchdb
from couchdb.http import PreconditionFailed
from uuid import uuid4
from docopt import docopt
from datetime import datetime


def _correct_timestamp(timestamp, timedelta):
    '''
    Correct timestamp which was recorded improperly.

    Input:
      timestamp, a string timestamp of activity instance
                 (format: %Y-%m-%dT%H:%M:%S)
      timedelta, a datetime object containing the time difference to be added
                 to timestamp

    Output:
      updated_timestamp, the corrected string timestamp
    '''

    mtime_regex = re.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}(?=(.*))")

    orig_date_str = mtime_regex.search(timestamp).group(0)
    orig_date_suffix_str = mtime_regex.search(timestamp).group(1)
    orig_datetime = datetime.strptime(orig_date_str, "%Y-%m-%d")
    updated_datetime = orig_datetime + timedelta

    # convert back to a string
    updated_timestamp = updated_datetime.strftime("%Y-%m-%d") + orig_date_suffix_str
    return updated_timestamp


def _calculate_timedelta(metadata_dir_path):
    '''
    Determine the temporal difference between the date stored in the name of
    datastore backup directory and the date of the included in the metadata of
    the most recent activity from that directory.

    Input:
      metadata_dir_path, the path to the backup directory with metadata files

    Output:
      timedelta, difference between dates

    '''
    date_regex = re.compile("(?<=(datastore-))[0-9]{4}-[0-9]{2}-[0-9]{2}(?=.*)")
    mtime_regex = re.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}(?=.*)")

    # Extract date from datastore dirname
    datastore_dirname = metadata_dir_path.split(os.sep)[-2]
    datastore_date_str = date_regex.search(datastore_dirname).group(0)
    datastore_datetime = datetime.strptime(datastore_date_str, "%Y-%m-%d")

    metadata_file = re.compile(r'.*\.metadata$')
    latest_datetime = datetime.min
    for file in os.listdir(metadata_dir_path):
        if metadata_file.match(file) is not None:
            metadata_filepath = metadata_dir_path + '/' + file
            with open(metadata_filepath, "r") as fp:
                all_data = fp.read()

                try:
                    activity_metadata = json.loads(all_data)
                except ValueError:
                    pass
                else:
                    try:
                        activity_mtime = activity_metadata['mtime']
                    except KeyError:
                        pass
                    else:
                        current_date_str = mtime_regex.search(activity_mtime).group(0)
                        current_datetime = datetime.strptime(current_date_str,
                                                         "%Y-%m-%d")
                        # update the latest date so far if we found
                        # a more recent date in activity timestamp
                        if current_datetime > latest_datetime:
                            latest_datetime = current_datetime

    # Determine difference between datastore date and latest_date
    timedelta = datastore_datetime - latest_datetime

    return timedelta


def _get_metadata(metadata_in, sugar_version):
    '''
    Select relevant activity metadata based on user's preference
    and output them in a dictionary.

    Input:
      metadata_in, dictionary of all key-value metadata from backup
      sugar_version, determines metadata available

    Output:
      metadata_out, dictionary of relevant key-value metadata
                    if no 'activity' metadatum', return {}
    '''

    global metadata

    metadata_out = {}
    activity_name = metadata_in.pop('activity')

    # sanitize activity name
    if activity_name:
        activity_name = re.split(r'\.', activity_name)[-1]
        metadata_out['activity'] = re.sub(r'Activity', '', activity_name)

        for key, val in metadata_in.items():
            if key in metadata:
                metadata_out[key] = val

    return metadata_out


def _process_metadata_files(metadata_dir_path, sugar_version):
    '''
    Captures instance metadata saved in .metadata files as json (Sugar 0.82)

    Input:
      metadata_dir_path, path to store directory with *.metadata files
      sugar_version, determines if to include extra metadata

    Output:
      compiled_stats, a list of dictionaries with activity instance metadata
    '''

    compiled_stats = []
    metadata_file = re.compile(r'.*\.metadata$')

    for file in os.listdir(metadata_dir_path):
        if metadata_file.match(file) is not None:
            # Store metadata in a dictionary
            metadata_filepath = metadata_dir_path + '/' + file
            with open(metadata_filepath, "r") as fp:
                data = fp.read()

                try:
                    metadata_in = json.loads(data)
                except ValueError:
                    # TODO: Deal with invalid escape characters
                    print "Could not read metadata from %s",  metadata_filepath
                else:
                    activity_metadata = _get_metadata(metadata_in,
                                                      sugar_version)
                    if len(activity_metadata) > 0:
                        try:
                            mtime = activity_metadata['mtime']
                        except KeyError:
                            print "This instance doesn't include mtime metadatum."
                        else:
                            if mtime:
                                year = mtime.split('-')[0]
                                if int(year) < 2006:
                                    timedelta = _calculate_timedelta(metadata_dir_path)
                                    activity_metadata['mtime'] = _correct_timestamp(mtime, timedelta)

                        compiled_stats.append(activity_metadata)

    return compiled_stats


def _get_metadata_paths_82(root_dir, serial_dir):
    '''
    Determine the path to metadata directories, the paths vary for different
    versions of Sugar.

    Sugar 0.82 - 0.88: [serial]/datastore-<timestamp>/[store]

    Input:
      root_dir, the backup root directory containing XO serial number dirs
      serial_dir, directory containing Journal backups for specific XO

    Output:
      metadata_paths, the paths to all metadata directories for XO with serial
    '''

    # exclude datastore-current and datastore-latest since they are just links
    datastore_name = re.compile('^datastore-[0-9]{4}-*')
    serial_num = re.compile('^[A-Z]{2}')

    metadata_paths = []

    if serial_num.match(serial_dir):
        path_to_serial = root_dir + '/' + serial_dir
    else:
        # Not a directory with metadata
        return []

    # iterate over all datastore backups for one serial number
    for datastore_dir in os.listdir(path_to_serial):
        path = path_to_serial + '/' + datastore_dir
        if datastore_name.match(datastore_dir) and os.path.islink(path) is False:
            store_dir = path + '/store'
            if os.path.isdir(store_dir):
                path = store_dir

            # make sure to include only unique directories
            diff = True
            for orig_dir in metadata_paths:
                cmp = filecmp.dircmp(orig_dir, path)
                diff = diff and (cmp.left_only or cmp.right_only or cmp.diff_files)

            if diff:
                print "Found valid journal dir: %s" % path
                metadata_paths.append(path)

    return metadata_paths


def _get_metadata_paths_96(root_dir, serial_dir, dirnames_regex):
    '''
    Determine the path to metadata dirs for Sugar 0.96 datastore format
    0.96 backup path:
      [serial]/datastore-*/[activity_short-id]/[activity_full_id]/metadata

    Input:
      root_dir, the backup root directory containing XO serial number dirs
      serial_dir, directory containing Journal backups for specific XO
      dirnames_regex, a dictionary of regular expressions to match directory
                      names in the backup path
    Output:
      metadata_paths, the paths to all metadata directories for XO with serial
    '''
    metadata_paths = []

    if dirnames_regex['serial_num'].match(serial_dir):
        path_to_serial = root_dir + '/' + serial_dir
    else:
        # Not a directory with metadata
        return []

    # for each datastore retrieve activity metadata and store them in json
    for datastore_backup in os.listdir(path_to_serial):
        if dirnames_regex['datastore'].match(datastore_backup):
            path_to_datastore = path_to_serial + '/' + datastore_backup
            # we have found a backup dir
            for activity_short_id in os.listdir(path_to_datastore):
                if dirnames_regex['activity_id'].match(activity_short_id):
                    # assume that there is activity full id dir if
                    # activity short id dir exists
                    activity_long_id = os.listdir(path_to_datastore
                                                  + '/' + activity_short_id)[0]

                    path = path_to_datastore + '/' + activity_short_id + '/' + activity_long_id + '/metadata'
                    if os.path.isdir(path):
                        print "Found valid journal dir: %s" % path
                        metadata_paths.append(path)

    return metadata_paths


def _get_metadata_96(path_to_metadata_dir):
    '''
    Read data from files in metadata directory and store them into a dictionary
    for further processing.

    Input:
      path_to_metadata_dir, path to a dir which holds all metadata about
                            one activity instance
    Output:
      metadata_out, dictionary of key-value metadata
    '''

    # store metadata in a dictionary
    metadata_96 = {}
    for key in os.listdir(path_to_metadata_dir):
        path_to_metadata_file = path_to_metadata_dir + '/' + key
        with open(path_to_metadata_file, "r") as fp:
            value = fp.read()
        metadata_96[key] = value

    return _get_metadata(metadata_96, 0.96)


def _get_sugar_version(root_dir, dirnames_regex):
    '''
    Determine Sugar version.

    Input:
      root_dir, the backup root directory containing XO serial number dirs
      dirnames_regex, a dictionary of regular expressions to match directory
                      names in the backup path
    Ouput:
      sugar_version, a float (currently either 0.82 or 0.96)
    '''
    for serial_dir in os.listdir(root_dir):
        if dirnames_regex['serial_num'].match(serial_dir):
            path_to_serial = root_dir + '/' + serial_dir
            for datastore_dir in os.listdir(path_to_serial):
                if dirnames_regex['datastore_current'].match(datastore_dir):
                    path_to_datastore = path_to_serial + '/' + datastore_dir
                    for dir in os.listdir(path_to_datastore):
                        if dirnames_regex['activity_id'].match(dir):
                            return 0.96
                        # assume there is always a 'store' subdir in Sugar 0.82
                        elif dirnames_regex['store'].match(dir):
                            return 0.82

    # did not find a valid datastore path
    return None


def _process_journals(root_dir):
    '''
    Output stats from all specified journals in JSON

    Input:
      root_dir, the backup root directory containing XO serial number dirs

    Output:
      all_journals_stats, a list of dictionaries each containing
                          metadata for one activity instance
    '''

    global metadata
    all_journals_stats = []
    dirnames_regex = {}
    dirnames_regex['serial_num'] = re.compile('^[A-Z]{2}')
    dirnames_regex['datastore'] = re.compile('^datastore-*')
    dirnames_regex['datastore_current'] = re.compile('^datastore-current$')
    dirnames_regex['activity_id'] = re.compile('^[a-z0-9]{2}$')
    dirnames_regex['store'] = re.compile('store')

    sugar_version = _get_sugar_version(root_dir, dirnames_regex)
    if sugar_version == 0.82:
        for serial_dir in os.listdir(root_dir):
            metadata_paths = _get_metadata_paths_82(root_dir, serial_dir)

            # process each datastore backup per serial number
            for metadata_path in metadata_paths:
                current_journal_stats = _process_metadata_files(metadata_path,
                                                                sugar_version)
                all_journals_stats += current_journal_stats
    elif sugar_version == 0.96:
        # additional metadata is available in Sugar 0.96 datastore
        metadata = ast.literal_eval(metadata)
        metadata += ['buddies', 'filesize', 'creation_time', 'launch-times']

        for serial_dir in os.listdir(root_dir):
            metadata_paths = _get_metadata_paths_96(root_dir, serial_dir,
                                                    dirnames_regex)
            for metadata_path in metadata_paths:
                current_journal_stats = _get_metadata_96(metadata_path)
                if current_journal_stats:
                    all_journals_stats.append(current_journal_stats)
    else:
        print "The datastore format of this Sugar version is currently not supported."

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


def prepare_json(instance_stats, deployment):
    '''
    Prepare JSON with activity instance metadata to be inserted in the db
    '''

    global metadata

    instance_stats['deployment'] = deployment
    # activity_id is unique per activity instance
    try:
        instance_id = instance_stats.pop('activity_id')
    except KeyError:
        instance_id = uuid4().hex
        instance_stats['_id'] = instance_id

    return instance_stats, instance_id


def insert_into_db(collected_stats, db_name, server_url, deployment):
    '''
    Insert collected statistics into CouchDB one activity instance per
    document
    '''

    couch = couchdb.Server(url=server_url)

    # create a new database if it doesn't exist
    try:
        db = couch.create(db_name)
    except PreconditionFailed:
        db = couch[db_name]
        print "Importing documents into existing database %s." % db_name

    count = 0
    for instance_stats in collected_stats:
        instance_stats, instance_id = prepare_json(instance_stats, deployment)
        if instance_stats is not None:
            try:
                # update the document if it already exists in the database
                orig_doc = db.get(instance_id)
                if orig_doc is not None:
                    try:
                        instance_stats['_rev'] = orig_doc['_rev']
                    except KeyError:
                        # doesn't every doc have a _rev field?
                        pass
                db.save(instance_stats)
                count += 1
            except PreconditionFailed:
                # not clear why db.save can raise this exception, but it does
                # when the document already exists
                pass

    print "%s Journal records inserted into db: %s" % (count, db_name)


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
        deployment = arguments['--deployment']
        collected_stats = _process_journals(backup_dir)
        # put collected stats into CouchDB
        insert_into_db(collected_stats, db_name, server_url, deployment)

    elif arguments['activity']:
        metadata = arguments['-s']
        metadata = metadata.split(',') if metadata else []
        collected_stats = _process_journals(backup_dir)
        _print_activity_stats(collected_stats, outfile, format)
        print "Output file: %s" % outfile


if __name__ == "__main__":
    main()
