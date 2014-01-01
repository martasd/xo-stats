xo-stats
========

## Description

The project's objective is to determine how XOs are used in Nepalese
classrooms. This will be done by collecting data from the XO Journal backups on
the schoolserver, analyzing and visualizing the captured data, and formulating
recommendations for improving the program based on the analysis.

`process_journal_stats.py` is a script which takes a directory with XO Journal
backups as input, extracts the Journal metadata and outputs them in a CSV or
JSON file as output. Currently, CSV and JSON were chosen since these are the
formats that most data visualization and analysis tools expect as input.

_Note_: Journal backups can be in a variety of formats depending on the version
of Sugar. The script currently supports backup format present in Sugar versions
0.82 - 0.88 since the laptops distributed in Nepal are XO-1s running Sugar
0.82. Support for later versions of Sugar is going to be added in the next
version of the script.


## Usage

The script currently supports two ways to output statistical data. To produce
all statistical data from the Journal, one row per Journal record, call:

    process_journal_stats.py all
	
To extract statistical data about the use of activities on the system, use:

    process_journal_stats.py activity

Here is the full documentation for using the script, including all its options.

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


## Further Analysis and Visualization

I am currently evaluating the most appropriate tools to use for analysis and
useful visualization of the data extracted by the script.

## Acknowledgments

This script is based on
[olpc-journal-processor](https://github.com/Leotis/olpc_journal_processor)
script Leotis' Buchanan and
[get-journal-stats](http://gitorious.paraguayeduca.org/get-journal-stats) by
Raul Gutierrez Segales.
