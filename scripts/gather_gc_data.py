#!/usr/bin/python2.7

import re
import os
import glob
from optparse import OptionParser

usage = '%prog [options] directory_to_search_in'

parser = OptionParser(usage)
parser.add_option('-p', '--num_partitions', dest='num_partitions', type='int', default=4,
                  help='num partitions: number of partitions to search for')
parser.add_option('-d', '--detector_name', dest='detector_name', type='string',
                  help='detector name: name of the detector to search for')
(options, args) = parser.parse_args()

if len(args) != 1:
    parser.error('Must supply a directory to search in')

num_partitions = options.num_partitions
detector_name = options.detector_name

gc_type_to_totals = {}

for log_dir in glob.glob(os.path.join(args[0], '{}-{}-*-*{}*-*-log'.format(num_partitions, num_partitions, detector_name))):

    match = re.search('.+?-([^-]+)-log$', log_dir)
    gc_type = match.group(1)

    if gc_type not in gc_type_to_totals:
        gc_type_to_totals[gc_type] = (0, 0, 0, 0, 0)

    for perf_file in glob.glob(os.path.join(log_dir, '*.perf')):
        with open(perf_file, 'r') as f:
            gc_stats = filter(lambda line: 'perf.gc  - Garbage' in line, f.readlines())
        if not gc_stats:
            continue
        #print perf_file
        if 'G1' in gc_stats[0]:
            (scavenge_cnt, _, scavenge_ms, _, _, _, major_cnt, _, major_ms, _) = gc_stats[0].split(' ')[15:]
        else:
            (scavenge_cnt, _, scavenge_ms, _, _, major_cnt, _, major_ms, _) = gc_stats[0].split(' ')[14:]

        totals = gc_type_to_totals[gc_type]
        gc_type_to_totals[gc_type] = (totals[0] + 1, totals[1] + int(scavenge_cnt), totals[2] + int(scavenge_ms),
                                      totals[3] + int(major_cnt), totals[4] + int(major_ms))

for gc_type, totals in gc_type_to_totals.iteritems():
    print gc_type
    print '{} (totaled from {} files)'.format(map(lambda v: v/float(totals[0]), totals[1:]), totals[0])