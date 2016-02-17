#!/usr/bin/python2.7

import os
import sys
import subprocess
from math import floor
from optparse import OptionParser
import heapq

#
# Script to generate a plot of batch start/end times
# Currently just displays the output, but gnuplot can also generate e.g. svg / eps
#

usage = """%prog [options] path_to_part0.perf path_to_part1.perf ..."""

parser = OptionParser(usage)
parser.add_option('-t', '--terminal', type='string', dest='terminal', default='x11',
                  help='Terminal type: x11, png, wxt (default x11)')
parser.add_option('-i', '--batch-boundary-interval', type='int', dest='batch_boundary_interval', default=1,
                  help='Batch boundary interval: Skip this many batches between each batch ending line (def 1)')
parser.add_option('-p', '--batch-boundary-phases', type='string', dest='batch_boundary_phases', default='0',
                  help='Batch boundary phases: A space-separated list of phases to plot batch boundaries for (def 0)')
(options, args) = parser.parse_args()

if (len(args)) < 1:
    parser.error("Must specify partition data files")

gnuplot_script = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'phaseplot.gnu')

start_ts = sys.maxint
num_partitions = 0

lowest_batch_timestamp = None
batch_interval = None

batch_boundaries = {}  # phase_num -> (part_num -> list(batch_end_ts))
batch_boundary_interval = options.batch_boundary_interval

desired_phases = map(int, options.batch_boundary_phases.split(' '))

term_type = options.terminal

for p in desired_phases:
    batch_boundaries[p] = {}

for fname in args:
    with open(fname, 'r') as f:
        lines = f.readlines()

    batch_lines = [l[:-1].split(' ')[6:] for l in lines if 'START' in l or 'END' in l]
    gc_lines = [l[:-1].split(' ')[7:] for l in lines if 'end_of_minor_GC' in l or 'end_of_major_GC' in l]
    # batch_lines = (partitionId, phaseId, batchEndTs(millis), currentTimeMillis, START|END)
    # gc_lines = (gcType, gcEvent, gcReason, gcStartMillis, gcEndMillis, currentTimeMillis)

    partition_num = int(batch_lines[0][0])
    num_partitions = max(num_partitions, partition_num + 1)
    num_phases = max(map(lambda l: int(l[1]), batch_lines)) + 1

    batch_count = {}
    for p in desired_phases:
        batch_count[p] = 0
        batch_boundaries[p][partition_num] = list()

    start_ts = min(start_ts, min(map(lambda l: int(l[3]), batch_lines)))
    if lowest_batch_timestamp is None:
        two_smallest = heapq.nsmallest(2, set(map(lambda l: int(l[2]), batch_lines)))
        lowest_batch_timestamp = two_smallest[0]
        batch_interval = two_smallest[1] - lowest_batch_timestamp

    phases = list()
    for i in xrange(0, num_phases+2):  # last 2 are minor/major GC
        phases.append((list(), list(), list()))  # START, END, batchEndTimestamp

    for line in batch_lines:
        phase_num = int(line[1])
        phases[phase_num][0 if line[4] == 'START' else 1].append(int(line[3]))
        if line[4] == 'START':
            phases[phase_num][2].append(int(line[2]))
        elif line[4] == 'END' and phase_num in desired_phases:
            if (batch_count[phase_num] % batch_boundary_interval) == 0:
                batch_boundaries[phase_num][partition_num].append(line[3])
            batch_count[phase_num] += 1

    for line in gc_lines:
        p = phases[-1 if line[1] == 'end_of_major_GC' else -2]
        duration_ms = int(line[4]) - int(line[3])
        p[0].append(int(line[5]) - duration_ms)
        p[1].append(int(line[5]))
        p[2].append(lowest_batch_timestamp - batch_interval)

    with open('/tmp/partition{}.dat'.format(partition_num), 'w') as f:
        for phase_num, phase in enumerate(phases):
            color_idx = 0
            for start, end, batch_ts in zip(phase[0], phase[1], phase[2]):
                f.write('{} {} {} {} {}\n'.format(phase_num, start, end, int((batch_ts-lowest_batch_timestamp)/batch_interval), color_idx % 6 + 1))
                color_idx += 1

for phase_num, partitioned_batches in batch_boundaries.iteritems():
    with open('/tmp/batches{}.dat'.format(phase_num), 'w') as f:
        f.write('-1 {}\n'.format(' '.join(partitioned_batches[0])))
        for part_num, batches in partitioned_batches.iteritems():
            f.write('{} {}\n'.format(part_num * (num_phases+3) + phase_num, ' '.join(batches)))
        f.write('{} {}\n'.format(num_partitions * (num_phases+3), ' '.join(partitioned_batches[num_partitions-1])))

replay_ytics = list()
for ytic in xrange(-1, num_partitions*(num_phases+3)):
    if ytic == -1 or ytic == num_partitions*(num_phases+3) or ytic % (num_phases+3) == num_phases+2:
        continue
    if ytic % (num_phases+3) >= num_phases:
        label = 'minor gc' if ytic % (num_phases+3) == num_phases else 'major gc'
    else:
        label = '{}-{}'.format(int(floor(ytic/(num_phases+3))), ytic % (num_phases+3))
    replay_ytics.append('"{}" {}'.format(label, ytic))

with open(gnuplot_script, 'r') as f:
    old = f.read()
    with open('/tmp/phaseplot.tmp.gnu', 'w') as f_new:
        f_new.write(old.replace('_REPLAY_YTICS_', '({})'.format(', '.join(replay_ytics))))
        
subprocess.call(['gnuplot', '-e',
                 'start_ts={}; num_phases={}; num_partitions={}; term_type="{}"; batch_boundary_cols={}; batch_boundary_phases="{}"'
                .format(start_ts, num_phases, num_partitions, term_type,
                        len(batch_boundaries[desired_phases[0]][0]), ' '.join(map(str, desired_phases))),
                 '/tmp/phaseplot.tmp.gnu'])

for idx in xrange(0, num_partitions):
    os.unlink('/tmp/partition{}.dat'.format(idx))
os.unlink('/tmp/phaseplot.tmp.gnu')

