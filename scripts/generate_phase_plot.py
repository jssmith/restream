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
parser.add_option('-x', '--xrange-max', type='int', dest='xrange_max', default=-1,
                  help='xrange maximum: Define the upper limit for the x-range of the plot, in ms. Negative for automatic scaling (def -1)')
parser.add_option('-m', '--xrange-min', type='int', dest='xrange_min', default=0,
                  help='xrange minimum: Define the lower limit for the x-range of the plot, in ms (def 0)')
parser.add_option('-w', '--output-width', type='int', dest='output_width', default=2000,
                  help='output width: Width of the output (if using png terminal type) (def 2000)')
parser.add_option('-y', '--output-height', type='int', dest='output_height', default=800,
                  help='output height: Height of the output (if using png terminal type) (def 800)')
parser.add_option('-o', '--output-filename', type='string', dest='output_filename', default='phaseplot',
                  help='output filename: Name of file to save to (if using png terminal type) (def phaseplot)')
(options, args) = parser.parse_args()

if (len(args)) < 1:
    parser.error("Must specify partition data files")

gnuplot_script = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'phaseplot.gnu')

start_ts = sys.maxint
num_partitions = 0

lowest_batch_timestamp = None
highest_batch_timestamp = None
min_size_for_label = None
batch_interval = None
no_label_batch_num = None

batch_boundaries = {}  # phase_num -> (part_num -> list(batch_end_ts))
batch_boundary_interval = options.batch_boundary_interval

desired_phases = map(int, options.batch_boundary_phases.split(' '))
xrange_max = '*' if options.xrange_max < 0 else options.xrange_max
xrange_min = options.xrange_min

term_type = options.terminal

for p in desired_phases:
    batch_boundaries[p] = {}

for fname in args:
    with open(fname, 'r') as f:
        lines = f.readlines()

    batch_lines = [l[:-1].split(' ')[6:] for l in lines if 'replaydb.perf.batchtiming' in l and ('START' in l or 'END' in l)]
    gc_lines = [l[:-1].split(' ')[7:] for l in lines if 'end_of_minor_GC' in l or 'end_of_major_GC' in l]
    netty_lines = [l[:-1].split(' ')[10:] for l in lines if 'END_HANDLER' in l]
    # batch_lines = (partitionId, phaseId, batchEndTs(millis), currentTimeMillis, START|END)
    # gc_lines = (gcType, gcEvent, gcReason, gcStartMillis, gcEndMillis, currentTimeMillis)
    # netty_lines = ( handleTimeStart, handleTimeFinish )

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
        no_label_batch_num = lowest_batch_timestamp - batch_interval
        real_timestamps = map(lambda l: int(l[3]), batch_lines)
        min_size_for_label = (max(real_timestamps) - min(real_timestamps)) / 1000

    phases = list()
    for i in xrange(0, num_phases+3):  # last 3 are minor/major GC, netty IO
        phases.append((list(), list(), list()))  # START, END, batchEndTimestamp

    for line in batch_lines:
        phase_num = int(line[1])
        phases[phase_num][0 if line[4] == 'START' else 1].append(int(line[3]))
        if line[4] == 'END':
            if (phases[phase_num][1][-1] - phases[phase_num][0][-1]) < min_size_for_label:
                phases[phase_num][2].append(no_label_batch_num)
            else:
                phases[phase_num][2].append(int(line[2]))
            if phase_num in desired_phases:
                if (batch_count[phase_num] % batch_boundary_interval) == 0:
                    batch_boundaries[phase_num][partition_num].append(line[3])
                batch_count[phase_num] += 1

    for line in gc_lines:
        p = phases[-2 if line[1] == 'end_of_major_GC' else -3]
        duration_ms = int(line[4]) - int(line[3])
        p[0].append(int(line[5]) - duration_ms)
        p[1].append(int(line[5]))
        p[2].append(no_label_batch_num)

    for line in netty_lines:
        phases[-1][0].append(int(line[0]))
        phases[-1][1].append(int(line[1]))
        phases[-1][2].append(no_label_batch_num)

    with open('/tmp/partition{}.dat'.format(partition_num), 'w') as f:
        for phase_num, phase in enumerate(phases):
            color_idx = 0
            for start, end, batch_ts in zip(phase[0], phase[1], phase[2]):
                color = 12 if phase_num == num_phases + 2 else color_idx % 6 + 1
                f.write('{} {} {} {} {}\n'.format(phase_num, start, end, int((batch_ts-lowest_batch_timestamp)/batch_interval), color))
                color_idx += 1

for phase_num, partitioned_batches in batch_boundaries.iteritems():
    with open('/tmp/batches{}.dat'.format(phase_num), 'w') as f:
        f.write('-1 {}\n'.format(' '.join(partitioned_batches[0])))
        for part_num, batches in partitioned_batches.iteritems():
            f.write('{} {}\n'.format(part_num * (num_phases+4) + phase_num, ' '.join(batches)))
        f.write('{} {}\n'.format(num_partitions * (num_phases+4), ' '.join(partitioned_batches[num_partitions-1])))

replay_ytics = list()
for ytic in xrange(-1, num_partitions*(num_phases+4)):
    if ytic == -1 or ytic == num_partitions*(num_phases+4) or ytic % (num_phases+4) == num_phases+3:
        continue
    if ytic % (num_phases+4) == num_phases:
        label = 'minor gc'
    elif ytic % (num_phases+4) == num_phases+1:
        label = 'major gc'
    elif ytic % (num_phases+4) == num_phases+2:
        label = 'netty'
    else:
        label = '{}-{}'.format(int(floor(ytic/(num_phases+4))), ytic % (num_phases+4))
    replay_ytics.append('"{}" {}'.format(label, ytic))

with open(gnuplot_script, 'r') as f:
    old = f.read()
    with open('/tmp/phaseplot.tmp.gnu', 'w') as f_new:
        f_new.write(old.replace('_REPLAY_YTICS_', '({})'.format(', '.join(replay_ytics))))
        
subprocess.call(['gnuplot', '-e',
                 'start_ts={}; num_phases={}; num_partitions={}; term_type="{}"; xrange_min={}; xrange_max="{}"; '
                 'output_width={}; output_height={}; output_filename="{}"; batch_boundary_cols={}; batch_boundary_phases="{}"'
                .format(start_ts, num_phases, num_partitions, term_type, xrange_min, xrange_max,
                        options.output_width, options.output_height, options.output_filename,
                        len(batch_boundaries[desired_phases[0]][0]), ' '.join(map(str, desired_phases))),
                 '/tmp/phaseplot.tmp.gnu'])

for idx in xrange(0, num_partitions):
    os.unlink('/tmp/partition{}.dat'.format(idx))
os.unlink('/tmp/phaseplot.tmp.gnu')

