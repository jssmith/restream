#!/usr/bin/python2.7

import os
import sys
import subprocess
from math import floor
import heapq

#
# Script to generate a plot of batch start/end times
# Currently just displays the output, but gnuplot can also generated e.g. svg / eps
# Usage: ./generate_phase_plot.py path_to_part0.perf path_to_part1.perf ...
#

if len(sys.argv) < 2:
    print 'Usage: ./generate_phase_plot.py path_to_part0.perf path_to_part1.perf ... '
    quit()

start_ts = sys.maxint
num_partitions = 0

lowest_batch_timestamp = None
batch_interval = None

for fname in sys.argv[1:]:
    with open(fname, 'r') as f:
        lines = f.readlines()

    lines = [l[:-1].split(' ')[6:] for l in lines if 'START' in l or 'END' in l]

    # lines = (partitionId, phaseId, batchEndTs(millis), currentTime(nanos), START|END)

    partition_num = int(lines[0][0])
    num_partitions = max(num_partitions, partition_num + 1)
    num_phases = max(map(lambda l: int(l[1]), lines)) + 1

    start_ts = min(start_ts, min(map(lambda l: int(l[3]), lines)))
    if lowest_batch_timestamp is None:
        two_smallest = heapq.nsmallest(2, set(map(lambda l: int(l[2]), lines)))
        print str(two_smallest)
        lowest_batch_timestamp = two_smallest[0]
        batch_interval = two_smallest[1] - lowest_batch_timestamp

    phases = list()
    for i in xrange(0, num_phases):
        phases.append((list(), list(), list()))  # START, END, batchEndTimestamp

    for line in lines:
        phases[int(line[1])][0 if line[4] == 'START' else 1].append(int(line[3]))
        if line[4] == 'START':
            phases[int(line[1])][2].append(int(line[2]))

    with open('/tmp/partition{}.dat'.format(partition_num), 'w') as f:
        for phase_num, phase in enumerate(phases):
            color_idx = 0
            for start, end, batch_ts in zip(phase[0], phase[1], phase[2]):
                f.write('{} {} {} {} {}\n'.format(phase_num, start, end, int((batch_ts-lowest_batch_timestamp)/batch_interval), color_idx % 6 + 1))
                color_idx += 1

replay_ytics = list()
for ytic in xrange(-1, num_partitions*(num_phases+1)):
    if ytic == -1 or ytic == num_partitions*(num_phases+1) or ytic % (num_phases+1) == num_phases:
        continue
    replay_ytics.append('"{}" {}'.format('{}-{}'.format(int(floor(ytic/(num_phases+1))), ytic % (num_phases+1)), ytic))

with open('phaseplot.gnu', 'r') as f:
    old = f.read()
    with open('/tmp/phaseplot.tmp.gnu', 'w') as f_new:
        f_new.write(old.replace('_REPLAY_YTICS_', '({})'.format(', '.join(replay_ytics))))
        
subprocess.call(['gnuplot', '-e', 'start_ts={}; num_phases={}; num_partitions={}'.format(start_ts, num_phases, num_partitions), '/tmp/phaseplot.tmp.gnu'])

for idx in xrange(0, num_partitions):
    os.unlink('/tmp/partition{}.dat'.format(idx))
os.unlink('/tmp/phaseplot.tmp.gnu')

