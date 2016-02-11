
set grid x y
unset key

set yrange [-1:((num_phases+1)*num_partitions)] reverse
set ytics _REPLAY_YTICS_
set xrange [0:*]
set xlabel 'Time Since Start (ms)'
set ylabel 'ReplayDB (partitionId-phaseId)'

set terminal x11 persist

line_weight = 10
ns_to_ms = 1000000

set style arrow 1 nohead filled lw (line_weight) lc rgb "#FF5555"
set style arrow 2 nohead filled lw (line_weight) lc rgb "#33FFFF"
set style arrow 3 nohead filled lw (line_weight) lc rgb "#FF33FF"
set style arrow 4 nohead filled lw (line_weight) lc rgb "#FFFF33"
set style arrow 5 nohead filled lw (line_weight) lc rgb "#5555FF"
set style arrow 6 nohead filled lw (line_weight) lc rgb "#33FF33"
filename(n) = sprintf('/tmp/partition%d.dat', n)

plot \
  for [i=0:(num_partitions-1)] filename(i) \
    using (($2-start_ts)/ns_to_ms):($1+(num_phases+1)*i):(($3-$2)/ns_to_ms):(0.0):5 \
    with vectors arrowstyle variable, \
  for [i=0:(num_partitions-1)] filename(i) \
    using ((($2-start_ts)+($3-$2)/2)/ns_to_ms):($1+(num_phases+1)*i):4 with labels # font 'Arial Bold,10'

                                                                                   # ^ currently does nothing
