
set grid x y
unset key

set yrange [-1:((num_phases+1)*num_partitions)] reverse
set ytics _REPLAY_YTICS_
set xrange [0:*]
set xlabel 'Time Since Start (ms)'
set ylabel 'ReplayDB (partitionId-phaseId)'

if (term_type eq "png") {
  set terminal pngcairo size 2000,500
  set output "phaseplot.png"
} else {
  if (term_type eq "wxt") {
    set terminal wxt persist
  } else {
    set terminal x11 persist
  }
}

line_weight = 10
ns_to_ms = 1000000

set style arrow 1 nohead filled lw (line_weight) lc rgb "#FF5555"
set style arrow 2 nohead filled lw (line_weight) lc rgb "#33FFFF"
set style arrow 3 nohead filled lw (line_weight) lc rgb "#FF33FF"
set style arrow 4 nohead filled lw (line_weight) lc rgb "#FFFF33"
set style arrow 5 nohead filled lw (line_weight) lc rgb "#5555FF"
set style arrow 6 nohead filled lw (line_weight) lc rgb "#33FF33"

set style line 1 lw 1 lt 1
set style line 2 lw 3 lt 0
set style line 3 lw 2 lt 1
set style line 4 lw 5 lt 0

filename(n) = sprintf('/tmp/partition%d.dat', n)
batch_filename(n) = sprintf('/tmp/batches%d.dat', n / batch_boundary_cols)

batch_boundary_phase_cnt = words(batch_boundary_phases)

plot \
  for [i=0:(num_partitions-1)] filename(i) \
    using (($2-start_ts)/ns_to_ms):($1+(num_phases+1)*i):(($3-$2)/ns_to_ms):(0.0):5 \
    with vectors arrowstyle variable, \
  for [i=0:(num_partitions-1)] filename(i) \
    using ((($2-start_ts)+($3-$2)/2)/ns_to_ms):($1+(num_phases+1)*i):4 with labels, \
  for [i=0:(batch_boundary_cols*batch_boundary_phase_cnt-1)] batch_filename(i) \
    using ((column((i%batch_boundary_cols)+2)-start_ts)/ns_to_ms):1 \
    with lines ls (i/batch_boundary_cols+1) lc (i%batch_boundary_cols)

