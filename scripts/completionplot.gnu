
set grid x y
unset key

set xlabel 'Time Since Start (ms)'
set ylabel 'Event Time / Progress Through Log (s)'

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

line_weight = 1
point_size = 1
point_type = 5
point_interval = 1

ns_to_ms = 1000000
ms_to_s = 1000

filename(n) = sprintf('/tmp/phase%d-%d.dat', (n / num_phases), (n % num_phases))

plot \
  for [i=0:(num_partitions*num_phases-1)] filename(i) \
    using (($1-start_ts)/ns_to_ms):($2/ms_to_s):3 \
    with linespoints lc rgb variable lw (line_weight) ps (point_size) pt (point_type) pi (point_interval)
