reset
###############################################################################
set term qt 1 enhanced dashed size 500,400 font "Arial,10"
set encoding iso_8859_1
set encoding utf8

f1="./gt.dat"

# 'gt.dat' figure
set xlabel "t(s)"
set ylabel "G(t) (Pa)"
set logscale xy
set format x "10^{%L}"

p f1 u 1:2 w l ls 1 lc rgb "black" lw 2.0 title "G(t)", 
###############################################################################
set term qt 2 enhanced dashed size 500,400 font "Arial,10"
set encoding iso_8859_1
set encoding utf8

f1="./gtp.dat"

# 'gtp.dat' figure
set xlabel "ω (s^{-1})"
set ylabel "G (Pa)"
set logscale xy
set format x "10^{%L}"
set format y "10^{%L}"
set key top left

p f1 u 1:2 w l ls 1 lc rgb "red" lw 2.0 title "G'(ω)",   f1 u 1:3 w l ls 1 lc rgb "blue" lw 2.0 title "G''(ω)"
