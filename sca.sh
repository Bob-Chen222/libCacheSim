for i in 1 2 4 8 16 32; do taskset -c 0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70\ 
_build_new/bin/cachesim ../dummy.txt oracleGeneral fifo 1000000 --num-thread=$i --ignore-obj-size=1 >> read.txt; done 

for i in 1 2 4 8 16 32; do taskset -c 0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70\ 
_build_par/bin/cachesim ../dummy.txt oracleGeneral fifo 1000000 --num-thread=$i --ignore-obj-size=1 >> no_read.txt; done 