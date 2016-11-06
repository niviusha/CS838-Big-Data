# Script that carries out the processing of the net and disk
# statistics for all the different parts of the application
# and aggregates the output to a file.

rm diskstats/final_output.txt
rm net_stats/final_output.txt

i=1
while [ $i -le 3 ]; do
  echo "Aggregate for Part${i}" >> diskstats/final_output.txt
  sh diskstats/run_all_diskstats.sh statistics/Part${i}/diskstats/ diskstats >> diskstats/final_output.txt
  echo "Aggregate for Part${i}" >> net_stats/final_output.txt
  sh net_stats/run_all_net_process.sh statistics/Part${i}/dev_stats/ net_stats >> net_stats/final_output.txt
  i=$((i+1))
done
