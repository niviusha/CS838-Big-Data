# The file processes all the statistics files so far.
# The first arg is the directory where the files are stored
# The second arg is the directory where this script is stored

outDir=$1

rm $2/output.txt

# For my application, it generates 5 files. Hence the value.
i=1
while [ $i -le 5 ]; do
  python $2/disk_stat_process.py ${outDir}/diskstats${i} >> $2/aggregate_output.txt
  i=$((i+1))
done

python $2/get_aggregate_diskstats.py $2/aggregate_output.txt >> $2/output.txt
rm $2/aggregate_output.txt

cat $2/output.txt
