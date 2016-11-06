# Script to move files every 5 second between 2 
# directory in hadoop
i=1
while [ $i -le 1127 ]; do
  hadoop fs -mv split-dataset/${i}.csv dataDirectory/
  echo "Moved ${i}.csv"
  sleep 5
  i=$((i+1))
done
