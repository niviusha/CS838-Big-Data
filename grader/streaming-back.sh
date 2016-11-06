# Script to move the files in hadoop
i=1
while [ $i -le 128 ]; do
  hadoop fs -mv dataDirectory/${i}.csv split-dataset/
  echo "Moved ${i}.csv"
  i=$((i+1))
done
