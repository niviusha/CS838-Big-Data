"""
Script that aggregates and prints the average of the
disk statistics.

"""
from sys import argv, exit

if __name__ == "__main__":
  if len(argv) != 2:
    print("Error usage: " + argv[0] + " <file_name>")
    exit()

  with open(argv[1]) as f:
    lines = f.read().strip().split('\n')

  line_len = len(lines)
  total = line_len/2

  read_sec = 0
  write_sec = 0
  
  for (x,y) in zip(xrange(0,line_len,2), xrange(1,line_len,2)):
    read_sec += int(lines[x].split()[2])
    write_sec += int(lines[y].split()[2])

  print("Avg sectors read: " + str(read_sec/total))
  print("Avg sectors written: " + str(write_sec/total))
