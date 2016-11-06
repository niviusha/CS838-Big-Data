"""
Script that aggregates and prints the average of the 
net statistics.

"""
from sys import argv, exit

if __name__ == "__main__":
  if len(argv) != 2:
    print("Error usage: " + argv[0] + " <file_name>")
    exit()

  with open(argv[1]) as f:
    lines = f.read().strip().split('\n')

  line_len = len(lines)
  total = line_len/4

  bytes_rec = 0
  bytes_trans = 0
  pack_rec = 0
  pack_trans = 0

  for x in xrange(0, line_len, 4):
    bytes_rec = int(lines[x].split()[2])
    bytes_trans = int(lines[x+1].split()[2])
    pack_rec = int(lines[x+2].split()[2])
    pack_trans = int(lines[x+3].split()[2])

  print("Avg bytes received: " + str(bytes_rec/total))
  print("Avg bytes transmitted: " + str(bytes_trans/total))
  print("Avg packets received: " + str(pack_rec/total))
  print("Avg packets transmitted: " + str(pack_trans/total))
