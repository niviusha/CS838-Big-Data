"""
Script that processes the /proc/diskstat statistics. The statistics
stored  in the files before the program begin and after the program
end is given as input and then the difference is printed.

Input format:
The program expects the before and after statistics to be stored as
xb.txt and xa.txt. Hence, the user is required to just give x for the
file name and rest is taken care of.

"""
from __future__ import print_function
from sys import argv, exit

# Parse a statistics file and return the desired output
def get_statistics(file_name):
  with open(file_name, "r") as f:
    lines = f.read().strip().split("\n")

    for line in lines:
      stats = [word for word in line.strip().split(' ') if word ]
      
      if stats[2] != "vda1":
        continue
      
      sectors_read = int(stats[6])
      sectors_written = int(stats[10])

      return (sectors_read, sectors_written)

if __name__ == "__main__":
  if len(argv) != 2:
    print("Error: usage " + argv[0] + " <path_to_disk_stat> ")
    exit()
  
  b1, b2 = get_statistics(argv[1] + "b.txt")
  a1, a2 = get_statistics(argv[1] + "a.txt")
  #print(a1,a2,b1,b2)

  print("sectors read: " + str(abs(a1-b1)))
  print("sectors written: " + str(abs(a2-b2)))
