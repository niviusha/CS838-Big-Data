"""
Script that processes the /proc/dev/net statistics. The statistics 
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
def read_net_stats_from_file(file_name):
  with open(file_name, "r") as f:
    lines = f.read().split('\n')

    #for line in lines:
      #print(line)
    
  eth_line = [word for word in lines[2].strip().split(' ') if len(word) > 0]
    
  bytes_received = int(eth_line[1])
  bytes_transmit = int(eth_line[9])
  packet_received = int(eth_line[2])
  packet_transmit = int(eth_line[10])

  return (bytes_received, bytes_transmit, packet_received, packet_transmit)

if __name__ == "__main__":
  if len(argv) != 2:
    print("Error: Usage " + argv[0] + " <file_name>")
    exit()

  bbr, bbt, bpr, bpt = read_net_stats_from_file(argv[1] + "b.txt")
  abr, abt, apr, apt = read_net_stats_from_file(argv[1] + "a.txt")
  
  print("Bytes received: ", abs(abr-bbr))
  print("Bytes transmitted: ", abs(abt-bbt))
  print("Packets received: ", abs(apr-bpr))
  print("Packets transmitted: ", abs(apt-bpt))
