"""
Script to copy to all the other vms

The vms were named vm-2-1, vm-2-2, etc
"""

from __future__ import print_function
import subprocess
from sys import argv, exit

def copy_file(file_name, path_name):
	p = subprocess.Popen(['scp', '-r', file_name, path_name], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
	out, err = p.communicate()
	#print(out)
	#print(err)

if __name__ == '__main__':
	if len(argv) != 3:
		# you have to specify path with the colon :
		print("ERROR: Correct usage - " + argv[0] + " <file_to_copy> <path_in_vm>")
		exit()

	vm = "vm-2-"

	for i in xrange(2,6):
		copy_file(argv[1], vm + str(i) + argv[2])
		print("Done for vm" + str(i))	
