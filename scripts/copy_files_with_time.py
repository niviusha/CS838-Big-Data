"""
Script that copies files from the source directory to distination directory
locally with a time limit of 5 seconds

"""
from __future__ import print_function
from sys import argv, exit
import os
import subprocess
import time

def check_source_dest_validity():
  # check if the source paths exit
  if not os.path.isdir(argv[1]):
    print("ERROR: The source path is not a valid directory")
    exit()

  # check if destination path exists else create one
  if not os.path.exists(argv[2]):
    os.makedirs(argv[2])

def get_file_list_in_directory(dir_name):
  p = subprocess.Popen(['ls',dir_name], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
  out, err = p.communicate()
  return out.strip()

def move_files_from_source_to_destination(src, dest):
  file_list = get_file_list_in_directory(src)

  src = src + ('/' if src[-1] != '/' else '')
  dest = dest + ('/' if dest[-1] != '/' else '')

  for file_name in file_list.split('\n'):
    p = subprocess.Popen(['mv', src + file_name, dest], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    out, err = p.communicate()
    print("Moved " + file_name)
    time.sleep(5)

  print('Moving completed!')

if __name__ == "__main__":
  if len(argv) != 3:
    print("ERROR: Usage %s <source_path> <destination_path>" % (argv[0]))
    exit()

  check_source_dest_validity()

  #print(get_file_list_in_directory(argv[1]))

  move_files_from_source_to_destination(argv[1], argv[2])

