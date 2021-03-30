'''
This is a Python 2.7 version of table_log

@author: alex

Copyright (C) 2022 University of Central Florida

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
'''

import csv
import os


class TableLog:
  """
  A simple class that can log debug data into a csv file
  """

  def __init__(self, filename):
    self.filename = filename
    if os.path.exists(filename):
      os.remove(filename)

  def log(self, data):
    with open(self.filename, 'ab') as f:
      w = csv.writer(f)
      w.writerow(data)


class NoneLog:

  def __init__(self, filename):
    pass

  def log(self, data):
    pass
