''' 
Created by Alexander Goponenko at 10/19/2021

This script corrects inconsistent jobs and removes unusable jobs.
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

import sys


# names of columns in swf files
col_names = [
    'job_id',
    'Submit Time',
    'Wait Time',
    'Run Time',
    'Number of Allocated Processors',
    'Average CPU Time Used',
    'Used Memory',
    'Requested Number of Processors',
    'Requested Time',
    'Requested Memory',
    'Status',
    'User ID',
    'Group ID',
    'Executable',
    'Queue Number',
    'Partition Number',
    'Preceding Job Number',  # also is reused to store number of "underpredictions"
    'Think Time'  # also is reused to store predictions
]

col_idx = {}
for i in range(len(col_names)):
    col_idx[col_names[i]] = i


def filter(input, output):
    with open(input, 'r') as src:
        with open(output, 'w') as out:
            while True:
                line = src.readline()
                if not line:
                    break
                if line[0] == ';':
                    # header
                    out.write(line)
                else:
                    values = line.split()
                    if int(values[col_idx["Run Time"]]) <=0:
                        # remove jobs with 0 or no run time
                        continue
                    if int(values[col_idx["Run Time"]]) > int(values[col_idx['Requested Time']]):
                        # adjust run time if over time limit
                        values[col_idx["Run Time"]] = values[col_idx['Requested Time']]
                    if int(values[col_idx['Requested Number of Processors']]) <= 0 \
                            and int(values[col_idx['Number of Allocated Processors']]) <= 0:
                        # remove jobs with no resources used
                        continue
                    # correct jobs if resources requested/used were missing
                    if int(values[col_idx['Requested Number of Processors']]) <= 0:
                        values[col_idx['Requested Number of Processors']] = values[col_idx['Number of Allocated Processors']]
                    if int(values[col_idx['Number of Allocated Processors']]) <= 0:
                        values[col_idx['Number of Allocated Processors']] = values[col_idx['Requested Number of Processors']]
                    out.write(" ".join(values) + "\n")

if __name__ == "__main__":
    filter(sys.argv[1], sys.argv[2])
