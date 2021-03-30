"""

Script to convert BlueWaters torque logs to swf format
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
"""

import os 
import re
import shutil
import glob
from datetime import datetime

dir_path = os.path.dirname(os.path.realpath(__file__))
inFilename = dir_path+'/2019.log'

# Our dict of jobs.
jobs = {}
# Our list of users with script-designated natural number IDs.
users = []
# Our list of groups with script-designated natural number IDs.
groups = []
# Our list of executables with script-designated natural number IDs.
exes = []
# Our list of queues with script-designated natural number IDs.
# TODO: Unused.
queues = []

with open(inFilename, 'r') as file:
    data = file.read()
matches = re.findall(r'([0-9\/: ]*);E;([0-9]*)\.bw;user=([A-Za-z0-9-_ .]*) group=([A-Za-z0-9-_ .]*) account=([A-Za-z0-9-_ .]*) jobname=([A-Za-z0-9-_ .]*) queue=([A-Za-z0-9-_ .]*) ctime=([0-9]*) qtime=([0-9]*) etime=([0-9]*) start_count=([0-9]*) start=([0-9]*) owner=([A-Za-z0-9-_ .@]*) exec_host=([0-9\-\/+]*) login_node=([A-Za-z0-9-_ .]*) .*\n', data)

for match in matches:
    # Start by adding newly seen values to our lists.
    if match[2] not in users:
        users.append(match[2])
    if match[3] not in groups:
        groups.append(match[3])
    if match[5] not in exes:
        exes.append(match[5]) # TODO: Verify this is the best value to pick.
    # Add each of the fields in the file to the jobs list.
    job = {}
    job['jobNumber'] = int(match[1])
    job['submitTime'] = int(match[8])
    job['waitTime'] = int(match[11]) - int(match[8])
    job['runTime'] = -1                 # Fill this in later.
    job['numberOfAllocatedProcessors'] = -1 # Fill this in later.
    job['averageCpuTimeUsed'] = -1          # We don't bother with this.
    job['UsedMemory'] = -1              # We don't bother with this.
    job['RequestedNumberOfProcessors'] = -1 # Fill this in later.
    job['requestedTime'] = -1               # Fill this in later.
    job['requestedMemory'] = -1             # We don't bother with this.
    job['status'] = 1 # All E jobs (which are all we consider) have status 1.
    job['userId'] = users.index(match[2])
    job['groupId'] = groups.index(match[3])
    job['executableNumber'] = exes.index(match[5])
    job['queueNumber'] = -1             # We don't bother with this.
    job['partitionNumber'] = -1             # We don't bother with this.
    job['precedingJobNumber'] = -1          # We don't bother with this.
    job['thinkTimeFromPrecedingJob'] = -1   # We don't bother with this.
    jobs[job['jobNumber']] = job

# Update our jobs, according to our other matches.
nodesMatches = re.findall(r'.*;E;([0-9]*)\.bw;.*Resource_List.nodes=([0-9]*).*\n', data)
for match in nodesMatches:
    if int(match[0]) in jobs:
        jobs[int(match[0])]['numberOfAllocatedProcessors'] = int(match[1])

rNodeMatches = re.findall(r'.*;E;([0-9]*)\.bw;.*Resource_List.neednodes=([0-9]*).*\n', data)
for match in rNodeMatches:
    if int(match[0]) in jobs:
        jobs[int(match[0])]['RequestedNumberOfProcessors'] = int(match[1])

walltimeMatches = re.findall(r'.*;E;([0-9]*)\.bw;.*resources_used\.walltime=([0-9\/: ]*).*\n', data)
for match in walltimeMatches:
    if int(match[0]) in jobs:
        # Convert to seconds.
        h, m, s = match[1].split(':')
        seconds = int(h) * 3600 + int(m) * 60 + int(s)
        jobs[int(match[0])]['runTime'] = seconds

rWalltimeMatches = re.findall(r'.*;E;([0-9]*)\.bw;.*Resource_List.walltime=([0-9\/: ]*).*\n', data)
for match in rWalltimeMatches:
    if int(match[0]) in jobs:
        # Convert to seconds.
        h, m, s = match[1].split(':')
        seconds = int(h) * 3600 + int(m) * 60 + int(s)
        jobs[int(match[0])]['requestedTime'] = seconds

text_file = open(dir_path+'/torque.swf', 'w')
# Write the header.
text_file.write('; MaxNodes: 26864\n; Note: 22636 XE nodes + 4228 XK nodes\n; \n')
# Write each job to file, based on the swf.
for key in jobs:
    print(jobs[key])
    jobstring = '%d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d\n' % (jobs[key]['jobNumber'],
             jobs[key]['submitTime'],
             jobs[key]['waitTime'],
             jobs[key]['runTime'],
             jobs[key]['numberOfAllocatedProcessors'],
             jobs[key]['averageCpuTimeUsed'],
             jobs[key]['UsedMemory'],
             jobs[key]['RequestedNumberOfProcessors'],
             jobs[key]['requestedTime'],
             jobs[key]['requestedMemory'],
             jobs[key]['status'],
             jobs[key]['userId'],
             jobs[key]['groupId'],
             jobs[key]['executableNumber'],
             jobs[key]['queueNumber'],
             jobs[key]['partitionNumber'],
             jobs[key]['precedingJobNumber'],
             jobs[key]['thinkTimeFromPrecedingJob'])
    text_file.write(jobstring)
text_file.close()
