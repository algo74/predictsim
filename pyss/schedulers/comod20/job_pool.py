"""
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

from sortedcontainers import SortedSet

class JobPool(object):

  def __init__(self):
    self.pending = []
    self.running = set()
    self.pending_by_nodes = SortedSet(key=lambda job: job.num_required_processors)

  def add_pending(self, job):
    self.pending.append(job)
    self.pending_by_nodes.add(job)

  def move_to_running(self, job):
    self.pending.remove(job)
    self.pending_by_nodes.remove(job)
    self.running.add(job)

  def remove_from_running(self, job):
    self.running.remove(job)

  def get_pending_jobs_list(self):
    return self.pending[:]

  def get_running_jobs(self):
    return self.running

  def min_nodes_in_pending(self):
    return self.pending_by_nodes[0].num_required_processors