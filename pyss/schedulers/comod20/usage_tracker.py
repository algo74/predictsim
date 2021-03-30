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

from sortedcontainers import SortedDict

class UsageTracker(object):
  min_time = -1

  def __init__(self, start_value, initial_assignments=None):
    if initial_assignments is None:
      self.list = SortedDict({self.min_time: start_value})
    else:
      self.list = SortedDict(initial_assignments)
      self.list[self.min_time] = start_value


  def add_usage(self, start, end, value):
    assert start >= 0
    assert end >= start
    if value == 0 or start == end:
      return
    if start in self.list:
      index = self.list.index(start)
      # saved value is needed in case we need to "split the interval" at the end
      saved_value = self.list[start]
      assert index > 0
      if self.list[start] + value == self.list.peekitem(index-1)[1]:
        # delete for optimization
        del self.list[start]
      else:
        self.list[start] += value
        index += 1
    else:
      # add new item to "split the interval" - no need to delete anything for optimization
      index = self.list.bisect(start)
      assert index > 0
      # save value in case we need to "split the interval" at the end
      saved_value = self.list.peekitem(index-1)[1]
      self.list[start] = saved_value + value
      index += 1
    max_index = len(self.list) - 1
    while index <= max_index:
      cur_time, cur_value = self.list.peekitem(index)
      if cur_time >= end:
        break
      # update the value if start < time < end
      saved_value = cur_value
      self.list[cur_time] += value
      index +=1
    if index > max_index or cur_time > end:
      # "split the interval" - no need to delete anything for optimization
      self.list[end] = saved_value
    else:
      assert cur_time == end
      # delete the end (for optimization) if it becomes equal to the previous value
      # (as the previous node was modified and the end wasn't)
      if cur_value == saved_value:
        del self.list[end]


  def remove_till_end(self, start, value):
    assert start >= 0
    if value == 0:
      return
    if start in self.list:
      index = self.list.index(start)
      assert index > 0
      if self.list[start] - value == self.list.peekitem(index - 1)[1]:
        # delete for optimization
        del self.list[start]
      else:
        self.list[start] -= value
        index += 1
    else:
      # "split the interval"
      index = self.list.bisect(start)
      assert index > 0
      self.list[start] = self.list.peekitem(index - 1)[1] - value
      index += 1
    max_index = len(self.list) - 1
    while index <= max_index:
      cur_time, cur_value = self.list.peekitem(index)
      self.list[cur_time] -= value
      index +=1

  def when_not_above(self, after, duration, max_value):
    assert after >= 0
    assert duration > 0
    index = self.list.bisect(after) - 1
    max_index = len(self.list) - 1
    assert index >= 0
    assert max_index >= 0
    cur_time, cur_value = self.list.peekitem(index)
    while True:
      while cur_value > max_value:
        index += 1
        if (max_index < index):
          return -1

        cur_time, cur_value = self.list.peekitem(index)
      start = max(cur_time, after)
      end = start + duration
      while(cur_value <= max_value):
        index += 1
        if (max_index < index):
          return start

        cur_time, cur_value = self.list.peekitem(index)
        if (cur_time >= end):
          return start

  def value_at(self, when):
    assert  when >= 0
    index = self.list.bisect(when) - 1
    _, cur_value = self.list.peekitem(index)
    return cur_value