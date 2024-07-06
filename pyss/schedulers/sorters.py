'''

Created by Alexander Goponenko at 11/23/2021

Sorters to be used to configure the schedulers
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

if __name__ == "__main__":
    print("Hello world!")


def sorter_laf(queue, cur_time):
  return sorted(queue, key=lambda job: -(job.num_required_processors * job.predicted_run_time))


def sorter_saf(queue, cur_time):
  return sorted(queue, key=lambda job: (job.num_required_processors * job.predicted_run_time))


def sorter_lrf(queue, cur_time):
  return sorted(queue, key=lambda job: (-job.num_required_processors,
                                        -job.predicted_run_time,))


def sorter_srf(queue, cur_time):
  return sorted(queue, key=lambda job: (job.num_required_processors,
                                        job.predicted_run_time,))


def sorter_ljf(queue, cur_time):
  return sorted(queue, key=lambda job: (-job.predicted_run_time,
                                        -job.num_required_processors,))


def sorter_sjf(queue, cur_time):
  return sorted(queue, key=lambda job: (job.predicted_run_time, job.num_required_processors,))


def sorter_srd2f(queue, cur_time):
  return sorted(queue, key=lambda job: (job.num_required_processors * job.predicted_run_time * job.predicted_run_time,
                                        job.num_required_processors * job.predicted_run_time,
                                        job.submit_time))


def sorter_none(queue, cur_time):
  return queue[:]


def sorter_wfp(queue, cur_time):
  """
  W. Tang, Z. Lan, N. Desai, and D. Buettner,
  "Fault-aware, utility-based job scheduling on Blue, Gene/P systems,"
  in 2009 IEEE International Conference on Cluster Computing and Workshops,
  Aug. 2009, pp. 1-10. doi: 10.1109/CLUSTR.2009.5289206.

  Parameters
  ----------
  queue

  Returns
  -------

  """
  return sorted(queue, key=lambda job: (job.num_required_processors * (float(job.submit_time-cur_time) / job.predicted_run_time)**3,
                                        job.submit_time,
                                        job.id))


sorters = {
  'LAF': sorter_laf,
  'LRF': sorter_lrf,
  'LJF': sorter_ljf,
  'SAF': sorter_saf,
  'SRF': sorter_srf,
  'SJF': sorter_sjf,
  'SRD2F': sorter_srd2f,
  'WFP': sorter_wfp,
  'None': sorter_none
}