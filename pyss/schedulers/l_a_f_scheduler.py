'''

Created by Alexander Goponenko at 11/15/2021

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

from .list_prediction_scheduler import ListPredictionScheduler

class LAFScheduler(ListPredictionScheduler):
  """
  Largest Resource First (then longest first)
  """

  def make_sorted_queue(self, queue):
    """
    This method sorts queue Largest area first.
    Parameters
    ----------
    queue

    Returns
    -------
    sorted queue
    """
    return sorted(queue, key=lambda job: (-job.num_required_processors * job.predicted_run_time, -job.predicted_run_time, job.submit_time))