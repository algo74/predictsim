'''
In-memory recorder.
  Stores the records in RAM instead of a database.
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

Created on Apr 21, 2020

@author: alex
'''

import logging
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)



class Recorder(object):

  def __init__(self, path="/LDMS_data/SOS/results"):
    """ This recorder keeps all the records in a dict in RAM

    :param path: not used for this recorder
    """

    self.db = {}

    # Note: this in-memory recorder does not use schemas.
    # It is for the reference only.
    schema_template = [
      {"name": "variety_id", "type": "char_array"},
      {"name": "parameter", "type": "char_array"},
      {"name": "avg", "type": "double"},
      {"name": "var", "type": "double"},
      {"name": "w_count", "type": "double"},
      {"name": "w_sum", "type": "double"},
      {"name": "vid_par", "type": "join", "join_attrs": ["variety_id", "parameter"], "index": {}}
    ]


  def getRecord(self, variety_id, param):
    """ Retrieves record for given tag (variety_id) and parameter name

    :param variety_id: tag of the job group
    :param param: parameter (i.e. the name of the predicted value)
    :return: the record as a tuple (avg, var, w_count, w_sum)
             or None if such record not found
    :rtype: tuple or None
    """
    key = (variety_id, param)
    record = self.db.get(key, None)
    if not record:
      return None
    else:
     return record

  def saveRecord(self, variety_id, param, avg, var, w_count, w_sum):
    """Stores the record

    :param variety_id: tag of the job group
    :param param: parameter (i.e. the name of the predicted value)
    :param avg: average value of the parameter
    :param var: variance for the parameter
    :param w_count: "count of weights" for calculating the average value
    :param w_sum:  "sum of weighted values" for calculating the average value
    :return: nothing
    """
    logger.debug("saving variety_id: \"%s\", parameter: \"%s\", avg: %f, var: %f, count: %f, sum: %f",
                 variety_id, param, avg, var, w_count, w_sum)
    key = (variety_id, param)
    self.db[key] = (avg, var, w_count, w_sum)
