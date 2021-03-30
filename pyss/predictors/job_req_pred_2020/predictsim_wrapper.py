'''

This is an wrapper to make a predicsim predictor from job_req_pred_2020 predictor

Created by Alexander Goponenko at 4/7/2021

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

class PredictorWrapper(object):

    def __init__(self, jrp_predictor, param_name):
        self.predictor = jrp_predictor
        self.param = param_name
        return


    def predict(self, job, current_time, list_running_jobs):
        """
        Modify the predicted_run_time of job.
        Called when a job is submitted to the system.
        the predicted runtime should be an int!
        """
        jrp_job = self._make_jrp_job(job)
        result = self.predictor.predict_requirements(jrp_job, self.param, job.actual_run_time)
        # print("Got prediction: {}".format(result))
        if result is None:
            job.predicted_run_time = job.user_estimated_run_time
        else:
            result = int(round(result))
            job.predicted_run_time = min(result, job.user_estimated_run_time)
        return


    def fit(self, job, current_time):
        """
        Add a job to the learning algorithm.
        Called when a job end.

        :return: (optionally; only needed for evaluation)
                 newly calculated (prediction, error)
        """
        jrp_job = self._make_jrp_job(job)
        value = job.actual_run_time
        res = self.predictor.update_param(jrp_job, self.param, value)
        if res is None:
            return job.user_estimated_run_time, job.user_estimated_run_time
        else:
            avg, var = res
            return (avg or job.user_estimated_run_time, var or job.user_estimated_run_time)


    def _make_jrp_job(self, job):
        jrp_job = {
            'job_name': job.executable_id,
            'user': job.user_id,
            'timelimit': job.user_estimated_run_time,
            'nodes': job.num_required_processors
        }
        return jrp_job
