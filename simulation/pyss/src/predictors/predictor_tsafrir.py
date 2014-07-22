from predictor import Predictor

class PredictorTsafrir(Predictor):
	"""
	estimate_runtime = (prev_runtime + prev_prev_runtime)/2
	"""

        def __init__(self,max_procs=None, max_runtime=None, loss="squared_loss", eta=0.01, regularization="l1",alpha=1,beta=0,verbose=True):
		self.user_run_time_prev = {}
		self.user_run_time_last = {}

	def predict(self, job, current_time):
		"""
		Modify the estimate_runtime of job.
		Called when a job is submitted to the system.
		"""
		if not self.user_run_time_last.has_key(job.user_id):
			self.user_run_time_prev[job.user_id] = None
			self.user_run_time_last[job.user_id] = None

		if self.user_run_time_prev[job.user_id] != None:
			average =  int((self.user_run_time_last[job.user_id] + self.user_run_time_prev[job.user_id])/ 2)
			job.predicted_run_time = min (job.user_estimated_run_time, average)

	def fit(self, job, current_time):
		"""
		Add a job to the learning algorithm.
		Called when a job end.
		"""
		assert self.user_run_time_last.has_key(job.user_id) == True
		assert self.user_run_time_prev.has_key(job.user_id) == True
		self.user_run_time_prev[job.user_id] = self.user_run_time_last[job.user_id]
		self.user_run_time_last[job.user_id] = job.actual_run_time