class Predictor(object):
	"""
	This is an "interface" to declare a predictor
	"""

	def __init__(self, options):
		print("Do it")

	def predict(self, job, current_time, list_running_jobs):
		"""
		Modify the predicted_run_time of job.
		Called when a job is submitted to the system.
		the predited runtime should be an int!
		"""
		print("Do it")

	def fit(self, job, current_time):
		"""
		Add a job to the learning algorithm.
		Called when a job end.
		"""
		print("Do it")
