#! /usr/bin/env python2
"""
Configuration file intended to reproduce EASY++ scheduling

Created by Alexander Goponenko
"""

#The scheduler to use.
#To list them: for s in schedulers/*_scheduler.py ; do basename -s .py $s; done
scheduler = {
	"name":'easy_cust_scheduler',
	"presorter": None,
	"postsorter": 'LAF',

	#The predictor (if needed) to use.
	#To list them: for s in predictors/predictor_*.py ; do basename -s .py $s; done
	'predictor': {
		"name":"predictor_reqtime",
		"max_cores":"auto",
		# "eta":5000,
		# "loss":"composite",
		# "rightside":'abs',
		# "rightparam":1,
		# "leftside":'square',
		# "leftparam":1,
		# "threshold":0,
		# "weight":"1+log(m*r)",
		# "quadratic":True,
		# "cubic": False,
		# "gd": "NAG",
		# "regularization":"l2",
		# "lambda":4000000000
	},

	#The corrector (if needed) to use.
	#Choose between: "+str(schedulers.common_correctors.correctors_list())
	'corrector': {"name":"reqtime"},

	}

#Force the number of available processors in the simulated parallel machine
#num_processors = 80640

#should some stats have to be computed?
stats = False

