#! /usr/bin/env python2
"""

Created by Alexander Goponenko
"""

#The scheduler to use.
#To list them: for s in schedulers/*_scheduler.py ; do basename -s .py $s; done
scheduler = {
	"name":'pure_b_f_scheduler',
	"presorter": 'SJF',

	#The predictor (if needed) to use.
	#To list them: for s in predictors/predictor_*.py ; do basename -s .py $s; done
	'predictor': {
		"name": "predictor_clairvoyant",

	},

	#The corrector (if needed) to use.
	#Choose between: "+str(schedulers.common_correctors.correctors_list())
	'corrector': {"name":"reqtime"},

	}

#Force the number of available processors in the simulated parallel machine
#num_processors = 80640

#should some stats have to be computed?
stats = False

