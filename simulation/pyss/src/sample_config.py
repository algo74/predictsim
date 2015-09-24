#! /usr/bin/env python2


#The scheduler to use.
#To list them: for s in schedulers/*_scheduler.py ; do basename -s .py $s; done
scheduler = {
	"name":'easy_backfill_scheduler',

	#The predictor (if needed) to use.
	#To list them: for s in predictors/predictor_*.py ; do basename -s .py $s; done
	'predictor': {"name":None, "option1":"bar"},

	#The corrector (if needed) to use.
	#Choose between: "+str(schedulers.common_correctors.correctors_list())
	'corrector': {"name":None, "option1":"foo"},

	"more_option":"foo"
	}

#Force the number of available processors in the simulated parallel machine
#num_processors = 80640

#should some stats have to be computed?
stats = True

