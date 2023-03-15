Modified scheduling simulator
==============================



based on SC'15 submission [Improving Backfilling by using Machine Learning to predict Running Times](http://freux.fr/papers/SC15_backfilling_with_ML_runtime_predictions.pdf).
Some files from the original repository do not work properly anymore.


License
-------

Because the simulator uses `pyss` (https://code.google.com/archive/p/pyss/), which was published under GNU GPL v2 license,
the additions are published under the same license.


Changelog
---------

### Modifications

The major change in the simulator engine is related to the mechanism by which the scheduler is engaged. In the original simulator, the scheduling is initiated after each termination/submission of a job. If several jobs are terminated at a same time, the scheduler ran several times as well. Now, the scheduler never runs more than once per second. This is attained by scheduling RunSchedulerEvent, which has the lowest priority and which is not scheduled again until the already scheduled event occurs. Such behavior is more consistent with real-life behavior. 


### New schedulers features

#### Customizable EASY (`easy_cust_scheduler.py`)

EASY scheduler that can be configured to use various "initial" and "backfill" sorting orders.
The sorting orders are defined in `schedulers/sorters.py`


#### EASY-LABF (`pyss/schedulers/easy_labf_scheduler.py`)

EASY scheduler in which the job queue before backfilling is sorted largest area first (the first job is scheduled according to the original order). This scheduler is similar (although somewhat opposite) to the EASY++ (which is EASY with shortest-job-first backfilling)
> the same scheduler can be cofigured using [Customizable Easy](#customizable-easy-easy_cust_schedulerpy)


#### "Aggressive" schedulers

This set of schedulers is build from `pyss/schedulers/list_prediction_scheduler.py`:
* Largest area first: `pyss/schedulers/l_a_f_scheduler.py`
* Longest job first: `pyss/schedulers/l_j_f_scheduler.py`
* Largest resource requirement first: `pyss/schedulers/l_r_f_scheduler.py`
* Smallest area first: `pyss/schedulers/s_a_f_scheduler.py`
* Shortest job first: `pyss/schedulers/s_j_f_scheduler.py`


#### Pure backfilling (aka JustBF) with predictions (`pyss/schedulers/pure_b_f_scheduler.py`)

The resources are reserved for each job so that none of the jobs can be delayed by backfilling the lowest priority job. Note that if the predictions of job durations are inaccurate, this guarantee does not hold.

By configuring the sorting order, the scheduler can be modified to be SAF-JustBF, SJF-JustBF, LAF-JustBF, etc.

Note that the original conservative backfill (conservative_scheduler.py) is implemented according to 
Feitelson, D.G. and Weil, A.M. 1998. Utilization and predictability in scheduling the IBM SP2 with backfilling. Proceedings of the First Merged International Parallel Processing Symposium and Symposium on Parallel and Distributed Processing, 542–546.
 When a job finishes, it does not do full reschedule but "compresses it" (in __reschedule_jobs(self, current_time)_ a job may not get an earlier time due to conflict with a lower priority job scheduled in a previous attempt). In `pure_b_f_scheduler.py` all jobs are rescheduled at each run of the scheduler.


### New predictors features

#### "predict_multiplier"

Some predictors (including `pyss/predictors/predictor_clairvoyant.py` and `pyss/predictors/predictor_reqtime.py`) have `predict_multiplier` option that allow alter the prediction by multiplying it by the value of the option. This allows to double the predictions or make a "sweep" that is described in the paper.


#### Predictor_complete (`pyss/predictors/predictor_complete.py`)

"Complete" aka "Hierarchy" predictor that uses 16 templates. 

It also has an option to increase prediction by _sigma_*_sigma_factor_. Note that the behavior is different when _sigma_factor_=None and when _sigma_factor_=0 (in the latter case a category still require to have 2 or more prior observation in order to be considered).  


#### Predictor_exact

This is similar to _predictor_complete_ but instead of using 16 hierarchical templates it only has one (the most specific) template. If no record exist, the predictor falls back to the user requested timelimit right away.

It also has an option to increase prediction by _sigma_*_sigma_factor_ (unchanged from _predictor_complete_). Note that the behavior is different when _sigma_factor_=None and when _sigma_factor_=0 (in the latter case a category still require to have 2 or more prior observation in order to be considered).  


#### PredictorConditionalPercent (pyss/predictors/predictor_top_percent.py)

Survival probability (SD-x%) aka "Top-percent" predictor, described elsewhere.

`pyss/predictors/predictor_conditional_percent.py` describes a slightly modified version of this approach.



Running Experiments
-------------------

### Running a single experiment

The common script to run an experiment is `pyss/run_simulator.py`, which expects command line arguments as described in the source file documentation.


### Running multiple experiments in batch

Running multiple experiments with different configurations can be performed with `bin/run_batch.py` and with `bin/run_sweep.py`, as described in the help messages of the scripts.


### Configurations for the experiments

#### `configs/algorithms`

Set of configuration for the Example 1: algorithms.


#### `configs/predcompare`

Set of configuration for the Example 2: runtime predictions.


#### `configs/Galleguillos2018DataDrivenJobDispatching`

Configurations that were found to reproduce best the results from _Improving Backfilling by using Machine Learning to predict Running Times_ (_SC'15_)


### Data for the experiments

The cleaned and filtered swf files used for the simulations are in `data/swf_filtered`.

### Outdated code

Some files (including
`pyss/run_valexpe_client.py`,
`pyss/run_valexpe_server.py`, and
`pyss/run_valexpe.py`)
were not tested and likely are outdated.


Analysis
--------------------

Scripts used to analyze the results resides in `analysis` directory. Please see documentation inside the scripts 

### Frequent job history

The script `plot_frequent_job_history.py` (not used for the paper) is better suited to `analysis` directory but is located in `pyss` because it uses predictors from the simulator.  


Original documentation (mostly outdated because the project structure is now different)
======================

Improving Backfilling by using Machine Learning to predict Running Times.
=========================================================================

This repository is home to the scheduling simulator, machine learning tools, and experimental results from the SC'15 submission [Improving Backfilling by using Machine Learning to predict Running Times](http://freux.fr/papers/SC15_backfilling_with_ML_runtime_predictions.pdf)

Experimental Results
====================
The following files contain metrics for all the so-called heuristic triples, in the CSV format.

experiments/data/CEA-curie/sim_analysis/metrics_complete

experiments/data/KTH-SP2/sim_analysis/metrics_complete

experiments/data/CTC-SP2/sim_analysis/metrics_complete

experiments/data/SDSC-SP2/sim_analysis/metrics_complete

experiments/data/SDSC-BLUE/sim_analysis/metrics_complete

experiments/data/Metacentrum2013/sim_analysis/metrics_complete

Scheduling Simulator
====================
The Scheduling simulator used in this paper is a fork of the pyss open source scheduler. It found in the folder:

simulation/pyss/src

Machine Learning Algorithms
===========================
Implementations of the NAG algorithm for learning the model are located at:

simulation/pyss/src/predictors/

<!--Necessary R stuff:-->
<!--install.packages("argparse","ggplot2","gridExtra")-->
