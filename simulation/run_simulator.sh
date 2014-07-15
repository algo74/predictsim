#!/bin/bash
#use as run_simulator swf_folder num-processors output_folder

source ../../simulation/job_pool.sh

echoinred() {
	echo -e '\e[0;31m'"$@"
	tput sgr0
}
# afficher en bleu le premier parametre
echoinblue() {
	echo -e '\e[0;34m'"$@"
	tput sgr0
}

input_folder=$1

simulated_files_dir=$3

algos="EasySJBFScheduler EasyBackfillScheduler"

PROCS=$(($(grep -c ^processor /proc/cpuinfo)))

# initialize the job pool to allow $PROCS parallel jobs and echo commands
job_pool_init $PROCS 1

mkdir -p $simulated_files_dir

echoinred "==== RUNNING SIMULATIONS ===="

for f in $input_folder/*
do
	for sched in $algos
	do
		#./run_simulator.py --num-processors=42 --input-file=sample_input --scheduler=EasyBackfillScheduler --output-swf=dada.swf
		fshort=$(basename -s .swf "$f")
# 		echoinblue ./pyss/src/run_simulator.py --input-file="$f" --scheduler="$sched" --output-swf="$simulated_files_dir/${fshort}_${sched}.swf"
		 job_pool_run pypy ../../simulation/pyss/src/run_simulator.py --input-file="$f" --scheduler="$sched"  --num-processors=$2 --output-swf="$simulated_files_dir/${fshort}_${sched}.swf" --no-stats
	done
done

# wait until all jobs complete before continuing
job_pool_wait
# don't forget to shut down the job pool
job_pool_shutdown

# check the $job_pool_nerrors for the number of jobs that exited non-zero
echo "job_pool_nerrors: ${job_pool_nerrors}"

#echoinred "==== RUNNING ANALYSIS ===="
#echoinblue Rscript simul_analysis.R $final_csv_file $simulated_files_dir/*.swf
#Rscript simul_analysis.R $final_csv_file $simulated_files_dir/*.swf


#echoinred "==== THE END. Results are in results.csv ===="

