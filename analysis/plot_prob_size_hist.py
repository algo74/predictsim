'''

Copyright (C) 2023 University of Central Florida

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

Created by Alexander Goponenko at 9/26/23

'''
import os
import glob
import argparse
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


# names of columns in swf files
col_names = [
    'job_id',
    'Submit Time',
    'Wait Time',
    'Run Time',
    'Number of Allocated Processors',
    'Average CPU Time Used',
    'Used Memory',
    'Requested Number of Processors',
    'Requested Time',
    'Requested Memory',
    'Status',
    'User ID',
    'Group ID',
    'Executable',
    'Queue Number',
    'Partition Number',
    'Preceding Job Number',  # also is reused to store number of "underpredictions"
    'Think Time'  # also is reused to store predictions
]


def plot_psize_hist(run_filename, queue_filename, ax, bins=50):
  # read swf file
  run_df = pd.read_csv(run_filename)
  queue_df = pd.read_csv(queue_filename)
  # print(run_df.head())
  time_lim = (min(run_df['timestamp'].min(), queue_df['timestamp'].min()), max(run_df['timestamp'].max(), queue_df['timestamp'].max()))
  ax.hist([run_df['problem_size'], queue_df['problem_size']], bins=bins, label=['run', 'queue'], density=True)
  ax.legend()
  # plot probability distribution of problem size




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot histogram of scheduling problem size for file")
    parser.add_argument('input_file', help="base swf filename")
    args = parser.parse_args()
    input_base = args.input_file
    print (input_base)
    run_file = input_base + ".run.csv"
    queue_file = input_base + ".queue.csv"
    # create figure
    fig, ax = plt.subplots(1, 1, figsize=(8, 4))
    fig.subplots_adjust(bottom=0.15, left=0.15)
    plot_psize_hist(run_file, queue_file, ax)

    plt.show()

        
        

