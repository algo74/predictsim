'''

Plot histogram of problem sizes for swf files given by a glob wildcard in a single plot
See Fig. 7 in DEBS2024 paper

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
import matplotlib
import matplotlib.ticker as mtick
import pandas as pd
import numpy as np
import editdistance

from calculate_problem_size import calculate_problem_size

workloads = ['KTH-SP2', 'CTC-SP2',   'SDSC-SP2',  'SDSC-BLUE',  'CEA-CURIE',  'BW201911']

markers = ["D", "v", "s", "*", "^", "o", "<", ">", "p", "P", "X"]
# colors
colors = ['b', 'g', 'm', 'r', 'tab:brown', 'tab:gray']

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


def best_str_match_idx(str, str_list):
  # get base names of files
  str_list = [os.path.basename(s) for s in str_list]
  # cut strings until "___" and convert to upper case
  str_list = [s[:s.find("___")].upper() for s in str_list]
  # calculate distance to each string in the list
  dists = [editdistance.eval(str, s) for s in str_list]
  # return the string with the smallest distance
  return np.argmin(dists)


if __name__ == "__main__":
  matplotlib.rc('text', usetex=True)
  matplotlib.rc(
      'text.latex',
      # libertine and newtxmath are for ACM style
      preamble='\\usepackage{bm} \\usepackage{xfrac} \\usepackage{relsize} \\usepackage{libertine} \\usepackage[libertine]{newtxmath}'
  )
  # set format for x-axis labels (logarithmic scale)
  mtick._mathdefault = lambda x: '\\mathdefault{%s}'%x
  plt.rcParams['figure.dpi'] = 300
  parser = argparse.ArgumentParser(description="Plot histogram of problem sizes for swf files given by a glob wildcard")
  parser.add_argument('input_glob', help="wildcard of swf files to process")
  args = parser.parse_args()
  input_files = glob.glob(args.input_glob)
  input_files.sort()
  print (input_files)
  n_files = len(input_files)
  # find best number of columns and rows for subplots
  fig, ax = plt.subplots(1, 1, figsize=(8, 3.7))
  # bin_range = (1, 2000)
  # bin_range = (1, 100)
  # bins = np.logspace(np.log10(bin_range[0]), np.log10(bin_range[1]), 50)
  max_bin = 100
  queue_dfs = []
  for n in range(n_files):
    # if files don't exist, run the script to create them
    if not os.path.exists(input_files[n] + '.run.csv') or not os.path.exists(input_files[n] + '.queue.csv'):
      calculate_problem_size(input_files[n])
    # plot histogram for this file
    queue_dfs.append(pd.read_csv(input_files[n] + '.queue.csv'))
    # adjust bins to the range of problem sizes (for cumulative histogram)
    max_bin = max(queue_dfs[n]['problem_size'].max() + 1, max_bin)
  bins = range(-1, max_bin + 1)
  cur_count = 1
  # print header
  print("#Jobs", end='')
  while cur_count <= max_bin:
    print("\t{}".format(cur_count), end='')
    cur_count *= 2
  print()
  hists = []
  for s in workloads:
    n = best_str_match_idx(s, input_files)
    # print("Plotting histogram for {} from {}".format(s, input_files[n]))
    hist, edges = np.histogram(queue_dfs[n]['problem_size'], bins=bins, density=True)
    # make cumulative histogram
    hist = np.cumsum(hist)
    # print table row
    cur_count = 1
    print(s, end='')
    while cur_count <= max_bin:
      print("\t{:.0f}".format(hist[cur_count]*100), end='')
      cur_count *= 2
    print()
    # plot cumulative histogram
    ax.scatter(
        edges[1:],
        hist,
        label="$\\textit{{{}}}$".format(s),
        marker=markers[n],
        color=colors[n],
        rasterized=True)
  ax.set_xscale('log')
  ax.set_xlim(0.901, max_bin - 1)
  ax.set_xlabel("Number of jobs in wait queue", fontsize='16')
  ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
  ax.set_ylabel("Cumulative fraction", fontsize='16')
  ax.spines['top'].set_visible(False)
  ax.spines['right'].set_visible(False)
  ax.tick_params(axis='both', labelsize=16)
  ax.legend()
  leg = ax.legend(
      fontsize='16',
      framealpha=1,
      loc='lower right',
      borderaxespad=0,
      bbox_to_anchor=(1.0, 0.03))
  leg.get_frame().set_edgecolor('black')
  # Change the labels of x-axis to use same font as in the y-axis
  ax.set_xticklabels(
      [str(int(x)) for x in ax.get_xticks()],
      fontsize=16)
  # make tight layout
  plt.tight_layout()
  # plt.show()
  plt.savefig("problem_size_hist_combined.pdf")
  plt.savefig("problem_size_hist_combined.eps")
