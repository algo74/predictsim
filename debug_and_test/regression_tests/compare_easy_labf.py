#!/usr/bin/env bash

import os
import sys
import difflib

os.chdir(os.path.dirname(sys.argv[0]))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'pyss')))

from pyss.run_simulator import run_simulator

def diff_files(dst1, dst2):
    with open(dst1, "r") as f1:
        lines1 = f1.readlines()
    with open(dst2, "r") as f2:
        lines2 = f2.readlines()
    return difflib.context_diff(lines1, lines2, dst1, dst2, n=0)



run_simulator('data/KTH-SP2.swf', 'configs/old_EASY-LABF_reqtime.py', 'results/old_EASY-LABF_reqtime.swf', Exception)
run_simulator('data/KTH-SP2.swf', 'configs/new_EASY-LABF_reqtime.py', 'results/new_EASY-LABF_reqtime.swf', Exception)

for line in diff_files('results/old_EASY-LABF_reqtime.swf', 'results/new_EASY-LABF_reqtime.swf'):
    print(line)