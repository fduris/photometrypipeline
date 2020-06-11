#!/usr/bin/env python3 

import os
import argparse

# parse arguments
parser = argparse.ArgumentParser(description='Summary of the tcs_run script')
parser.add_argument('prefix',  metavar='P', type=str, help='Prefix (e.g., filter name)')

args = parser.parse_args()
prefix = args.prefix

status = {}
failed = 0
passed = 0

for dr in os.listdir():
    if dr.endswith('.fits'):
        continue
    if os.path.isfile(dr + '/' + dr + '.tsv'):
        status[dr] = 'PASSED'
        passed += 1
    else: 
        status[dr] = 'FAILED'
        failed += 1
        
#print('Failed: ' + str(failed))
#print('Passed: ' + str(passed))

f = open('../' + prefix + '_failed.txt', 'w')
p = open('../' + prefix + '_passed.txt', 'w')

for key in status.keys():
    if status[key] == 'FAILED':
        f.write(key + '\n')
    else:
        p.write(key + '\n')
        # some passed file to extract tsv header later
        KEY = key
f.close()
p.close()
