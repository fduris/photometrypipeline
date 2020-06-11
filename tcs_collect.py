#!/usr/bin/env python3 

import os
import glob
import subprocess
import shlex
import sqlite3
from astropy.io import fits

# parse arguments
import argparse
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
import textwrap

parser = ArgumentParser(formatter_class=RawDescriptionHelpFormatter, description=textwrap.dedent('''\

Data collection routine for tcs_run script.

'''))
parser.add_argument('path',        type=str,                                            help='Path to the folder with processed images')
parser.add_argument('--date',      type=str,                           default='',      help="Specify night for which to collect results")
parser.add_argument('--database',  choices=['new', 'append', 'skip'],  default='skip',  help="append, new, skip (default) global database")
parser.add_argument('--verbose',   action='store_true',                                 help="Print process output (use only for debug)")

args = parser.parse_args()

print(args)

print('Started for ' + args.path)

def readSextractor(filename):
    
    commandline = 'ldactoasc ' + filename

    try:
        ldac = subprocess.check_output(shlex.split(commandline))
    except Exception as e:
        print('>> FAILED to convert: ' + str(e))
        return None
        
    # read out the output table
    ldac_ascii = [] # ldac file in ascii without header
    ldac_cols  = [] # column names for the ldac_ascii
    for line in str(ldac)[2:].split('\\n'):
        # process header
        if line.startswith('#'):
            # extract 3rd column with the name
            ldac_cols.append(line.split()[2])
            continue
        # split into cols
        params = str(line).split()
        # skip short final line
        if len(params) < 29:
            continue
        # save the data
        ldac_ascii.append(params)
        
    return {'cols': ldac_cols, 'data': ldac_ascii}
#
def readScamp(filename):

    # conenct to the database
    try:
        sqlConnection = sqlite3.connect(filename)
        sqlCursor = sqlConnection.cursor()
    except Exception as e:
        print('Failed to convert: ' + str(e))
        return None
    
    # read header
    sqlCursor.execute("SELECT * FROM data WHERE 1=0")
    scamp_cols = [d[0] for d in sqlCursor.description]
    
    # read data
    scamp_ascii = [] # scamp database in ascii
    sqlCursor.execute('SELECT * FROM data')
    for row in sqlCursor:
        scamp_ascii.append(row)

    return {'cols': scamp_cols, 'data': scamp_ascii}
#

if args.database == 'new':
    print('Opening new database files (existing will be removed)')
    target = open(args.path + '/targets.tsv', 'w')
    sex    = open(args.path + '/sextractor.tsv', 'w')
    scamp  = open(args.path + '/scamp.tsv', 'w')
elif args.database == 'append':
    print('Opening database files to append')
    target = open(args.path + '/targets.tsv', 'a')
    sex    = open(args.path + '/sextractor.tsv', 'a')
    scamp  = open(args.path + '/scamp.tsv', 'a')

# global headers (have they been written?)
header_target = False
header_sex = False
header_scamp = False

WRITE_GLOBAL = not args.database == 'skip'


# observation dates
if args.date == '':
    # all in folder
    DATES = os.listdir(args.path)

    NIGHT_FILES = False
else:
    # only given
    DATES = [args.date]
    
    print('Opening new night database files (existing will be removed)')
    
    night_target = open(args.path + '/' + args.date + '_targets.tsv', 'w')
    night_sex    = open(args.path + '/' + args.date + '_sextractor.tsv', 'w')
    night_scamp  = open(args.path + '/' + args.date + '_scamp.tsv', 'w')
    
    night_header_target = False
    night_header_sex    = False
    night_header_scamp  = False

    NIGHT_FILES = True

print("")

# loop through the dates
for DATE in DATES:

    # dates must be integers
    try:
        val = int(DATE)
    except ValueError:
        continue

    # extract year, month, day
    YEAR = DATE[0:2]
    MONTH = DATE[2:4]
    DAY = DATE[4:6]

    # observed targets for the DATE
    for TARGET in os.listdir(args.path + '/' + DATE):

        # skip rubbish
        if TARGET == 'FLAT':
            continue

        if TARGET == 'BIAS':
            continue

        if TARGET == 'DARK':
            continue

        # used filters (muscat)
        for FILTER in ['g', 'r', 'i', 'z_s']:

            # skip non-existing filters (problems with camera etc.)
            if not os.path.isdir(args.path + '/' + DATE + '/' + TARGET + '/' + FILTER):
                continue
            
            # remember the number of passed and failed files for the current DATE, TARGET, FILTER
            failed = 0
            passed = 0

            # open local files
            if args.verbose: 
                print('>> Opening local files')
                print('')

            # local databases
            sex_local = open(args.path + '/' + DATE + '/' + TARGET + '/' + FILTER + '_sex.tsv', 'w')
            scamp_local = open(args.path + '/' + DATE + '/' + TARGET + '/' + FILTER + '_scamp.tsv', 'w')
            target_local = open(args.path + '/' + DATE + '/' + TARGET + '/' + FILTER + '_target.tsv', 'w')

            # local files with filenames of passed and failed files
            failed_files = open(args.path + '/' + DATE + '/' + TARGET + '/' + FILTER + '_failed.txt', 'w')
            passed_files = open(args.path + '/' + DATE + '/' + TARGET + '/' + FILTER + '_passed.txt', 'w')

            # local headers (have they been written?)
            header_target_local = False
            header_sex_local = False
            header_scamp_local = False

            # loop through images
            for FILE in sorted(os.listdir(args.path + '/' + DATE + '/' + TARGET + '/' + FILTER)):
                
                # only go inside computation folders
                if FILE.endswith('.fits'):
                    continue

                if args.verbose:
                    print(DATE, TARGET, FILTER, FILE, sep=', ')

                # read target data (*.tsv file with results)
                if args.verbose:
                    print('>> Reading target data')
                
                # if the result tsv file exists
                if os.path.isfile(args.path + '/' + DATE + '/' + TARGET + '/' + FILTER + '/' + FILE + '/' + FILE + '.tsv'):

                    if args.verbose: 
                        print('>> Target tsv exists')
                    
                    # count the passed file
                    passed += 1
                    
                    # save the filename to file with passed files
                    if args.verbose: 
                        print('>> Writing file to passed files')
                    passed_files.write(FILE + '\n')
                    
                    # load it to memory
                    if args.verbose:
                        print('>> Reading tsv file')
                    
                    with open(args.path + '/' + DATE + '/' + TARGET + '/' + FILTER + '/' + FILE + '/' + FILE + '.tsv') as f:
                        content = f.readlines()
                    
                    # write header to global database, if requested
                    if WRITE_GLOBAL:
                        if not header_target:
                            if args.verbose:
                                print('>> Writing header to global targets')
                            target.write('\t'.join(['date', 'year', 'month', 'day', 'filter', 'target', 'julian']) + '\t' + content[0])
                            header_target = True
                    
                    # write header to night database, if requested
                    if NIGHT_FILES:
                        if not night_header_target:
                            if args.verbose:
                                print('>> Writing header to night targets')
                            night_target.write('\t'.join(['date', 'year', 'month', 'day', 'filter', 'target', 'julian']) + '\t' + content[0])
                            night_header_target = True

                    # write header to local database
                    if not header_target_local:
                        if args.verbose: print('>> Writing header to local targets')
                        target_local.write('\t'.join(['date', 'year', 'month', 'day', 'filter', 'target', 'julian']) + '\t' + content[0])
                        header_target_local = True
                    
                    # get the julian date
                    julian = str(content[1].split()[28])
                    if args.verbose:
                        print('>> Julian date = ' + julian)
                    
                    # write the data from the results file to global database, if requested
                    if WRITE_GLOBAL:
                        if args.verbose:
                            print('>> Writing target to global targets')
                        target.write('\t'.join([DATE, YEAR, MONTH, DAY, FILTER, TARGET, julian]) + '\t' + content[1])

                    # write the data from the results file to night database, if requested
                    if NIGHT_FILES:
                        if args.verbose:
                            print('>> Writing target to night targets')
                        night_target.write('\t'.join([DATE, YEAR, MONTH, DAY, FILTER, TARGET, julian]) + '\t' + content[1])

                    # write the data from the results file to local database
                    if args.verbose:
                        print('>> Writing target to local targets')
                    target_local.write('\t'.join([DATE, YEAR, MONTH, DAY, FILTER, TARGET, julian]) + '\t' + content[1])
                else: 
                    # file failed
                    if args.verbose:
                        print('>> Target tsv does not exist')
                    
                    # count the failed file
                    failed += 1

                    # save the filename to file with passed files
                    if args.verbose:
                        print('>> Writing file to failed files')
                    failed_files.write(FILE + '\n')
                
                
                
                # read the number of pp runs
                runs = sorted(glob.glob(args.path + '/' + DATE + '/' + TARGET + '/' + FILTER + '/' + FILE + '/run*'), reverse=True)
                
                if args.verbose:
                    print('>> Last pp run = run' + str(len(runs)))

                
                
                # read sextractor data
                if args.verbose:
                    print('>> Reading sextractor data')

                # if tsv version of ldac file exists 
                if os.path.isfile(runs[0] + '/' + FILE + '.ldac.tsv'):
                    
                    if args.verbose:
                        print('>> Sextractor tsv file exists, reading')
                    
                    # load it to memory
                    if args.verbose:
                        print('>> Reading tsv file')

                    with open(runs[0] + '/' + FILE + '.ldac.tsv') as f:
                        content = f.readlines()

                    # write header to global database, if requested
                    if WRITE_GLOBAL:
                        if not header_sex:
                            if args.verbose:
                                print('>> Writing header to global sextractor file')
                            sex.write('\t'.join(['date', 'year', 'month', 'day', 'file', 'filter', 'target', 'julian']) + '\t' + content[0])
                            header_sex = True
                    
                    # write header to night database, if requested
                    if NIGHT_FILES:
                        if not night_header_sex:
                            if args.verbose:
                                print('>> Writing header to night sextractor file')
                            night_sex.write('\t'.join(['date', 'year', 'month', 'day', 'file', 'filter', 'target', 'julian']) + '\t' + content[0])
                            night_header_sex = True
                    
                    # write header to local database
                    if not header_sex_local:
                        if args.verbose:
                            print('>> Writing header to local sex file')
                        sex_local.write('\t'.join(['date', 'year', 'month', 'day', 'file', 'filter', 'target', 'julian']) + '\t' + content[0])
                        header_sex_local = True
                    
                    # write the data to databases, if requested
                    if args.verbose:
                        print('>> Writing data to sextractor tables')
                    for item in content[1:]:
                        # local database
                        sex_local.write('\t'.join([DATE, YEAR, MONTH, DAY, FILE, FILTER, TARGET, julian]) + '\t' + item)
                        # global database
                        if WRITE_GLOBAL:
                            sex.write('\t'.join([DATE, YEAR, MONTH, DAY, FILE, FILTER, TARGET, julian]) + '\t' + item)
                        # night database
                        if NIGHT_FILES:
                            night_sex.write('\t'.join([DATE, YEAR, MONTH, DAY, FILE, FILTER, TARGET, julian]) + '\t' + item)
                    
                    if args.verbose:
                        print('>> OK')
                # if tsv file is not there but there is ldac file
                elif os.path.isfile(runs[0] + '/' + FILE + '.ldac'):
                    
                    if args.verbose: 
                        print('>> Sextractor tsv data not found')
                        print('>> Found ldac file, converting')
                    
                    # transform ldac to tsv
                    ldac = readSextractor(runs[0] + '/' + FILE + '.ldac')
                    
                    if not ldac == None:

                        if args.verbose:
                            print('>> Done')

                        # write ldac to tsv
                        tsv = open(runs[0] + '/' + FILE + '.ldac.tsv', 'w')

                        # header of the tsv version of the ldac file
                        tsv.write('\t'.join(ldac['cols']) + '\n')
                        
                        # write header to global database, if requested
                        if WRITE_GLOBAL:
                            if not header_sex:
                                if args.verbose:
                                    print('>> Writing header to global sextractor file')
                                sex.write('\t'.join(['date', 'year', 'month', 'day', 'file', 'filter', 'target', 'julian']) + '\t' + '\t'.join(ldac['cols']) + '\n')
                                header_sex = True
                        
                        # write header to night database, if requested
                        if NIGHT_FILES:
                            if not night_header_sex:
                                if args.verbose:
                                    print('>> Writing header to night sextractor file')
                                night_sex.write('\t'.join(['date', 'year', 'month', 'day', 'file', 'filter', 'target', 'julian']) + '\t' + '\t'.join(ldac['cols']) + '\n')
                                night_header_sex = True
                        
                        # write header to local database
                        if not header_sex_local:
                            if args.verbose:
                                print('>> Writing header to local sextractor file')
                            sex_local.write('\t'.join(['date', 'year', 'month', 'day', 'file', 'filter', 'target', 'julian']) + '\t' + '\t'.join(ldac['cols']) + '\n')
                            header_sex_local = True
                        
                        # write data to databases
                        if args.verbose:
                            print('>> Writing data to sextractor tables')

                        for item in ldac['data']:
                            # tsv version of ldac
                            tsv.write('\t'.join([str(it) for it in item]) + '\n')
                            # local database
                            sex_local.write('\t'.join([DATE, YEAR, MONTH, DAY, FILE, FILTER, TARGET, julian]) + '\t' + '\t'.join([str(it) for it in item]) + '\n')
                            # global database
                            if WRITE_GLOBAL:
                                sex.write('\t'.join([DATE, YEAR, MONTH, DAY, FILE, FILTER, TARGET, julian]) + '\t' + '\t'.join([str(it) for it in item]) + '\n')
                            # night database
                            if NIGHT_FILES:
                                night_sex.write('\t'.join([DATE, YEAR, MONTH, DAY, FILE, FILTER, TARGET, julian]) + '\t' + '\t'.join([str(it) for it in item]) + '\n')
                        
                        if args.verbose:
                            print('>> OK')
                    else:
                        if args.verbose:
                            print('>> FAILED to convert sextractor data, skipping')
                else:
                    if args.verbose:
                        print('>> Sextractor data does not exist, skipping')
    
               
                
                # read scamp data
                if args.verbose: 
                    print('>> Reading scamp data')

                # if tsv version of ldac.db file exists
                if os.path.isfile(runs[0] + '/' + FILE + '.ldac.db.tsv'):
                    
                    if args.verbose: print('>> Scamp tsv file exists, reading')
                    
                    # load it to memory
                    with open(runs[0] + '/' + FILE + '.ldac.db.tsv') as f:
                        content = f.readlines()

                    # write header to global database, if requested
                    if WRITE_GLOBAL:
                        if not header_scamp:
                            if args.verbose:
                                print('>> Writing header to global scamp file')
                            scamp.write('\t'.join(['date', 'year', 'month', 'day', 'file', 'filter', 'target', 'julian']) + '\t' + content[0])
                            header_scamp = True
                    
                    # write header to night database, if requested
                    if NIGHT_FILES:
                        if not night_header_scamp:
                            if args.verbose:
                                print('>> Writing header to night scamp file')
                            night_scamp.write('\t'.join(['date', 'year', 'month', 'day', 'file', 'filter', 'target', 'julian']) + '\t' + content[0])
                            night_header_scamp = True
                    
                    # write header to local database
                    if not header_scamp_local:
                        if args.verbose:
                            print('>> Writing header to local scamp file')
                        scamp_local.write('\t'.join(['date', 'year', 'month', 'day', 'file', 'filter', 'target', 'julian']) + '\t' + content[0])
                        header_scamp_local = True
                    
                    # write the data to databases, if requested
                    if args.verbose: 
                        print('>> Writing data to scamp tables')
                    
                    for item in content[1:]:
                        # local
                        scamp_local.write('\t'.join([DATE, YEAR, MONTH, DAY, FILE, FILTER, TARGET, julian]) + '\t' + item)
                        # global
                        if WRITE_GLOBAL:
                            scamp.write('\t'.join([DATE, YEAR, MONTH, DAY, FILE, FILTER, TARGET, julian]) + '\t' + item)
                        # night
                        if NIGHT_FILES:
                            night_scamp.write('\t'.join([DATE, YEAR, MONTH, DAY, FILE, FILTER, TARGET, julian]) + '\t' + item)
                    
                    if args.verbose:
                        print('>> OK')
                # if tsv file is not there but there is ldac file
                elif os.path.isfile(runs[0] + '/' + FILE + '.ldac.db'):
                    
                    if args.verbose:
                        print('>> Scamp tsv data not found')
                        print('>> Found ldac.db file, converting')
                    
                    # transform ldac.db to tsv
                    scampdb = readScamp(runs[0] + '/' + FILE + '.ldac.db')
                    
                    if not scampdb == None:

                        if args.verbose:
                            print('>> Done')

                        # write ldac.db to tsv
                        tsv = open(runs[0] + '/' + FILE + '.ldac.db.tsv', 'w')

                        # write header
                        tsv.write('\t'.join(scampdb['cols']) + '\n')

                        # write header to global database, if requested
                        if WRITE_GLOBAL:
                            if not header_scamp:
                                if args.verbose:
                                    print('>> Writing header to global scamp file')
                                scamp.write('\t'.join(['date', 'year', 'month', 'day', 'file', 'filter', 'target', 'julian']) + '\t' + '\t'.join(scampdb['cols']) + '\n')
                                header_scamp = True
                        
                        # write header to night database, if requested
                        if NIGHT_FILES:
                            if not night_header_scamp:
                                if args.verbose:
                                    print('>> Writing header to night scamp file')
                                night_scamp.write('\t'.join(['date', 'year', 'month', 'day', 'file', 'filter', 'target', 'julian']) + '\t' + '\t'.join(scampdb['cols']) + '\n')
                                night_header_scamp = True
                        
                        # write header to local database
                        if not header_scamp_local:
                            if args.verbose:
                                print('>> Writing header to local scamp file')
                            scamp_local.write('\t'.join(['date', 'year', 'month', 'day', 'file', 'filter', 'target', 'julian']) + '\t' + '\t'.join(scampdb['cols']) + '\n')
                            header_scamp_local = True
                        
                        # write data to databases, if requested
                        if args.verbose:
                            print('>> Writing data to scamp tables')
                        
                        for item in scampdb['data']:
                            # tsv version of ldac.db dile
                            tsv.write('\t'.join([str(it) for it in item]) + '\n')
                            # local
                            scamp_local.write('\t'.join([DATE, YEAR, MONTH, DAY, FILE, FILTER, TARGET, julian]) + '\t' + '\t'.join([str(it) for it in item]) + '\n')
                            # global
                            if WRITE_GLOBAL:
                                scamp.write('\t'.join([DATE, YEAR, MONTH, DAY, FILE, FILTER, TARGET, julian]) + '\t' + '\t'.join([str(it) for it in item]) + '\n')
                            # night
                            if NIGHT_FILES:
                                night_scamp.write('\t'.join([DATE, YEAR, MONTH, DAY, FILE, FILTER, TARGET, julian]) + '\t' + '\t'.join([str(it) for it in item]) + '\n')
                        
                        if args.verbose:
                            print('>> OK')
                    else:
                        if args.verbose: print('>> FAILED to convert scamp data, skipping')
                else:
                    if args.verbose:
                        print('>> Scamp data does not exist, skipping')
    

                
                if args.verbose:
                    print('')

            # all files in DATE, TARGET, FILTER processed
            if args.verbose:
                print('>> Closing local files')

            # close files
            failed_files.close()
            passed_files.close()
            
            target_local.close()
            sex_local.close()
            scamp_local.close()

            # print stats
            if args.verbose:
                print('>> passed ' + str(passed) + ', failed ' + str(failed))
            else:
                print(DATE + ', ' + TARGET + ', ' + FILTER + ': passed ' + str(passed) + ', failed ' + str(failed))

        # all filters in DATE, TARGET processed
    # all TARGETS in DATE processed
# all DATES processed


print('')
print('Closing files')

if WRITE_GLOBAL:
    target.close()
    sex.close()
    scamp.close()

if NIGHT_FILES:
    night_target.close()
    night_sex.close()
    night_scamp.close()
    







