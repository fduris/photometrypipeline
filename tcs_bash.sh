#!/bin/bash

function usage
{
    echo "Usage: pp_bash -p STR [-d INT] [-D {new,append,skip}] [-h]"
    echo ""
    echo "  -p / --path        path to folder with dates"
    echo "  -d / --date        process specific date"
    echo "  -D / --database    new, append, skip (default) global database"
    echo "  -h / --help        print this help"
    echo ""
}

function thread
{
    local FOLDER=$1
    local TARGET=$2
    local FILTER=$3
    local MODE=$4
    local BACKUP_FILTER=$5
    local LOG_FILE=$6

    #echo $FOLDER
    #echo $TARGET
    #echo $FILTER
    #echo $MODE
    #echo $BACKUP_FILTER
    #echo $LOG_FILE
    #exit


    cd $FOLDER

    # data reduction
    for FILE in *.fits; do

        case "$MODE" in
                init)
                    #echo "python3 ~/photometrypipeline/tcs_run.py $FILE --target $TARGET --iplots --dplots --zscale &> ${LOG_FILE}"
                    python3 ~/photometrypipeline/tcs_run.py $FILE --target $TARGET --iplots --dplots --zscale \
                    &>> ${LOG_FILE}
                    ;;
                time)
                    python3 ~/photometrypipeline/tcs_run.py $FILE --target $TARGET --iplots --dplots --zscale --backup time \
                    &>> ${LOG_FILE}
                    ;;
                filter)
                    python3 ~/photometrypipeline/tcs_run.py $FILE --target $TARGET --iplots --dplots --zscale --backup filter --use-filter ${BACKUP_FILTER}
                    &>> ${LOG_FILE}
                    ;;
                sextime)
                    python3 ~/photometrypipeline/tcs_run.py $FILE --target $TARGET --iplots --dplots --zscale --backup sex --use-time \
                    &>> ${LOG_FILE}
                    ;;
                sexfilter)
                    python3 ~/photometrypipeline/tcs_run.py $FILE --target $TARGET --iplots --dplots --zscale --backup sex --use-filter ${BACKUP_FILTER} \
                    &>> ${LOG_FILE}
                    ;;
        esac
        
    done

}

function process_targets_in_parallel
{
    MODE=$1
    LOG_SUFFIX=$2
    BACKUP_FILTER=$3

    # loop through targets
    for TARGET in `ls $FOLDER/$DATE`; do

        # skip DARK frames
        if [ "$TARGET" == "DARK" ]; then
            continue
        fi

        # skip FLAT frames
        if [ "$TARGET" == "FLAT" ]; then
            continue
        fi

        # loop through filters
        for  FILTER in g r i z_s; do
            
            # check if filter exists
            if [ ! -d "$FOLDER/$DATE/$TARGET/$FILTER" ]; then
                continue
            fi

            # check if backup filter is not the same as the processed
            if [ "${BACKUP_FILTER}" == "$FILTER" ]; then
                continue
            fi

            # check the results of previous processing
            if [ ! -f $FOLDER/$DATE/$TARGET/${FILTER}_failed.txt ]; then
                FAILED=1
            else
                FAILED=`cat $FOLDER/$DATE/$TARGET/${FILTER}_failed.txt | wc -l`
            fi

            # start the process, if necessary
            if [ "$FAILED" == "0" ] && [ "$PASSED" != 0 ] ; then
                echo "$DATE: $TARGET ($FILTER) is fully registered"
            else
                echo "$DATE: Adding a thread for $TARGET ($FILTER)"
                thread $FOLDER/$DATE/$TARGET/$FILTER \
                       $TARGET \
                       $FILTER \
                       $MODE \
                       ${BACKUP_FILTER} \
                       $FOLDER/$DATE/$TARGET/tcs_bash_${FILTER}_${LOG_SUFFIX}.log \
                       &
            fi

        done
    done

}

function gather_processing_summary
{
    # loop through targets
    for TARGET in `ls $FOLDER/$DATE`; do

        # skip DARK frames
        if [ "$TARGET" == "DARK" ]; then
            continue
        fi

        # skip FLAT frames
        if [ "$TARGET" == "FLAT" ]; then
            continue
        fi

        for  FILTER in g r i z_s; do
            
            # check if filter exists
            if [ ! -d "$FOLDER/$DATE/$TARGET/$FILTER" ]; then
                echo "$DATE: $TARGET ($FILTER): missing"
                continue
            fi
            
            cd $FOLDER/$DATE/$TARGET/$FILTER

            python3 ~/photometrypipeline/tcs_summary.py $FILTER
            PASSED=`cat ../${FILTER}_passed.txt | wc -l`
            FAILED=`cat ../${FILTER}_failed.txt | wc -l`

            echo "$DATE: $TARGET ($FILTER): passed: $PASSED, failed: $FAILED"
            
        done
    done
}

# check that there are parameters given
if [ "$1" == "" ]; then
    usage
    exit
fi

# parse parameters
FOLDER=""
SELECTED_DATE=""
DATABASE="skip"
while [ "$1" != "" ]; do
    case $1 in
        -p | --path )           shift
                                FOLDER=$1
                                ;;
        -d | --date )           shift
                                SELECTED_DATE=$1
                                ;;
        -D | --database )       shift
                                DATABASE=$1
                                ;;
        -h | --help )           usage
                                exit
                                ;;
        * )                     usage
                                exit 1
    esac
    shift
done


if [ "$FOLDER" == "" ]; then
    echo "Failed to set folder"
    exit
else
    cd $FOLDER
    echo "Starting to process $FOLDER"
    echo "Found dates: `ls -d */ | tr '\n' ' '`"
fi
  
# loop through dates
for DATE in `ls -d */`; do
    
    # if the date was specified, process only those files
    if [ "${SELECTED_DATE}" != "" ] && [ "$DATE" != "${SELECTED_DATE}/" ]; then
        echo "Skipping $DATE as not selected"
        continue
    fi

    cd $DATE
    echo "$DATE: Found targets: `ls -d */ | tr '\n' ' '`"

    #
    # INITIAL PROCESSING
    #
    echo "$DATE: Starting initial processing of all targets"
    process_targets_in_parallel init init a
    echo "$DATE: Waiting..."
    wait
    echo "$DATE: Done"
    echo "$DATE: Summarizing"
    gather_processing_summary
    echo "$DATE: Done"
    

    #
    # FIRST BACKUP (sex-time)
    #
    echo "$DATE: Starting the sex-time backup for all targets"
    process_targets_in_parallel sextime bST a
    echo "$DATE: Waiting..."
    wait
    echo "$DATE: Done"
    echo "$DATE: Summarizing"
    gather_processing_summary
    echo "$DATE: Done"

    #
    # SECOND BACKUP (sex-filter)
    #
    echo "$DATE: Starting the sex-filter backup for all targets (g)"
    process_targets_in_parallel sexfilter bSG g
    echo "$DATE: Waiting..."
    wait
    echo "$DATE: Done"
    echo "$DATE: Summarizing"
    gather_processing_summary
    echo "$DATE: Done"

    echo "$DATE: Starting the sex-filter backup for all targets (r)"
    process_targets_in_parallel sexfilter bSR r
    echo "$DATE: Waiting..."
    wait
    echo "$DATE: Done"
    echo "$DATE: Summarizing"
    gather_processing_summary
    echo "$DATE: Done"

    echo "$DATE: Starting the sex-filter backup for all targets (i)"
    process_targets_in_parallel sexfilter bSI i
    echo "$DATE: Waiting..."
    wait
    echo "$DATE: Done"
    echo "$DATE: Summarizing"
    gather_processing_summary
    echo "$DATE: Done"

    echo "$DATE: Starting the sex-filter backup for all targets (z_s)"
    process_targets_in_parallel sexfilter bSZ z_s
    echo "$DATE: Waiting..."
    wait
    echo "$DATE: Done"
    echo "$DATE: Summarizing"
    gather_processing_summary
    echo "$DATE: Done"

done

# collect results
echo "Collecting results for all targets"
if [ "SELECTED_DATE" != "" ]; then
    python3 ~/photometrypipeline/tcs_collect.py $FOLDER --date $SELECTED_DATE --database $DATABASE &> $FOLDER/collect.log
else
    python3 ~/photometrypipeline/tcs_collect.py $FOLDER --database $DATABASE &> $FOLDER/collect.log
fi

