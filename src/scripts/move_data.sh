#!/bin/bash
#====================================================================================
# Author: Mohammad Zain Abbas
# Date: 20th March, 2022
#====================================================================================
# Basic script is copy all .csv files to project's import directory for Neo4j
#====================================================================================

# Enable exit on error
set -e -u -o pipefail

log () {
    echo "[[ log ]] $1"
}

error () {
    echo "[[ error ]] $1"
}

#Function that shows usage for this script
function usage()
{
cat << HEREDOC
Copy all .csv files to a given path
Usage: 
    
    $progname [OPTION] [Value]

Options:

    -p, --path              Desc. path
    -h, --help              Show usage

Examples:

    $ $progname -p ~/Downloads/sdm
    ⚐ → Copies all .csv files to "~/Downloads/sdm".

HEREDOC
}

progname=$(basename $0)
path=~/Downloads/sdm

#Get all the arguments and update accordingly
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -p|--path) scale="$2"; shift ;;
        -h|--help)
        usage
        exit 1
        ;;
        *) printf "\n$progname: invalid option → '$1'\n\n⚐ Try '$progname -h' for more information\n\n"; exit 1 ;;
    esac
    shift
done

csv_files_count=`ls -1 $path/*.csv 2> /dev/null | wc -l`

if [ $csv_files_count != 0 ]
then
    log "Removing old data"
    rm $path/*.csv
fi

log "Copy new data"
cp ../../data/*.csv $path

log "All done !!"