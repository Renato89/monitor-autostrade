#!/bin/bash

fname="data/feb2016/01.02.2016_cleaned.csv"
day="2016-02-01"
header=1
day_regex='[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}'
timestamp_regex='[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}' # hh-mm-ss

first=$(sed '2q;d' $fname) # first line
[[ $first =~ $timestamp_regex ]]
# convert timestamp in seconds
pre_timestamp=$(date --date "$day ${BASH_REMATCH[0]}" +%s)

while read -r line; do

    if [ $header -eq 0 ]; then

        [[ $line =~ $timestamp_regex ]]

        timestamp=$(date --date "$day ${BASH_REMATCH[0]}" +%s)

        if [ $timestamp -ne $pre_timestamp ]; then
            delta=$((timestamp - pre_timestamp))
            
            sleep $delta
            #sleep 0.1
            pre_timestamp=$timestamp
        fi
        # substitute the old timestamp with current one
        comp_date_regex='[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}'
        curr_timestamp=$(date +"%Y-%m-%d %T")
        [[ $line =~ $comp_date_regex ]]
        # substitution
        line=${line/$BASH_REMATCH/$curr_timestamp}

        echo "$line"

    else
        # skip header
        header=0
    fi

done <"$fname"
