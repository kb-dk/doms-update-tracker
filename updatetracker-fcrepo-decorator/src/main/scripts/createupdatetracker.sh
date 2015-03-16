#!/bin/bash
# Dump database table with timestamps to a file
psql domsTripleStore -c "\copy (SELECT * FROM t1 ORDER BY o) TO STDOUT" > alltimestamps
# Remove old input file if any
rm -f updatetrackerdata
# Read each timestamp
i = 0
cat alltimestamps | while read object date; do
    # Extract object name
    object=${object##<info:fedora/}
    object=${object%%>}
    # Skip if this is a datastream, not an object
    if [[ "$object" == */* ]]; then continue; fi
    # Extract date
    date=${date##\"}
    date=${date%%\"^^<http://www.w3.org/2001/XMLSchema#dateTime>}
    #date=$(date -d "${date/T/ }" +%s%3N)
    # Add to update tracker log
    echo -e $object\\t$date\\tingest >> updatetrackerdata
    # Report progress
    i=$((i+1))
    if [[ $((i%100000)) == 0 ]]; then date; echo "Handled $i timestamps"; fi
done
# Ingest into updatetracker log database table
cat updatetrackerdata | psql avisdoms -c "\copy \"updateTrackerLogs\" (pid, happened, method) from stdin"
