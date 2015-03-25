#!/bin/bash
# Dump database table with timestamps to a file. Note: It should be checked that t1 is the correct table beforehand.
date; echo "Dumping all lastModified triples from database"
lastmodifiedtable=$(psql domsTripleStore -c "\copy (SELECT pkey FROM tmap WHERE p='<info:fedora/fedora-system:def/view#lastModifiedDate>') TO STDOUT")
psql domsTripleStore -c "\copy (SELECT * FROM t$lastmodifiedtable ORDER BY o) TO STDOUT" > alltimestamps
date; echo "Done dumping. Now parsing triples."
# Rename old input file if any
if [[ -e updatetrackerdata ]]; then mv updatetrackerdata updatetrackerdata.$(date -Iseconds); fi

# Read each timestamp. Format looks like this:
#   <info:fedora/uuid:b994d7bf-7911-4413-8475-bb8dc3f7b804>	"2014-10-21T04:26:13.987Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>
#   <info:fedora/uuid:ad4796af-4b75-4691-a784-ffe8e2833936/DC>	"2014-12-19T17:42:22.25Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>
#   <info:fedora/uuid:bec3f8e5-b632-449f-b4dc-8a307ded2bd2/RELS-EXT>	"2014-12-26T16:21:47.837Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>
#   <info:fedora/uuid:b07d26be-022f-480a-aff4-480f32df401a>	"2015-03-02T00:28:57.77Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>
sed -e '/<info:fedora\/[^/]*>/!d;s/^<info:fedora\///;s%>\t"%\t%;s%"^^<http://www.w3.org/2001/XMLSchema#dateTime>$%\tingest%' alltimestamps >> updatetrackerdata
#timestamp=$(date -d "${timestamp/T/ }" +%s%3N)

# Ingest into updatetracker log database table. Assumes table already exists.
date; echo "Ingesting all triples into database"
cat updatetrackerdata | psql avisdoms -c "\copy \"updateTrackerLogs\" (pid, happened, method) from stdin"
date; echo "All done."
