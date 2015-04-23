#!/usr/bin/env bash

set -e

# This is a script for building the UpdateTracker database from an MPT store database.
# It's supposed to be used while Fedora is NOT RUNNING and on an empty instance.
# It is explicitly tailored for the newspaper content models, and must be updated if
# the content models are updated to have a different description of views.

lastmodifiedtable=t$(  psql domsTripleStore -c "\copy (SELECT pkey FROM tmap WHERE p='<info:fedora/fedora-system:def/view#lastModifiedDate>')                      TO STDOUT")
statetable=t$(         psql domsTripleStore -c "\copy (SELECT pkey FROM tmap WHERE p='<info:fedora/fedora-system:def/model#state>')                                TO STDOUT")
modeltable=t$(         psql domsTripleStore -c "\copy (SELECT pkey FROM tmap WHERE p='<info:fedora/fedora-system:def/model#hasModel>')                             TO STDOUT")
haseditionpagetable=t$(psql domsTripleStore -c "\copy (SELECT pkey FROM tmap WHERE p='<http://doms.statsbiblioteket.dk/relations/default/0/1/#hasEditionPage>')    TO STDOUT")
succeedingtable=t$(    psql domsTripleStore -c "\copy (SELECT pkey FROM tmap WHERE p='<http://www.loc.gov/mods/rdf/v1#relatedSucceeding>')                         TO STDOUT")
ispartofnptable=t$(    psql domsTripleStore -c "\copy (SELECT pkey FROM tmap WHERE p='<http://doms.statsbiblioteket.dk/relations/default/0/1/#isPartOfNewspaper>') TO STDOUT")
hasparttable=t$(       psql domsTripleStore -c "\copy (SELECT pkey FROM tmap WHERE p='<info:fedora/fedora-system:def/relations-external#hasPart>')                 TO STDOUT")
hasfiletable=t$(       psql domsTripleStore -c "\copy (SELECT pkey FROM tmap WHERE p='<http://doms.statsbiblioteket.dk/relations/default/0/1/#hasFile>')           TO STDOUT")

### Records

## SummaVisible records for edition pages

echo $(date) "Get all objects of CM_EditionPage + their last modified date & status"

psql domsTripleStore -c "\copy (
    select
        'SummaVisible',
        substring(trim('<>' from $lastmodifiedtable.s), 13),
        'doms:Newspaper_Collection',
        null,
        CASE when $statetable.o='<info:fedora/fedora-system:def/model#Active>' then split_part($lastmodifiedtable.o, '\"', 2) else null end,
        split_part($lastmodifiedtable.o, '\"', 2)
    from
        $lastmodifiedtable, $statetable, $modeltable
    where
        $lastmodifiedtable.s=$statetable.s
        and $lastmodifiedtable.s=$modeltable.s
        and $modeltable.o='<info:fedora/doms:ContentModel_EditionPage>'
) TO STDOUT" > records

echo $(date) "Add to records with SummaVisible view, Newspaper_Collection collection, inactive timestamp, and possibly active timestamp"

psql domsUpdateTracker -c "\copy records (viewangle, entrypid, collection, deleted, active, inactive) FROM STDIN" < records

## SummaAuthority for authority records

echo $(date) "Get all objects of CM_Newspaper + their last modified date & status"

psql domsTripleStore -c "\copy (
    select
        'SummaAuthority',
        substring(trim('<>' from $lastmodifiedtable.s), 13),
        'doms:Newspaper_Collection',
        null,
        CASE when $statetable.o='<info:fedora/fedora-system:def/model#Active>' then split_part($lastmodifiedtable.o, '\"', 2) else null end,
        split_part($lastmodifiedtable.o, '\"', 2)
    from
        $lastmodifiedtable, $statetable, $modeltable
    where
        $lastmodifiedtable.s=$statetable.s
        and $lastmodifiedtable.s=$modeltable.s
        and $modeltable.o='<info:fedora/doms:ContentModel_Newspaper>'
) TO STDOUT" > authority

echo $(date) "Add to records with SummaAuthority view, Newspaper_Collection collection, inactive timestamp, and possibly active timestamp"

psql domsUpdateTracker -c "\copy records (viewangle, entrypid, collection, deleted, active, inactive) FROM STDIN" < authority

## SBOI for Roundtrip records

echo $(date) "Get all objects of CM_Roundtrip + their last modified date & status"

psql domsTripleStore -c "\copy (
    select
        'SBOI',
        substring(trim('<>' from $lastmodifiedtable.s), 13),
        'doms:Newspaper_Collection',
        null,
        CASE when $statetable.o='<info:fedora/fedora-system:def/model#Active>' then split_part($lastmodifiedtable.o, '\"', 2) else null end,
        split_part($lastmodifiedtable.o, '\"', 2)
    from
        $lastmodifiedtable, $statetable, $modeltable
    where
        $lastmodifiedtable.s=$statetable.s
        and $lastmodifiedtable.s=$modeltable.s
        and $modeltable.o='<info:fedora/doms:ContentModel_RoundTrip>'
) TO STDOUT" > roundtrips

echo $(date) "Add to records with SBOI view, Newspaper_Collection collection, inactive timestamp, and possibly active timestamp"

psql domsUpdateTracker -c "\copy records (viewangle, entrypid, collection, deleted, active, inactive) FROM STDIN" < roundtrips

## GUI for newspapers and editions

echo $(date) "Get all objects of CM_Roundtrip + their last modified date & status"

psql domsTripleStore -c "\copy (
    select
        'GUI',
        substring(trim('<>' from $lastmodifiedtable.s), 13),
        'doms:Newspaper_Collection',
        null,
        CASE when $statetable.o='<info:fedora/fedora-system:def/model#Active>' then split_part($lastmodifiedtable.o, '\"', 2) else null end,
        split_part($lastmodifiedtable.o, '\"', 2)
    from
        $lastmodifiedtable, $statetable, $modeltable
    where
        $lastmodifiedtable.s=$statetable.s
        and $lastmodifiedtable.s=$modeltable.s
        and ($modeltable.o='<info:fedora/doms:ContentModel_Edition>'
            or $modeltable.o='<info:fedora/doms:ContentModel_Newspaper>')
) TO STDOUT" > guirecords

echo $(date) "Add to records with GUI view, Newspaper_Collection collection, inactive timestamp, and possibly active timestamp"

psql domsUpdateTracker -c "\copy records (viewangle, entrypid, collection, deleted, active, inactive) FROM STDIN" < guirecords

### Memberships

## General

echo $(date) "Add all records to have memberships of themselves"

psql domsUpdateTracker -c "insert into memberships (viewangle, entrypid, collection, objectpid)
    select viewangle, entrypid, 'doms:Newspaper_Collection', entrypid
    from records"

## SummaVisible memberships (note recursive relation with succeeding newspapers handled later)

echo $(date) "Get all hasEditionPage relations"

psql domsTripleStore -c "\copy (
    select
        'SummaVisible',
        substring(trim('<>' from $haseditionpagetable.o), 13),
        'doms:Newspaper_Collection',
        substring(trim('<>' from $haseditionpagetable.s), 13)
    from
        $haseditionpagetable
) TO STDOUT" > editions

echo $(date) "Add the edition to memberships for all rows where page is present"

psql domsUpdateTracker -c "\copy memberships (viewangle, entrypid, collection, objectpid) FROM STDIN" < editions

## SBOI memberships

echo $(date) "Get all hasPart relations to roundtrips to get batches"

psql domsTripleStore -c "\copy (
    select
        'SBOI',
        substring(trim('<>' from $hasparttable.o), 13),
        'doms:Newspaper_Collection',
        substring(trim('<>' from $hasparttable.s), 13)
    from
        $hasparttable, $modeltable
    where
        $modeltable.s=$hasparttable.o
        and $modeltable.o='<info:fedora/doms:ContentModel_RoundTrip>'
) TO STDOUT" > hasparts

echo $(date) "Add the batches to memberships"

psql domsUpdateTracker -c "\copy memberships (viewangle, entrypid, collection, objectpid) FROM STDIN" < hasparts

## GUI memberships

echo $(date) "Get all editionPage relations"

psql domsTripleStore -c "\copy (
    select
        'GUI',
        substring(trim('<>' from $haseditionpagetable.s), 13),
        'doms:Newspaper_Collection',
        substring(trim('<>' from $haseditionpagetable.o), 13)
    from
        $haseditionpagetable
) TO STDOUT" > editionsforgui

echo $(date) "Add the edition to memberships for GUI"

psql domsUpdateTracker -c "\copy memberships (viewangle, entrypid, collection, objectpid) FROM STDIN" < editionsforgui

echo $(date) "Get all hasFile relations from editionpages"

psql domsTripleStore -c "\copy (
    select
        'GUI',
        substring(trim('<>' from $haseditionpagetable.s), 13),
        'doms:Newspaper_Collection',
        substring(trim('<>' from $hasfiletable.o), 13)
    from
        $hasfiletable, $haseditionpagetable
    where
        $haseditionpagetable.o=$hasfiletable.s
) TO STDOUT" > hasFiles

echo $(date) "Add the files to memberships"

psql domsUpdateTracker -c "\copy memberships (viewangle, entrypid, collection, objectpid) FROM STDIN" < hasFiles

## Succeeding newspapers - SummaVisible and SummaAuthority

echo $(date) "Get succeeding records"

psql domsTripleStore -c "\copy (
    select substring(trim('<>' from s), 13), substring(trim('<>' from o), 13)
    from $succeedingtable
) TO STDOUT" > succeeding

echo $(date) "Get editionpage to newspaper relations"

psql domsTripleStore -c "\copy (
    select substring(trim('<>' from $haseditionpagetable.o), 13), substring(trim('<>' from $ispartofnptable.o), 13)
    from $ispartofnptable, $haseditionpagetable
    where $ispartofnptable.s=$haseditionpagetable.s
) TO STDOUT" > editionpagenewspapers

echo $(date) "Calculate memberships"

./authoritymemberships.py > newspaperviews

echo $(date) "Insert into memberships"

psql domsUpdateTracker -c "\copy memberships (viewangle, entrypid, collection, objectpid) FROM STDIN" < newspaperviews

echo $(date) "Done"

#TODO: Radio/TV & Reklamefilm?
