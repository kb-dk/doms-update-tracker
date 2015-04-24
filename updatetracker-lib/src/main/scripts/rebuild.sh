#!/usr/bin/env bash

set -e

DOMSTRIPLESTOREDBNAME=domsTripleStore
DOMSUPDATETRACKERDBNAME=domsUpdateTracker

# This is a script for building the UpdateTracker database from an MPT store database.
# It's supposed to be used while Fedora is NOT RUNNING and on an empty instance.
# It is explicitly tailored for the newspaper content models, and must be updated if
# the content models are updated to have a different description of views.

lastmodifiedtable=t$(psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (SELECT pkey FROM tmap WHERE p='<info:fedora/fedora-system:def/view#lastModifiedDate>')                       TO STDOUT")
statetable=t$(       psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (SELECT pkey FROM tmap WHERE p='<info:fedora/fedora-system:def/model#state>')                                 TO STDOUT")
modeltable=t$(       psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (SELECT pkey FROM tmap WHERE p='<info:fedora/fedora-system:def/model#hasModel>')                              TO STDOUT")
collectiontable=t$(  psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (SELECT pkey FROM tmap WHERE p='<http://doms.statsbiblioteket.dk/relations/default/0/1/#isPartOfCollection>') TO STDOUT")

# Given a content model, a collection and a view,
# insert all objects of that content model into records for the given collection and view
function addRecordsForContentModel {
    cm=$1
    collection=$2
    view=$3
    echo $(date) "Get all objects of $1 + their last modified date & status"

    psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (
        select
            '$view',
            substring(trim('<>' from $lastmodifiedtable.s), 13),
            '$collection',
            null,
            CASE when $statetable.o='<info:fedora/fedora-system:def/model#Active>' then split_part($lastmodifiedtable.o, '\"', 2) else null end,
            split_part($lastmodifiedtable.o, '\"', 2)
        from
            $lastmodifiedtable, $statetable, $modeltable
        where
            $lastmodifiedtable.s=$statetable.s
            and $lastmodifiedtable.s=$modeltable.s
            and $modeltable.o='<info:fedora/$1>'
    ) TO STDOUT" > $1.records

    echo $(date) "Add to records with $view view, $collection collection, inactive timestamp, and possibly active timestamp"

    psql "$DOMSUPDATETRACKERDBNAME" -c "\copy records (viewangle, entrypid, collection, deleted, active, inactive) FROM STDIN" < $1.records
}

# Given a content model and a view,
# insert all objects of that content model into records for the given view and the collections those objects are members of
function addRecordsForContentModelUnknownCollection {
    cm=$1
    view=$2
    echo $(date) "Get all objects of $1 + their last modified date & status"

    psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (
        select
            '$view',
            substring(trim('<>' from $lastmodifiedtable.s), 13),
            substring(trim('<>' from $collectiontable.o), 13),
            null,
            CASE when $statetable.o='<info:fedora/fedora-system:def/model#Active>' then split_part($lastmodifiedtable.o, '\"', 2) else null end,
            split_part($lastmodifiedtable.o, '\"', 2)
        from
            $lastmodifiedtable, $statetable, $modeltable, $collectiontable
        where
            $lastmodifiedtable.s=$statetable.s
            and $lastmodifiedtable.s=$modeltable.s
            and $lastmodifiedtable.s=$collectiontable.s
            and $modeltable.o='<info:fedora/$1>'
    ) TO STDOUT" > $1.records

    echo $(date) "Add to records with $view view correct collection, inactive timestamp, and possibly active timestamp"

    psql "$DOMSUPDATETRACKERDBNAME" -c "\copy records (viewangle, entrypid, collection, deleted, active, inactive) FROM STDIN" < $1.records
}

# Given a relationname, a collection and a view,
# insert all objects with that relation into memberships, for the given source object, collection and view
function addMembershipsForRelations {
    relation=$1
    collection=$2
    view=$3

    echo $(date) "Get all $relation relations"

    relationtable2=t$(psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (SELECT pkey FROM tmap WHERE p='<$relation>') TO STDOUT")

    psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (
        select
            '$view',
            substring(trim('<>' from $relationtable2.s), 13),
            '$collection',
            substring(trim('<>' from $relationtable2.o), 13)
        from
            $relationtable2
    ) TO STDOUT" > relation-"${relation//\//_}"

    echo $(date) "Add to memberships for collection $collection and view $view"

    psql "$DOMSUPDATETRACKERDBNAME" -c "\copy memberships (viewangle, entrypid, collection, objectpid) FROM STDIN" < relation-"${relation//\//_}"
}

# Given a relationname, a collection, a view and a content model,
# insert all objects with that relation into memberships, for the given source object, collection and view
# but only if the source objects are of a specific content model
function addMembershipsForRelationsAndContentModel {
    relation=$1
    collection=$2
    view=$3
    contentmodel=$4

    echo $(date) "Get all $relation relations"

    relationtable2=t$(psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (SELECT pkey FROM tmap WHERE p='<$relation>') TO STDOUT")

    psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (
        select
            '$view',
            substring(trim('<>' from $relationtable2.s), 13),
            '$collection',
            substring(trim('<>' from $relationtable2.o), 13)
        from
            $relationtable2, $modeltable
        where
            $modeltable.s=$relationtable2.s
            and $modeltable.o='<info:fedora/$contentmodel>'
    ) TO STDOUT" > relation-"${relation//\//_}"

    echo $(date) "Add to memberships for collection $collection and view $view"

    psql "$DOMSUPDATETRACKERDBNAME" -c "\copy memberships (viewangle, entrypid, collection, objectpid) FROM STDIN" < relation-"${relation//\//_}"
}

# Given two relationnames, a collection and a view
# insert all objects connected by the two relations into memberships, for the given source object, collection and view
function addMembershipsForCombinedRelations {
    relation1=$1
    relation2=$2
    collection=$3
    view=$4

    echo $(date) "Get all $relation1 -> $relation2 relations"

    relationtable1=t$(psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (SELECT pkey FROM tmap WHERE p='<$relation1>') TO STDOUT")
    relationtable2=t$(psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (SELECT pkey FROM tmap WHERE p='<$relation2>') TO STDOUT")

    psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (
        select
            '$view',
            substring(trim('<>' from $relationtable1.s), 13),
            '$collection',
            substring(trim('<>' from $relationtable2.o), 13)
        from
            $relationtable1,$relationtable2
        where
            $relationtable1.o=$relationtable2.s
    ) TO STDOUT" > relation-"${relation1//\//_}"-"${relation2//\//_}"

    echo $(date) "Add to memberships for collection $collection and view $view"

    psql "$DOMSUPDATETRACKERDBNAME" -c "\copy memberships (viewangle, entrypid, collection, objectpid) FROM STDIN" < relation-"${relation1//\//_}"-"${relation2//\//_}"
}

# Given a relationname, a collection and a view,
# insert all objects with that relation into memberships, for the given target object, collection and view
function addMembershipsForInverseRelations {
    relation=$1
    collection=$2
    view=$3

    echo $(date) "Get all $relation relations"

    relationtable2=t$(psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (SELECT pkey FROM tmap WHERE p='<$relation>') TO STDOUT")

    psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (
        select
            '$view',
            substring(trim('<>' from $relationtable2.o), 13),
            '$collection',
            substring(trim('<>' from $relationtable2.s), 13)
        from
            $relationtable2
    ) TO STDOUT" > inverserelation-"${relation//\//_}"

    echo $(date) "Add to memberships for collection $collection and view $view"

    psql "$DOMSUPDATETRACKERDBNAME" -c "\copy memberships (viewangle, entrypid, collection, objectpid) FROM STDIN" < inverserelation-"${relation//\//_}"
}

# Given a relationname, a collection and a view,
# insert all objects with that relation into memberships, for the given target object, collection and view,
# but only if the target objects are of a specific content model
function addMembershipsForInverseRelationsAndContentModel {
    relation=$1
    collection=$2
    view=$3
    contentmodel=$4

    echo $(date) "Get all $relation relations to contentmodel $contentmodel"

    relationtable2=t$(psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (SELECT pkey FROM tmap WHERE p='<$relation>') TO STDOUT")

    psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (
        select
            '$view',
            substring(trim('<>' from $relationtable2.o), 13),
            '$collection',
            substring(trim('<>' from $relationtable2.s), 13)
        from
            $relationtable2, $modeltable
        where
            $modeltable.s=$relationtable2.o
            and $modeltable.o='<info:fedora/$contentmodel>'
    ) TO STDOUT" > inverserelation-"${relation//\//_}"

    echo $(date) "Add the batches to memberships"

    psql "$DOMSUPDATETRACKERDBNAME" -c "\copy memberships (viewangle, entrypid, collection, objectpid) FROM STDIN" < inverserelation-"${relation//\//_}"
}

# Given a relation of objects referring to each other in a linked list, a collection and a view,
# calculate all objects that are connected by that relation, and add those to memberships for all connected objects.
function addLinkedListRelations {
linkedlistrelation=$1
view=$2
collection=$3

echo $(date) "Get linked list relation $linkedlistrelation"

linkedlistrelationtable=t$(psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (SELECT pkey FROM tmap WHERE p='<$linkedlistrelation>') TO STDOUT")

psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (
    select substring(trim('<>' from s), 13), substring(trim('<>' from o), 13)
    from $linkedlistrelationtable
) TO STDOUT" > linkedlist-"${linkedlistrelation//\//_}"

echo $(date) "Calculate memberships"

./linkedlisttomemberships.py linkedlist-"${linkedlistrelation//\//_}" linkedlist-"${linkedlistrelation//\//_}" "$view" "$collection" > results-"${linkedlistrelation//\//_}"

echo $(date) "Insert into memberships"

psql "$DOMSUPDATETRACKERDBNAME" -c "\copy memberships (viewangle, entrypid, collection, objectpid) FROM STDIN" < results-"${linkedlistrelation//\//_}"
}

# Given a relation of objects referring to each other in a linked list, an inverse relation and a relation pointing to objects in this linked list plus a collection and a view,
# calculate all objects that are connected by the linked list relation,
# and add those to memberships for all objects connected to it by the inverse and normal relations.
function addLinkedListRelationsFromInverseForwardCombinedRelations {
linkedlistrelation=$1
inverserelation=$2
relation=$3
view=$4
collection=$5

echo $(date) "Get linked list relation $linkedlistrelation"

linkedlistrelationtable=t$(psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (SELECT pkey FROM tmap WHERE p='<$linkedlistrelation>') TO STDOUT")

psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (
    select substring(trim('<>' from s), 13), substring(trim('<>' from o), 13)
    from $linkedlistrelationtable
) TO STDOUT" > linkedlist-"${linkedlistrelation//\//_}"

echo $(date) "Get relations $relation to linkedlist relation $inverserelation"

relationtable1=t$(psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (SELECT pkey FROM tmap WHERE p='<$inverserelation>') TO STDOUT")
relationtable2=t$(    psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (SELECT pkey FROM tmap WHERE p='<$relation>') TO STDOUT")

psql "$DOMSTRIPLESTOREDBNAME" -c "\copy (
    select substring(trim('<>' from $relationtable1.o), 13), substring(trim('<>' from $relationtable2.o), 13)
    from $relationtable2, $relationtable1
    where $relationtable2.s=$relationtable1.s
) TO STDOUT" > linkedlist-"${relation//\//_}"

echo $(date) "Calculate memberships"

./linkedlisttomemberships.py linkedlist-"${linkedlistrelation//\//_}" linkedlist-"${relation//\//_}" "$view" "$collection"> results-"${relation//\//_}"

echo $(date) "Insert into memberships"

psql "$DOMSUPDATETRACKERDBNAME" -c "\copy memberships (viewangle, entrypid, collection, objectpid) FROM STDIN" < results-"${relation//\//_}"
}

### Records

## SummaVisible records for edition pages
addRecordsForContentModel doms:ContentModel_EditionPage doms:Newspaper_Collection SummaVisible

## SummaVisible records for radio tv
addRecordsForContentModel doms:ContentModel_Program doms:RadioTV_Collection SummaVisible

## SummaVisible records for reklamefilm
addRecordsForContentModel doms:ContentModel_Reklamefilm doms:Collection_Reklamefilm SummaVisible

## SummaAuthority for authority records
addRecordsForContentModel doms:ContentModel_Newspaper doms:Newspaper_Collection SummaAuthority

## SBOI for Roundtrip, Edition or Newspaper records
addRecordsForContentModel doms:ContentModel_RoundTrip doms:Newspaper_Collection SBOI
addRecordsForContentModel doms:ContentModel_Newspaper doms:Newspaper_Collection SBOI
addRecordsForContentModel doms:ContentModel_Edition doms:Newspaper_Collection SBOI

## GUI for newspapers & editions
addRecordsForContentModel doms:ContentModel_Newspaper doms:Newspaper_Collection GUI
addRecordsForContentModel doms:ContentModel_Edition doms:Newspaper_Collection GUI

## GUI for radiotvprograms and files
addRecordsForContentModel doms:ContentModel_Program doms:RadioTV_Collection GUI
addRecordsForContentModel doms:ContentModel_RadioTVFile doms:RadioTV_Collection GUI
addRecordsForContentModel doms:ContentModel_VHSFile doms:RadioTV_Collection GUI

## SummaVisible records for reklamefilm
addRecordsForContentModel doms:ContentModel_Reklamefilm doms:Collection_Reklamefilm GUI

## GUI for collections and licenses
addRecordsForContentModelUnknownCollection doms:ContentModel_Collection GUI
addRecordsForContentModelUnknownCollection doms:ContentModel_License GUI

### Memberships

## General

echo $(date) "Add all records to have memberships of themselves"

psql "$DOMSUPDATETRACKERDBNAME" -c "insert into memberships (viewangle, entrypid, collection, objectpid)
    select viewangle, entrypid, collection, entrypid
    from records"

## SummaVisible newspaper memberships

addMembershipsForInverseRelations http://doms.statsbiblioteket.dk/relations/default/0/1/#hasEditionPage doms:Newspaper_Collection SummaVisible
addLinkedListRelationsFromInverseForwardCombinedRelations http://www.loc.gov/mods/rdf/v1#relatedSucceeding http://doms.statsbiblioteket.dk/relations/default/0/1/#hasEditionPage http://doms.statsbiblioteket.dk/relations/default/0/1/#isPartOfNewspaper "SummaVisible" "doms:Newspaper_Collection"

## SummaAuthority newspaper memberships

addLinkedListRelations http://www.loc.gov/mods/rdf/v1#relatedSucceeding "SummaAuthority" "doms:Newspaper_Collection"

## SBOI memberships

addMembershipsForInverseRelationsAndContentModel info:fedora/fedora-system:def/relations-external#hasPart doms:Newspaper_Collection SBOI doms:ContentModel_RoundTrip

## GUI memberships for newspapers

addMembershipsForRelations http://doms.statsbiblioteket.dk/relations/default/0/1/#hasEditionPage doms:Newspaper_Collection GUI
addMembershipsForRelations http://doms.statsbiblioteket.dk/relations/default/0/1/#hasBrik doms:Newspaper_Collection GUI
addMembershipsForCombinedRelations http://doms.statsbiblioteket.dk/relations/default/0/1/#hasEditionPage http://doms.statsbiblioteket.dk/relations/default/0/1/#hasFile doms:Newspaper_Collection GUI
addMembershipsForCombinedRelations http://doms.statsbiblioteket.dk/relations/default/0/1/#hasBrik http://doms.statsbiblioteket.dk/relations/default/0/1/#hasFile doms:Newspaper_Collection GUI

## GUI memberships for radiotv

addMembershipsForRelationsAndContentModel http://doms.statsbiblioteket.dk/relations/default/0/1/#hasFile doms:RadioTV_Collection GUI doms:ContentModel_Program
addMembershipsForRelationsAndContentModel http://doms.statsbiblioteket.dk/relations/default/0/1/#hasFile doms:Collection_Reklamefilm GUI doms:ContentModel_Reklamefilm


echo $(date) "Done"
