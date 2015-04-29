#!/usr/bin/env bash

set -e

DOMSTRIPLESTOREDBNAME=domsTripleStore
DOMSUPDATETRACKERDBNAME=domsUpdateTracker

# This is a script for building the UpdateTracker database from an MPT store database.
# It's supposed to be used while Fedora is NOT RUNNING and on an empty instance.
# It is explicitly tailored for the newspaper content models, and must be updated if
# the content models are updated to have a different description of views.

source rebuildfunctions.include

### Records

## SummaVisible records for edition pages
addRecordsForContentModel "doms:ContentModel_EditionPage" "doms:Newspaper_Collection" "SummaVisible"

## SummaVisible records for radio tv
addRecordsForContentModel "doms:ContentModel_Program" "doms:RadioTV_Collection" "SummaVisible"

## SummaVisible records for reklamefilm
addRecordsForContentModel "doms:ContentModel_Reklamefilm" "doms:Collection_Reklamefilm" "SummaVisible"

## SummaAuthority for authority records
addRecordsForContentModel "doms:ContentModel_Newspaper" "doms:Newspaper_Collection" "SummaAuthority"

## SBOI for Roundtrip, Edition or Newspaper records
addRecordsForContentModel "doms:ContentModel_RoundTrip" "doms:Newspaper_Collection" "SBOI"
addRecordsForContentModel "doms:ContentModel_Newspaper" "doms:Newspaper_Collection" "SBOI"
addRecordsForContentModel "doms:ContentModel_Edition" "doms:Newspaper_Collection" "SBOI"

## GUI for newspapers & editions
addRecordsForContentModel "doms:ContentModel_Newspaper" "doms:Newspaper_Collection" "GUI"
addRecordsForContentModel "doms:ContentModel_Edition" "doms:Newspaper_Collection" "GUI"

## GUI for radiotvprograms and files
addRecordsForContentModel "doms:ContentModel_Program" "doms:RadioTV_Collection" "GUI"
addRecordsForContentModel "doms:ContentModel_RadioTVFile" "doms:RadioTV_Collection" "GUI"
addRecordsForContentModel "doms:ContentModel_VHSFile" "doms:RadioTV_Collection" "GUI"

## SummaVisible records for reklamefilm
addRecordsForContentModel "doms:ContentModel_Reklamefilm" "doms:Collection_Reklamefilm" "GUI"

## GUI for collections and licenses
addRecordsForContentModelUnknownCollection "doms:ContentModel_Collection" "GUI"
addRecordsForContentModelUnknownCollection "doms:ContentModel_License" "GUI"

### Memberships

## General

addMembershipsForAllRecords

## SummaVisible newspaper memberships

addMembershipsForInverseRelation "http://doms.statsbiblioteket.dk/relations/default/0/1/#hasEditionPage" "doms:Newspaper_Collection" "SummaVisible" "doms:ContentModel_EditionPage"
addLinkedListRelationsFromInverseForwardCombinedRelations "http://www.loc.gov/mods/rdf/v1#relatedSucceeding" "doms:ContentModel_Newspaper" "http://doms.statsbiblioteket.dk/relations/default/0/1/#hasEditionPage" "http://doms.statsbiblioteket.dk/relations/default/0/1/#isPartOfNewspaper" "SummaVisible" "doms:Newspaper_Collection" "doms:ContentModel_EditionPage"

## SummaAuthority newspaper memberships

addLinkedListRelations "http://www.loc.gov/mods/rdf/v1#relatedSucceeding" "doms:ContentModel_Newspaper" "SummaAuthority" "doms:Newspaper_Collection"

## SBOI memberships

addMembershipsForInverseRelation "info:fedora/fedora-system:def/relations-external#hasPart" "doms:Newspaper_Collection" "SBOI" "doms:ContentModel_RoundTrip"

## GUI memberships for newspapers

addMembershipsForRelation "http://doms.statsbiblioteket.dk/relations/default/0/1/#hasEditionPage" "doms:Newspaper_Collection" "GUI" "doms:ContentModel_Edition"
addMembershipsForRelation "http://doms.statsbiblioteket.dk/relations/default/0/1/#hasBrik" "doms:Newspaper_Collection" "GUI" "doms:ContentModel_Edition"
addMembershipsForCombinedRelations "http://doms.statsbiblioteket.dk/relations/default/0/1/#hasEditionPage" "http://doms.statsbiblioteket.dk/relations/default/0/1/#hasFile" "doms:Newspaper_Collection" "GUI"  "doms:ContentModel_Edition"
addMembershipsForCombinedRelations "http://doms.statsbiblioteket.dk/relations/default/0/1/#hasBrik" "http://doms.statsbiblioteket.dk/relations/default/0/1/#hasFile" "doms:Newspaper_Collection" "GUI"  "doms:ContentModel_Edition"

## GUI memberships for radiotv

addMembershipsForRelation "http://doms.statsbiblioteket.dk/relations/default/0/1/#hasFile" "doms:RadioTV_Collection" "GUI" "doms:ContentModel_Program"
addMembershipsForRelation "http://doms.statsbiblioteket.dk/relations/default/0/1/#hasFile" "doms:Collection_Reklamefilm" "GUI" "doms:ContentModel_Reklamefilm"

echo $(date) "Done"
