#!/usr/bin/env bash

psql -U domsFieldSearch -h achernar domsFieldSearch  -c "delete from updatetrackerlogs where pid in (select distinct pid from updatetrackerlogs where method='purgeObject');"