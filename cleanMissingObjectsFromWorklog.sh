#!/usr/bin/env bash

uniqueIDs=$(mktemp)

psql -U domsFieldSearch -h achernar domsFieldSearch  -c 'select distinct pid from updatetrackerlogs;' > $uniqueIDs

checkAfPids=$(mktemp)

cat $uniqueIDs | xargs -I'{}' curl -I -o /dev/null -s -S -w "%{url_effective} %{http_code}\n" -u 'fedoraAdmin:fedoraAdminPass' "http://localhost:7880/fedora/objects/{}"> $checkAfPids

IdsToDelete=$(mktemp)

cat $checkAfPids | grep ' 404' | cut -d' ' -f1 | cut -d'/' -f6-7 | grep ':' > $IdsToDelete

cat $IdsToDelete | xargs -I'{}' psql -U domsFieldSearch -h achernar domsFieldSearch  -c "delete from updatetrackerlogs  where updatetrackerlogs.pid='{}';"
