#!/usr/bin/env python
f = open('succeeding', 'r')
lines = []

for line in f:
    lines.append(list(line.strip().split()))

npstarters = set([i[0] for i in lines]) - set([i[1] for i in lines])
npnext = dict(lines)
nplists = dict()
for npstarter in npstarters:
    np = npstarter
    nplist = []
    while np :
        nplist.append(np)
        np = npnext.get(np)
    for np1 in nplist:
        nplists[np1]=nplist
        for np2 in nplist:
            print("SummaAuthority\t" + np1 + "\tdoms:Newspaper_Collection\t"+np2)

lines = []
f = open('editionpagenewspapers', 'r')
for line in f:
    lineparts = line.strip().split()
    editionpage = lineparts[0]
    newspaper = lineparts[1]
    nplist = nplists.get(newspaper)
    if nplist:
        for np2 in nplist:
            print("SummaVisible\t" + editionpage + "\tdoms:Newspaper_Collection\t" + np2)
    else:
        print("SummaVisible\t" + editionpage + "\tdoms:Newspaper_Collection\t" + newspaper)

