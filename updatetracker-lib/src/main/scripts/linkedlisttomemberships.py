#!/usr/bin/env python
import sys

# This script reads a list of x,y pairs defining a set of linked lists from a file.
# It maps these pairs to a dictionary of lists, mapping from x->[list], where [list] is the list containing x
# It then reads a list of objects and a corresponding value from the linked lists, and for each object prints
#   membership lines for all objects in the list referred to.

linkedlistpairfile = sys.argv[1]
referringobjectsfile = sys.argv[2]
view = sys.argv[3]
collection = sys.argv[4]

# Read all the pairs defining the linked lists, and put them in a dictionary
f = open(linkedlistpairfile, 'r')
linkedlistpairs=dict()
for line in f:
    lineparts = line.strip().split()
    linkedlistpairs[lineparts[0]] = lineparts[1]

# Find all objects on the left hand side of any pair, but not on any right hand side of any pair.
# These are the first objects in any list.
linkedlistheads = set(linkedlistpairs.keys())-set(linkedlistpairs.values())

# Use the list heads to follow relations using the dictionary.
# Create a dictionary linking each object to the list containing it.
lists = dict()
for linkedlisthead in linkedlistheads:
    currentElement = linkedlisthead
    currentList = []
    while currentElement:
        currentList.append(currentElement)
        currentElement = linkedlistpairs.get(currentElement)
    for np1 in currentList:
        lists[np1]=currentList

# Read all lines from file with objects with relations to these lists.
# For each line print the membership
lines = []
f = open(referringobjectsfile, 'r')
for line in f:
    lineparts = line.strip().split()
    subject = lineparts[0]
    subobject = lineparts[1]
    currentList = lists.get(subobject)
    if currentList:
        for object in currentList:
            print(view +"\t" + subject + "\t" + collection + "\t" + object)
    else:
        # If no list is matched, we should consider it to be a singleton list and add a single line
        print(view +"\t" + subject + "\t" + collection + "\t" + subobject)
