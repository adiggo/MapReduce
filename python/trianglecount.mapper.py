#!/usr/bin/python

# change above line to point to local 
# python executable

import sys
map1 = {}
tmp = {}
for line in sys.stdin:
	friends = [int(x) for x in line.split()]
	for p in friends[1:len(friends)]:
		filteredpool=filter(lambda a: a>p,friends[1:len(friends)])
		for v in filteredpool:
			s = sorted([friends[0],p,v])
			print "%d %d %d" %(s[0],s[1],s[2])

