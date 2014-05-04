#!/usr/bin/python

import sys
map1 = {}
for line in sys.stdin:
	friends = [int(x) for x in line.split()]
	t = friends[0],friends[1],friends[2]
	try:
		map1[t] +=1
	except KeyError:
		map1[t] = 1
for t in map1.keys():   # if exist duplicates
	if map1[t]>1:
		print "%d %d %d" %(t[0],t[1],t[2])
		print "%d %d %d" %(t[1],t[0],t[2])
		print "%d %d %d" %(t[2],t[0],t[1])

