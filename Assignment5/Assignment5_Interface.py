#
# Assignment5 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
import json
import re

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
	key = cityToSearch.strip()
	file = open(saveLocation1, 'w')
	result = collection.find({'city' : re.compile(key, re.IGNORECASE)}, {'_id':0, 'name':1, 'full_address':1, 'city':1, 'state':1 })
	for data in result:
		res = data['name'] + '$' + data['full_address'] + '$' + data['city'] + '$' + data['state']
		file.write(res.upper() + "\n")

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):

    pass

def DistanceFunction(lat2, lon2, lat1, lon1):
	R = 3959
	latradion1 = math.radians(lat1)
	latradion2 = math.radians(lat2)