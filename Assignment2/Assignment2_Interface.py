
import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #Implement RangeQuery Here.
    a = 'RangeRatingsPart'
    b = 'RoundRobinRatingsPart'
    f = open("rangeResult.txt","w+")

    cur = openconnection.cursor()
    cmd = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '" + a + "%';"
    cur.execute(cmd)
    openconnection.commit()
    
    range_part = cur.fetchone()[0]
    for i in range(0,range_part):
    	part = a + str(i)
    	cmd = "SELECT * FROM " + part + " WHERE Rating >= " + str(ratingMinValue) + " AND Rating <= " + str(ratingMaxValue) + ";"
    	cur.execute(cmd)
    	openconnection.commit()
    	values = cur.fetchall()
    	for each in values:
    		f.write(tableName + "," + str(each[0]) + "," + str(each[1]) + "," + str(each[2]) + "\n")

    cmd = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '" + b + "%';"
    cur.execute(cmd)
    openconnection.commit()
    
    rrobin_part = cur.fetchone()[0]
    for i in range(0,rrobin_part):
    	part = b + str(i)
    	cmd = "SELECT * FROM " + part + " WHERE Rating >= " + str(ratingMinValue) + " AND Rating <= " + str(ratingMaxValue) + ";"
    	cur.execute(cmd)
    	openconnection.commit()
    	values = cur.fetchall()
    	for each in values:
    		temp = part + ',' + str(each[0]) + ',' + str(each[1]) + ',' + str(each[2]) + '\n'
    		f.write(str(temp))
    f.close()    
    
    

def PointQuery(ratingsTableName, ratingValue, openconnection):

	a = 'RangeRatingsPart'
	b = 'RoundRobinRatingsPart'
	f = open("pointResult.txt","w+")

	cur = openconnection.cursor()
	cmd = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '" + a + "%';"
	cur.execute(cmd)
	openconnection.commit()

	range_part = cur.fetchone()[0]
	for i in range(0,range_part):
		part = a + str(i)
		cmd = "SELECT * FROM " + part + " WHERE Rating = " + str(ratingValue) + ";"
		cur.execute(cmd)
		openconnection.commit()
		values = cur.fetchall()
		for each in values:
			temp = part + ',' + str(each[0]) + ',' + str(each[1]) + ',' + str(each[2]) + '\n'
			f.write(str(temp))

	cmd = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '" + b + "%';"
	cur.execute(cmd)
	openconnection.commit()

	rrobin_part = cur.fetchone()[0]
	for i in range(0,rrobin_part):
		part = b + str(i)
		cmd = "SELECT * FROM " + part + " WHERE Rating = " + str(ratingValue) + ";"
		cur.execute(cmd)
		openconnection.commit()
		values = cur.fetchall()
		for each in values:
			temp = part + ',' + str(each[0]) + ',' + str(each[1]) + ',' + str(each[2]) + '\n'
			f.write(str(temp))
	f.close()
	

