
import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #Implement RangeQuery Here.
    cursor = openconnection.cursor()
    file_write = open(outputPath,"w+")
    command_get_range_table = "select count(*) from rangeratingsmetadata"
    cursor.execute(command_get_range_table)
    number_of_range_table = cursor.fetchone()[0]
    for table_num in range(0,number_of_range_table):
    	table_name = 'rangeratingspart' + str(table_num)
    	command = 'select * from ' +table_name+' where rating>=' +str(ratingMinValue)+ ' and rating<=' +str(ratingMaxValue)
    	cursor.execute(command)
    	result_set = cursor.fetchall()
    	for table_row in result_set:
    		make_output_line = str(table_name)+ ','+ str(table_row[0]) + ',' + str(table_row[1]) + ',' +str(table_row[2]) + '\n' 
    		file_write.write(make_output_line)

    command_get_range_table = "select * from roundrobinratingsmetadata"
    cursor.execute(command_get_range_table)
    number_of_roundrobin_table = cursor.fetchone()[0]
    for table_num in range(0,number_of_roundrobin_table):
    	table_name = 'roundrobinratingspart' + str(table_num)
    	command = 'select * from ' +table_name+' where rating>=' +str(ratingMinValue)+ ' and rating<=' +str(ratingMaxValue)
    	cursor.execute(command)
    	result_set = cursor.fetchall()
    	for table_row in result_set:
    		make_output_line = str(table_name)+ ','+ str(table_row[0]) + ',' + str(table_row[1]) + ',' +str(table_row[2]) + '\n' 
    		file_write.write(make_output_line)

    file_write.close()
    openconnection.commit()



def PointQuery(ratingValue, openconnection, outputPath):
    #Implement PointQuery Here.
  	cursor = openconnection.cursor()
  	file_write = open(outputPath,"w+")
  	command_get_range_table = "select count(*) from rangeratingsmetadata"
  	cursor.execute(command_get_range_table)
  	number_of_range_table = cursor.fetchone()[0]
  	for table_num in range(0,number_of_range_table):
  		table_name = 'rangeratingspart' + str(table_num)
  		command = 'select * from ' +table_name+' where rating=' +str(ratingValue)
  		print(command)
  		cursor.execute(command)
  		result_set = cursor.fetchall()
  		for table_row in result_set:
  			make_output_line = str(table_name)+ ','+ str(table_row[0]) + ',' + str(table_row[1]) + ',' +str(table_row[2]) + '\n'
  			file_write.write(make_output_line)

  	command_get_range_table = "select * from roundrobinratingsmetadata"
  	cursor.execute(command_get_range_table)
  	number_of_roundrobin_table = cursor.fetchone()[0]
  	for table_num in range(0,number_of_roundrobin_table):
  		table_name = 'roundrobinratingspart' + str(table_num)
  		command = 'select * from ' +table_name+' where rating=' +str(ratingValue)
  		print(command)
  		cursor.execute(command)
  		result_set = cursor.fetchall()
  		for table_row in result_set:
  			make_output_line = str(table_name)+ ','+ str(table_row[0]) + ',' + str(table_row[1]) + ',' +str(table_row[2]) + '\n'
  			file_write.write(make_output_line)

  	file_write.close()
  	openconnection.commit()