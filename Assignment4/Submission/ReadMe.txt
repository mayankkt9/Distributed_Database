Approach


Mapper is sending key as Join Column cell value and Value as the whole row

Key - Join Column Cell Value (Extracting the second value from the row, because that would be the join column)
Value - Entire Row


What does reducer gets as an Input ?

It gets the key K  and list of all rows that has its key as K.

for example

1 -> [R,1,XYZ,ABC] , [R,1,AMS,SJW], .. ... 
2 -> [R,2,KLZ,CAS] , [S,3,KSK,SSJ], .. ... 


So in reducer we have to traverse all the list of values of the particular key and output the ones whole table name is different



Driver class is performing these configurations
	- Specifying which is the mapper class
	- Specifying which is the reducer class
	- Specifying main class to create jar from 
	- Specifying the ouput key of the mapper class
	- Specifing the output value of the mapper class
	- Passing Inputs and Getting output from Mapper and Reducer.