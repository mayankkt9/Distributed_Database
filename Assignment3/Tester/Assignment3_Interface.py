#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    
    cursor = openconnection.cursor()
    total_threads = 5
    thread_id = []
    temp_table = 'temp_table'
    command_min_max = "select min("+SortingColumnName+"), max("+SortingColumnName+") from "+InputTable
    command_drop_output = 'drop table if exists '+OutputTable
    command_create_output = 'create table '+OutputTable+' as select * from '+ InputTable +' where true=false'
    cursor.execute(command_min_max)
    get_rating = cursor.fetchone()
    min_rating = get_rating[0]
    max_rating = get_rating[1]
    rating_range = (max_rating-min_rating)/total_threads

    for i in range(total_threads):
        rating_from = min_rating + i * rating_range
        rating_to = rating_from + rating_range
        t = threading.Thread(target=range_partition, args=(i, InputTable, SortingColumnName, rating_from, rating_to, temp_table, openconnection))
        thread_id.append(t)
        t.start()

    cursor.execute(command_drop_output)
    cursor.execute(command_create_output)

    for i in range(total_threads):
        thread_id[i].join()
        table_name = temp_table + str(i)
        command_insert = 'insert into '+OutputTable+' select * from '+table_name
        command_drop_table = 'drop table if exists '+table_name
        cursor.execute(command_insert)
        cursor.execute(command_drop_table)

    openconnection.commit()

def range_partition(i, InputTable, SortingColumnName, rating_from, rating_to, temp_table, openconnection):
    cursor = openconnection.cursor()
    table_name = temp_table + str(i)
    command_drop_table = 'drop table if exists '+table_name
    if 0==i:
        command = 'create table '+ table_name +' as select * from '+ InputTable +' where '+SortingColumnName+' >= '+str(rating_from)+' and '+SortingColumnName+' <= '+str(rating_to)+' order by '+SortingColumnName+' asc'
        cursor.execute(command)
    else:
        command = 'create table '+ table_name +' as select * from '+ InputTable +' where '+SortingColumnName+' > '+str(rating_from)+' and '+SortingColumnName+' <= '+str(rating_to)+' order by '+SortingColumnName+' asc'
        cursor.execute(command)


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    cursor = openconnection.cursor()
    total_threads = 5
    thread_id = []
    temp_table_1 = "temp1_table"
    temp_table_2 = "temp2_table"
    temp_table_3 = "temp3_table"
    command_metadata_table_1 = "select column_name,data_type from information_schema.columns where table_name = '"+InputTable1+"'"
    command_metadata_table_2 = "select column_name,data_type from information_schema.columns where table_name = '"+InputTable2+"'"
    command_drop_output = 'drop table if exists '+OutputTable
    command_create_output = 'create table '+OutputTable+' as select * from '+ InputTable1 +' where true=false'

    min_rating,max_rating = get_min_max(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, openconnection)
    rating_range = (max_rating-min_rating)/total_threads
    cursor.execute(command_metadata_table_1)
    table_1_metadata = cursor.fetchall()
    cursor.execute(command_metadata_table_2)
    table_2_metadata = cursor.fetchall()

    for i in range(total_threads):
        rating_from = min_rating + i * rating_range
        rating_to = rating_from + rating_range
        t = threading.Thread(target=range_partition_join, args=(i, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, table_1_metadata, table_2_metadata, rating_from, rating_to, temp_table_1, temp_table_2, temp_table_3, openconnection))
        thread_id.append(t)
        t.start()

    cursor.execute(command_drop_output)
    cursor.execute(command_create_output)
    generate_query = gen_query(OutputTable,table_2_metadata)
    cursor.execute(generate_query)
    for i in range(total_threads):
        thread_id[i].join()
        table_name_3 = temp_table_3 + str(i)
        command_insert = 'insert into '+OutputTable+' select * from '+table_name_3
        command_drop_table = 'drop table if exists '+table_name_3
        cursor.execute(command_insert)
        cursor.execute(command_drop_table)

    openconnection.commit()


def range_partition_join(i, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, table_1_metadata, table_2_metadata, rating_from, rating_to, temp_table_1, temp_table_2, temp_table_3, openconnection):
    
    cursor = openconnection.cursor()
    table_name_1 = temp_table_1 + str(i)
    table_name_2 = temp_table_2 + str(i)
    table_name_3 = temp_table_3 + str(i)
    
    command_drop_table_1 = 'drop table if exists '+table_name_1
    command_drop_table_2 = 'drop table if exists '+table_name_2
    command_drop_table_3 = 'drop table if exists '+table_name_3
    command_create_table_1 = 'create table '+table_name_1+' as select * from '+ InputTable1 +' where true=false'
    command_create_table_2 = 'create table '+table_name_2+' as select * from '+ InputTable2 +' where true=false'
    command_create_table_3 = 'create table '+table_name_3+' as select * from '+ InputTable1 +' where true=false'
    command_insert_table_1_i0 = 'insert into '+table_name_1+' select * from '+ InputTable1 + ' where '+Table1JoinColumn+' <= ' + str(rating_to)+ ' and ' +Table1JoinColumn+ ' >= '+str(rating_from)
    command_insert_table_2_i0 = 'insert into '+table_name_2+' select * from '+ InputTable2 + ' where '+Table2JoinColumn+' <= ' + str(rating_to)+ ' and ' +Table2JoinColumn+ ' >= '+str(rating_from)
    command_insert_table_1 = 'insert into '+table_name_1+' select * from '+ InputTable1 + ' where '+Table1JoinColumn+' <= ' + str(rating_to)+ ' and ' +Table1JoinColumn+ ' > '+str(rating_from)
    command_insert_table_2 = 'insert into '+table_name_2+' select * from '+ InputTable2 + ' where '+Table2JoinColumn+' <= ' + str(rating_to)+ ' and ' +Table2JoinColumn+ ' > '+str(rating_from)
    command_join = 'insert into '+table_name_3+ ' select * from '+table_name_1 + ' inner join '+table_name_2+' on '+ table_name_1+ '.'+Table1JoinColumn+'='+table_name_2+'.'+Table2JoinColumn
    
    cursor.execute(command_drop_table_1)
    cursor.execute(command_drop_table_2)
    cursor.execute(command_drop_table_3)
    cursor.execute(command_create_table_1)
    cursor.execute(command_create_table_2)
    cursor.execute(command_create_table_3)
    cursor.execute(gen_query(table_name_3,table_2_metadata))

    if 0==i:
        cursor.execute(command_insert_table_1_i0)
        cursor.execute(command_insert_table_2_i0)
    else:
        cursor.execute(command_insert_table_1)
        cursor.execute(command_insert_table_2)
    cursor.execute(command_join)
    cursor.execute(command_drop_table_1)
    cursor.execute(command_drop_table_2)


def gen_query(OutputTable,table_2_metadata):
    generate_query = 'alter table '+OutputTable+ ' '
    for i in range(len(table_2_metadata)):
        if i < (len(table_2_metadata)-1):
            generate_query += ' add column '+ table_2_metadata[i][0] + ' ' + table_2_metadata[i][1] + ','
        else:
            generate_query += ' add column '+ table_2_metadata[i][0] + ' ' + table_2_metadata[i][1]
    return generate_query


def get_min_max (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, openconnection):
    cursor = openconnection.cursor()
    command_min_max_table_1 = "select min("+Table1JoinColumn+"), max("+Table1JoinColumn+") from "+InputTable1
    command_min_max_table_2 = "select min("+Table2JoinColumn+"), max("+Table2JoinColumn+") from "+InputTable2

    cursor.execute(command_min_max_table_1)
    get_rating_1 = cursor.fetchone()
    min_rating_1 = get_rating_1[0]
    max_rating_1 = get_rating_1[1]

    cursor.execute(command_min_max_table_2)
    get_rating_2 = cursor.fetchone()
    min_rating_2 = get_rating_2[0]
    max_rating_2 = get_rating_2[1]

    min_rating = min(min_rating_1,min_rating_2)
    max_rating = max(max_rating_1,max_rating_2)

    return (min_rating,max_rating)


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


