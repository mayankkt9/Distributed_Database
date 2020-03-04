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
    thread_id = [0] * total_threads
    temp_table = 'temp_table'
    command_min_max = "select min("+SortingColumnName+"), max("+SortingColumnName+") from "+InputTable
    command_drop_output = 'drop table if exists '+OutputTable
    command_create_output = 'create table '+OutputTable+' as select * from ratings where true=false'
    cursor.execute(command_min_max)
    get_rating = cursor.fetchone()
    min_rating = get_rating[0]
    max_rating = get_rating[1]
    rating_range = (max_rating-min_rating)/total_threads

    for i in range(total_threads):
        rating_from = min_rating + i * rating_range
        rating_to = rating_from + rating_range
        thread_id[i] = threading.Thread(target=range_partition, args=(i, InputTable, SortingColumnName, rating_from, rating_to, temp_table, openconnection))
        thread_id[i].start()

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
    print(i)
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
    pass # Remove this once you are done with implementation


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


