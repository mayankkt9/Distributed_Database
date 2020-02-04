import psycopg2


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    create_command = "create table " + ratingstablename + " (userid int, temp1 char, movieid int, temp2 char, rating float, temp3 char, timestamp bigint)"
    delete_command = "alter table " + ratingstablename + " drop temp1, drop temp2, drop temp3, drop timestamp"
    delete_rating = "drop table if exists "+ratingstablename
    cursor = openconnection.cursor()
    cursor.execute(delete_rating)
    cursor.execute(create_command)
    filepointer = open(ratingsfilepath,'r')
    cursor.copy_from(filepointer, ratingstablename, ':')
    cursor.execute(delete_command)
    openconnection.commit()
    cursor.close()
    pass

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    max_rating = 5
    rating_range = max_rating/numberofpartitions
    cursor = openconnection.cursor()
    for partition_id in range(0, numberofpartitions):
        rating_from = partition_id * rating_range
        rating_to = rating_from + rating_range
        if 0 == partition_id:
            cursor.execute('create table range_part'+str(partition_id)+' as select * from ' +
                           ratingstablename+' where rating>=0 and rating<='+str(rating_to))
        else:
            cursor.execute('create table range_part'+str(partition_id)+' as select * from ' +
                           ratingstablename+' where rating>'+str(rating_from)+' and rating<='+str(rating_to))
    openconnection.commit()
    cursor.close()
    pass


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor()
    for i in range(0, numberofpartitions):
        command = 'create table rrobin_part'+str(i)+' as select * from (select row_number() over() as row_number, *  from ' + \
            ratingstablename+') as View where mod(View.row_number,'+str(
                numberofpartitions)+') = '+(str((i+1) % numberofpartitions))
        cursor.execute(command)
    openconnection.commit()
    cursor.close()
    pass


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    command_rating_insert = 'insert into ' + ratingstablename + '(userid, movieid, rating) values ( '+str(userid)+', '+str(itemid)+', '+str(rating)+' )'
    command_count = 'select count(*) from '+ratingstablename
    rrobin_part = 'rrobin_part%'
    command_get_all_table = "select count(*) from pg_stat_user_tables where relname like '"+rrobin_part+"'"
    cursor = openconnection.cursor()
    cursor.execute(command_rating_insert)
    cursor.execute(command_count)
    row_number = (cursor.fetchall())[0][0]
    cursor.execute(command_get_all_table)
    numberofpartitions = cursor.fetchone()[0]
    partition_id = (row_number-1) % numberofpartitions
    command = 'insert into rrobin_part'+str(partition_id) + ' (userid, movieid, rating) values ( '+str(userid)+', '+str(itemid)+', '+str(rating)+' )'
    cursor.execute(command)
    openconnection.commit()
    cursor.close()
    pass


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    max_rating = 5
    range_part_prefix = 'range_part%'
    command_rating_insert = 'insert into ' + ratingstablename + '(userid, movieid, rating) values ( '+str(userid)+', '+str(itemid)+', '+str(rating)+' )'
    command_get_range_table_count = "select count(*) from pg_tables where schemaname = 'public' and tablename like '"+range_part_prefix+"'"
    cursor = openconnection.cursor()
    cursor.execute(command_rating_insert)
    cursor.execute(command_get_range_table_count)
    numberofpartitions = cursor.fetchone()[0]
    rating_range = max_rating/numberofpartitions
    partition_id = (int)(rating/rating_range)
    if 0 != partition_id and 0 == (rating % rating_range):
        partition_id = partition_id - 1
    command_range_insert = 'insert into range_part'+str(partition_id) + '(userid, movieid, rating) values ( '+str(userid)+', '+str(itemid)+', '+str(rating)+' )'
    cursor.execute(command_range_insert)
    openconnection.commit()
    cursor.close()
    pass


def createDB(dbname='dds_assignment1'):
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
    cur.execute(
        'SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()


def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
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
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
