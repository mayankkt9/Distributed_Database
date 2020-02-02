import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cursor = openconnection.cursor()
    create_rating_table_command = 'create table if not exists '+ ratingstablename + ' (userid int, movieid int, rating float)'
    cursor.execute(create_rating_table_command)
    file = open(ratingsfilepath,'r')
    for line in file.readlines():
        get_data = line.split('::')
        cursor.execute('insert into '+ ratingstablename + '(userid, movieid, rating) values ( '+get_data[0]+', '+get_data[1]+', '+get_data[2]+' )')
        print('insert into '+ ratingstablename + '(userid, movieid, rating) values ( '+get_data[0]+' '+get_data[1]+' '+get_data[2]+' )')

    openconnection.commit()
    cursor.close()
    pass

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor()
    max_rating = 5
    rating_range = 5/numberofpartitions
    for i in range(0,numberofpartitions):
        start = i * rating_range
        end = start + rating_range
        if i==0:
            cursor.execute('create table range_part'+str(i)+' as select * from '+ratingstablename+' where rating>=0 and rating<='+str(end))
        else:
            cursor.execute('create table range_part'+str(i)+' as select * from '+ratingstablename+' where rating>'+str(start)+' and rating<='+str(end))
    openconnection.commit()
    cursor.close()
    pass


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor()
    for i in range(0,numberofpartitions):
        command = 'create table rrobin_part'+str(i)+' as select * from (select ROW_NUMBER() OVER() as row_number, *  from '+ratingstablename+') as V where mod(V.row_number,'+str(numberofpartitions)+') = '+(str((i+1)%numberofpartitions))
        cursor.execute(command)
    openconnection.commit()
    cursor.close()
    pass


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()
    cursor.execute('insert into '+ ratingstablename + '(userid, movieid, rating) values ( '+str(userid)+', '+str(itemid)+', '+str(rating)+' )')
    cursor.execute('select count(*) from '+ratingstablename)
    row_number = (cursor.fetchall())[0][0]
    rrobin_part = 'rrobin_part%'
    cursor.execute("select count(*) from pg_stat_user_tables where relname like '"+rrobin_part+"'")
    numberofpartitions = cursor.fetchone()[0]
    partition_id = (row_number-1)%numberofpartitions
    cursor.execute('insert into rrobin_part'+str(partition_id) + '(userid, movieid, rating) values ( '+str(userid)+', '+str(itemid)+', '+str(rating)+' )')
    openconnection.commit()
    cursor.close()
    pass


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()
    max_rating = 5
    range_part = 'range_part%'
    cursor.execute("select count(*) from pg_stat_user_tables where relname like '"+range_part+"'")
    numberofpartitions = cursor.fetchone()[0]
    rating_range = 5/numberofpartitions
    partition_id = (int)(rating/rating_range)
    if partition_id!=0 and rating%rating_range==0:
        partition_id = partition_id - 1
    cursor.execute('insert into range_part'+str(partition_id) + '(userid, movieid, rating) values ( '+str(userid)+', '+str(itemid)+', '+str(rating)+' )')
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
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
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
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
