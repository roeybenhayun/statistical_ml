#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    
    print("In Load_Ratings Function")
    # added a b and c for the special charecter
    command = (
        """
        create table if not exists Ratings (
            UserID int,
            a char,
            MovieID int,
            b char,
            Rating numeric,
            c char,
            time int
        )
        """
    )
    query = str.replace(command,'Ratings', ratingstablename)
    try:
        cursor = openconnection.cursor()

        # Delete the table if exists
        cursor.execute("drop table if exists Ratings")
        #openconnection.commit()

        cursor.execute(query)
        #openconnection.commit()
        
        # no need for the commit since the autocommit is set to true
        #connection.commit()
        print("Table created successfully")

        f = open(ratingsfilepath,'r')
        cursor.copy_from(f,ratingstablename,sep=":")
        #openconnection.commit()

        # remove the unused columns from the table
        command = (""" alter table Ratings drop column a, drop column b, drop column c, drop column time """)
        query = str.replace(command,'Ratings', ratingstablename)
        cursor.execute(query)
        #openconnection.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        #if (openconnection):
        #    cursor.close()
        #    openconnection.close()
            print("********Load_Ratings Completed********")
            print("PostgresSQL Connection is close")


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    N = numberofpartitions
    connection = openconnection
    try:
        cursor = connection.cursor()
        table_list = []
        # create table list according to the partition size
        for n in range(0,N):            
            table_name = 'range_part'+str(n)
            #print (table_name)
            table_list.append(table_name)
        
        rating_range_list = []
        for n in range(0,N):
            rating_range = n * (5.0/N)
            #print(rating_range)
            rating_range_list.append(rating_range)

        #print(table_list)
        #print(rating_range_list)


        command = (
        """
        create table if not exists RangeParitionedTable (
            UserID int,     
            MovieID int,            
            Rating numeric                    
        )
        """)

        command2 = (
        """
        create table if not exists RangePartitionMetadata (
            Id int,            
            MinRatingInRange numeric,
            MaxRatingInRange numeric                    
        )
        """)

        command3 = (
            """ insert into RangePartitionMetadata (Id,MinRatingInRange,MaxRatingInRange) values(_Id,_MinRatingInRange,_MaxRatingInRange) """
            )

        # create the metadata table - use this table to store the range partition information
        
        cursor.execute(command2)

        for n in range(0,N):            
            table_name = table_list[n]            
            query = str.replace(command,'RangeParitionedTable', table_name)            
            cursor.execute(query)


        print("Executing command - start")
        
        command = (
        """
        insert into range_part0
        select userid,movieid,rating
        from Ratings
        where rating >=0.0 and rating <=5.0
        """)
        left_boundery = 0.0

        if (N==1):
            print("N=1")
            cursor.execute(command)
            insert_query = str.replace(command3,'_Id',str(0))
            insert_query = str.replace(insert_query,'_MinRatingInRange',str(0.0))
            insert_query = str.replace(insert_query,'_MaxRatingInRange',str(5.0))
            cursor.execute(insert_query)
        else:
            print("N > 1")
            id = 0
            print(rating_range_list)
            right_boundery = rating_range_list[1]
            query = str.replace(command,'5.0',str(right_boundery))
            
            #print(query)

            print("Left = ",left_boundery, " Right = ", right_boundery)
            insert_query = str.replace(command3,'_Id',str(id))
            insert_query = str.replace(insert_query,'_MinRatingInRange',str(left_boundery))
            insert_query = str.replace(insert_query,'_MaxRatingInRange',str(right_boundery))

            cursor.execute(query)
            cursor.execute(insert_query)

            
            query = str.replace(query,table_list[0],table_list[1]) 
            query = str.replace(query,'>=','>') 

            for n in range (2,N):
                print("Left = ",left_boundery, " Right = ", right_boundery)
                current_right_boundery = rating_range_list[n]
                query = str.replace(query,str(right_boundery), str(current_right_boundery))                
                query = str.replace(query,str(left_boundery),str(right_boundery))

                #print(query)

                cursor.execute(query)

                query = str.replace(query,table_list[n-1],table_list[n]) 
                left_boundery = right_boundery
                right_boundery = current_right_boundery

                insert_query = str.replace(command3,'_Id',str(n-1))
                insert_query = str.replace(insert_query,'_MinRatingInRange',str(left_boundery))
                insert_query = str.replace(insert_query,'_MaxRatingInRange',str(right_boundery))
                
                #print(insert_query)
                cursor.execute(insert_query)

            # the last partition
            query = str.replace(query,str(right_boundery), '5.0')
            query = str.replace(query,str(left_boundery), str(right_boundery))
            
            # update last entry
            left_boundery=right_boundery
            right_boundery = 5.0
            insert_query = str.replace(command3,'_Id',str(N-1))
            insert_query = str.replace(insert_query,'_MinRatingInRange',str(left_boundery))
            insert_query = str.replace(insert_query,'_MaxRatingInRange',str(right_boundery))
        
            cursor.execute(query)
            cursor.execute(insert_query)


    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (connection):
            #cursor.close()
            #connection.close()
            print("********Range_Partition Completed********")
            print("PostgresSQL Connection is close")


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    N = numberofpartitions
    connection = openconnection
    
    try:
        cursor = connection.cursor()
        
        table_list = []
        # create table list according to the partition size
        for n in range(0,N):            
            table_name = 'range_part'+str(n)
            #print (table_name)
            table_list.append(table_name)

        #print(table_list)
        command = (
        """
        create table if not exists RoundRobinParitionedTable (
            UserID int,     
            MovieID int,            
            Rating numeric                    
        )
        """
        )
        # Create the partitioned tables
        for n in range(0,N):            
            table_name = table_list[n]            
            query = str.replace(command,'RoundRobinParitionedTable', table_name)            
            cursor.execute(query)


        command1 = (
            """
            insert into range_part0
            select userid,movieid,rating
            from Ratings
            """
        )

        # use this table as a metadata table to store the next partition to write to 
        # 
        command2 = (
        """
        create table if not exists RoundRobinParitionMetadata (
        NumberOfPartitions int,
        NextPartitionToWrite int
        )
        """
        )
        # create the metadata table
        cursor.execute(command2)


        command3 = (""" insert into RoundRobinParitionMetadata (NumberOfPartitions,NextPartitionToWrite) values(_NumberOfPartitions,_NextPartitionToWrite) """)

        if (N==1):
            print("N=1")
            cursor.execute(command1)
            
            query = str.replace(command3,'_NumberOfPartitions', str(N))
            query = str.replace(query,'_NextPartitionToWrite', str(N))

            cursor.execute(query)
        else:
            print("N>1")

            # get the size of the table. not sure need it. 
            cursor.execute("select count(*) from Ratings")
            result = cursor.fetchone()
            table_size = result[0]


            command = (
            """
            insert into RoundRobinParitionedTable
            select userid,movieid,rating
            from Ratings
            limit 1
            offset _offset
            """
            )
            

            NextPartitionToWrite = 0
            for n in range(0,table_size):
                NextPartitionToWrite = n%N
                selected_table = table_list[n%N]
                query = str.replace(command,'RoundRobinParitionedTable', selected_table)
                query = str.replace(query,'_offset', str(n))
                #print(query)
                cursor.execute(query)

            # update the metadata table
            query = str.replace(command3,'_NumberOfPartitions', str(N))
            query = str.replace(query,'_NextPartitionToWrite', str((NextPartitionToWrite+1)%N))
            cursor.execute(query)


    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (connection):
            #cursor.close()
            #connection.close()
            print("********RoundRobin_Partition Completed********")
            print("PostgresSQL Connection is close")


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    
    user_id = userid
    movie_id = itemid
    connection = openconnection

    enable_execute = True
    print ("In Range_Insert Function")

    command = (""" insert into Ratings (UserID,MovieID,Rating) values(_UserID,_MovieID,_Rating) """)
    query = str.replace(command,'Ratings', ratingstablename)
    query = str.replace(query,'_UserID',str(user_id))
    query = str.replace(query,'_MovieID',str(movie_id))
    query = str.replace(query,'_Rating',str(rating))

    try:
        cursor = connection.cursor()
        # Update the rating table
        cursor.execute(query)


        # find to which parition we should insert the new record based on the rating
        # handle the following use cases:

        command1 = (""" select * from RangePartitionMetadata where rating > MinRatingInRange and rating < MaxRatingInRange""")
        query = str.replace(command1,'rating', str(rating))

        if enable_execute == True:
            selected_partition = 0
            MinRatingInRange = 0
            MaxRatingInRange = 0
            # Update the rating table
            cursor.execute(query)
            result = cursor.fetchone()
            if (result):
                print("Rating is between bounderies")
                Id = result[0]
                MinRatingInRange = result[1]
                MaxRatingInRange = result[2]
                selected_partition = Id

            else:
                command2 = (""" select * from RangePartitionMetadata where rating = MinRatingInRange""")
                query = str.replace(command2,'rating', str(rating))
                cursor.execute(query)
                result = cursor.fetchone()
                if (result):
                    print("Rating is on min boundery.  Save the record in the previous partition")
                    Id = result[0]
                    MinRatingInRange = result[1]
                    MaxRatingInRange = result[2]
                    selected_partition = Id -1

                    # save the rating in the previous partition
                else:
                    command3 = (""" select * from RangePartitionMetadata where rating = MaxRatingInRange""")
                    query = str.replace(command3,'rating', str(rating))
                    cursor.execute(query)
                    result = cursor.fetchone()
                    if (result):
                        print("Rating is on max boundery. Save the record in the current partition")
                        Id = result[0]
                        MinRatingInRange = result[1]
                        MaxRatingInRange = result[2]
                        selected_partition = Id
            

            # now update the partitioned tabled
            table='range_part'+str(selected_partition)
            query = str.replace(command,'Ratings', table)
            query = str.replace(query,'_UserID',str(user_id))
            query = str.replace(query,'_MovieID',str(movie_id))
            query = str.replace(query,'_Rating',str(rating))
            cursor.execute(query)
    
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)        

    finally:
        if (connection):
            #cursor.close()                
            #connection.close()
            print("PostgresSQL Connection is close")
            print("********Range_Insert Completed********")    

def createDB(dbname='dds_assignment'):
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
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

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
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()