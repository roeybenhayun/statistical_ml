
#!/usr/bin/python
import psycopg2
import sys


enable_execute = True
enable_prints = True
enable_info_prints = False



def Range_Partition(table,N, connection):            
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
        if enable_execute == True:
            cursor.execute(command2)

        for n in range(0,N):            
            table_name = table_list[n]            
            query = str.replace(command,'RangeParitionedTable', table_name)
            #print(query)
            if enable_execute == True:
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
            if enable_execute == True:
                cursor.execute(command)
                insert_query = str.replace(command3,'_Id',0)
                insert_query = str.replace(insert_query,'_MinRatingInRange',str(0.0))
                insert_query = str.replace(insert_query,'_MaxRatingInRange',str(5.0))
                cursor.execute(insert_query)
        else:
            print("N > 1")
            id = 0
            print(rating_range_list)
            right_boundery = rating_range_list[1]
            query = str.replace(command,'5.0',str(right_boundery))
            if enable_info_prints==True:
                print(query)

            print("Left = ",left_boundery, " Right = ", right_boundery)
            insert_query = str.replace(command3,'_Id',str(id))
            insert_query = str.replace(insert_query,'_MinRatingInRange',str(left_boundery))
            insert_query = str.replace(insert_query,'_MaxRatingInRange',str(right_boundery))

            if enable_execute == True:
                cursor.execute(query)
                cursor.execute(insert_query)

            
            query = str.replace(query,table_list[0],table_list[1]) 
            query = str.replace(query,'>=','>') 

            for n in range (2,N):
                print("Left = ",left_boundery, " Right = ", right_boundery)
                current_right_boundery = rating_range_list[n]
                query = str.replace(query,str(right_boundery), str(current_right_boundery))                
                query = str.replace(query,str(left_boundery),str(right_boundery))

                if enable_info_prints == True:
                    print(query)

                if enable_execute == True:
                    cursor.execute(query)
                query = str.replace(query,table_list[n-1],table_list[n]) 
                left_boundery = right_boundery
                right_boundery = current_right_boundery

                insert_query = str.replace(command3,'_Id',str(n-1))
                insert_query = str.replace(insert_query,'_MinRatingInRange',str(left_boundery))
                insert_query = str.replace(insert_query,'_MaxRatingInRange',str(right_boundery))
                
                if enable_info_prints == True:
                    print(insert_query)

                if enable_execute == True:
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
        
            if enable_execute == True:    
                cursor.execute(query)
                cursor.execute(insert_query)


    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (connection):
            #cursor.close()
            connection.close()
            print("PostgresSQL Connection is close")


def RoundRobin_Partition(table,N,connection):
    try:
        cursor = connection.cursor()
        
        table_list = []
        # create table list according to the partition size
        for n in range(0,N):            
            table_name = 'range_part'+str(n)
            #print (table_name)
            table_list.append(table_name)

        print(table_list)
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
            print(query)

        if enable_execute == True:
                cursor.execute(query)

        command1 = (
            """
            insert into range_part0
            select userid,movieid,rating
            from Ratings
            """
        )

        #/* use this for the round robin case to split the data*/
        #SELECT * FROM Ratings limit 5 offset 5

        #/*use this for the round robin partition - save this in a metadata table?*/ 
        #select count(*) from Ratings

        # N - is the number of tables.
        # need to get the size of the table - lets assume the size table_size       

        # table_size = 10000054
        #table_size = 1054
        table_size = 12


        # use this table as a metadata table to store the next partition to write to 
        # 
        command2 = (
        """
        create table if not exists RoundRobinParitionMetadata (
        NumberOfPartitions int
        NextPartitionToWrite int
        )
        """
        )
        # create the metadata table
        if enable_execute == True:
            cursor.execute(command2)


        command3 = (""" insert into RoundRobinParitionMetadata (NumberOfPartitions,NextPartitionToWrite),values(_NumberOfPartitions,_NextPartitionToWrite) """)

        if (N==1):
            print("N=1")
            if enable_execute == True:
                cursor.execute(command1)
            
            query = str.replace(command3,'_NumberOfPartitions', str(N))
            query = str.replace(query,'_NextPartitionToWrite', str(N))

            if enable_execute == True:
                cursor.execute(query)
        else:
            print("N>1")

            # get the size of the table. not sure need it. 
            if enable_execute == True:            
                cursor.execute("select count(*) from Ratings")
                result = cursor.fetchone()
                table_size = result['count']


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
                print(query)
                if enable_execute == True:
                    cursor.execute(query)

            # update the metadata table
            query = str.replace(command3,'_NumberOfPartitions', str(N))
            query = str.replace(query,'_NextPartitionToWrite', str((NextPartitionToWrite+1)%N))
            if enable_execute == True:
                    cursor.execute(query)

            print(query)

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (connection):
            #cursor.close()
            connection.close()
            print("PostgresSQL Connection is close")

    

def RoundRobin_Insert(table,user_id,movie_id,rating):
    print("In RoundRobin_Insert Function")

    # need to check the table string ?
    # first insert to the rating table
    #     
    command = (""" insert into Ratings, (UserID,MovieID,Rating),values(_UserID,_MovieID,_Rating) """)
    query = str.replace(command,'Ratings', table)
    query = str.replace(query,'_UserID',str(user_id))
    query = str.replace(query,'_MovieID',str(movie_id))
    query = str.replace(query,'_Rating',str(rating))

    print(query)

    try:
        cursor = connection.cursor()
        
        if enable_execute == True:
            cursor.execute(query)

        NextPartitionToWrite = 0
        NumberOfPartitions = 1
        # get the round robin metadata table
        command = (""" select * from RoundRobinParitionMetadata""")

        if enable_execute == True:
            cursor.execute(command)
            result = cursor.fetchone()
            NumberOfPartitions = result['NumberOfPartitions']
            NextPartitionToWrite = result['NextPartitionToWrite']

        command = (""" insert into range_partX (UserID,MovieID,Rating),values(_UserID,_MovieID,_Rating,) """)

        query = str.replace(command,'range_partX', ('range_part'+str(NextPartitionToWrite)))
        query = str.replace(query,'_UserID',str(user_id))
        query = str.replace(query,'_MovieID',str(movie_id))
        query = str.replace(query,'_Rating',str(rating))

        if enable_execute == True:
            cursor.execute(query)

        # update the next partitionto write
        NextPartitionToWrite = (NextPartitionToWrite+1)%NumberOfPartitions
        
        # update the metadata table
        command3 = (""" insert into RoundRobinParitionMetadata (NumberOfPartitions,NextPartitionToWrite),values(_NumberOfPartitions,_NextPartitionToWrite) """)
        query = str.replace(command3,'_NumberOfPartitions', str(NumberOfPartitions))
        query = str.replace(query,'_NextPartitionToWrite', str(NextPartitionToWrite))

        if enable_execute == True:
            cursor.execute(query)


    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    
    finally:
        if (connection):
            if enable_execute == True:
                cursor.close()                
            connection.close()
            print("PostgresSQL Connection is close") 
    
    

def Range_Insert(table,user_id,movie_id,rating):
    print ("In Range_Insert Function")

    command = (""" insert into Ratings, (UserID,MovieID,Rating),values(_UserID,_MovieID,_Rating) """)
    query = str.replace(command,'Ratings', table)
    query = str.replace(query,'_UserID',str(user_id))
    query = str.replace(query,'_MovieID',str(movie_id))
    query = str.replace(query,'_Rating',str(rating))

    try:
        cursor = connection.cursor()
        if enable_execute == True:
            # Update the rating table
            cursor.execute(query)


        # find to which parition we should insert the new record based on the rating
        # handle the following use cases:

        command1 = (""" select * from RangePartitionMetadata, where rating > MinRatingInRange and rating < MaxRatingInRange""")
        query = str.replace(command1,'rating', str(rating))

        if enable_execute == True:
            # Update the rating table
            cursor.execute(query)
            result = cursor.fetchone()
            if (result):
                print("Do somthing here")
                range_id1 = result['Id']
                range_id2 = result['Id']
            else:
                command2 = (""" select * from RangePartitionMetadata, where rating = MinRatingInRange""")
                query = str.replace(command2,'rating', str(rating))
                cursor.execute(query)
                result = cursor.fetchone()
                if (result):
                    print("Do somthing here")
                    range_id1 = result['Id']
                    range_id2 = result['Id']
                else:
                    command3 = (""" select * from RangePartitionMetadata, where rating = MaxRatingInRange""")
                    query = str.replace(command3,'rating', str(rating))
                    cursor.execute(query)
                    result = cursor.fetchone()
                    if (result):
                        print("Do somthing here")



    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)        

    finally:
        if (connection):
            if enable_execute == True:
                cursor.close()                
            connection.close()
            print("PostgresSQL Connection is close") 


def Delete_Partitions(connection,delete_all,delete_ratings,delete_partitions, delete_metadata):

    try:
        cursor = connection.cursor()
        cursor.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_schema,table_name")
        rows = cursor.fetchall()

        if (delete_all==True):
                for row in rows:
                    print "dropping table: ", row[1]
                    command = (""" drop table if exists _table cascade""")
                    query = str.replace(command,'_table', row[1])
                    cursor.execute(query)
        else:
            if(delete_ratings == True):
                print("Dropping Ratings table")
                command = (""" drop table if exists _table cascade""")
                query = str.replace(command,'_table', "Ratings")
                cursor.execute(query)
            if (delete_partitions == True):
                print("Dropping Partitions tables")
                command = (""" drop table if exists _partitionTable cascade""")
                for n in range(0,100):
                    table = "range_part" + str(n)
                    query = str.replace(command,'_partitionTable', table)
                    cursor.execute(query)
            if (delete_metadata == True):
                command = (""" drop table if exists RangePartitionMetadata cascade""")
                cursor.execute(command)
                command = (""" drop table if exists RoundRobinParitionMetadata cascade""")
                cursor.execute(command)
                print("Dropping Metadata tables")

    
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (connection):
            cursor.close()                
            connection.close()
            print("PostgresSQL Connection is close") 



def Get_Connection():
    connection = psycopg2.connect(user = "roeybenhayun",
                                  password = "",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "postgres")
    connection.autocommit=True
    return connection


def Load_Ratings(path_to_dataset, connection):

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
    try:
        cursor = connection.cursor()
        #print ( connection.get_dsn_parameters(),"\n")
        #cursor.execute("SELECT version();")
        #record = cursor.fetchone()
        #print("You are connected to - ", record,"\n")


        # Delete the table if exists
        cursor.execute("drop table if exists Ratings")
        cursor.execute(command)
        
        # no need for the commit since the autocommit is set to true
        #connection.commit()
        print("Table created successfully")

        f = open(path_to_dataset,'r')
        cursor.copy_from(f,'Ratings',sep=":")
        #connection.commit()

        # remove the unused columns from the table
        cursor.execute("alter table Ratings drop column a, drop column b, drop column c, drop column time")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgresSQL Connection is close")

if __name__ == '__main__':
    connection = Get_Connection()
    
    # Delete all tables
    Delete_Partitions(connection,True,False,False,False)

    connection = Get_Connection()
    Load_Ratings("ml-10M100K/ratings_small.dat", connection)

    connection = Get_Connection()
    Range_Partition('Ratings',10, connection)

    #RoundRobin_Partition('Ratings',10,connection)

    #RoundRobin_Insert('Ratings',1,539,3)

    #1::539::5::838984068
    #Range_Insert('Ratings',uid,mid,rating)

    
