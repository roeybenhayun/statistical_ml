
#!/usr/bin/python
import psycopg2
import sys


enable_execute = False
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
            MinRatingInRange numeric
            MaxRatingInRange numeric                    
        )
        """)

        command3 = (""" insert into RangePartitionMetadata (Id,MinRatingInRange,MaxRatingInRange),values(_Id,_MinRatingInRange,_MaxRatingInRange) """)

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
                cursor.execute(command)

        

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
            where rating >=0.0 and rating <=5.0
            """
        )

        if (N==1):
            print("N=1")
            if enable_execute == True:
                cursor.execute(command1)
        else:
            print("N>1")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (connection):
            #cursor.close()
            connection.close()
            print("PostgresSQL Connection is close")

    

def RoundRobin_Insert(table,user_id,movie_id,rating):
    print("In RoundRobin_Insert Function")

def Range_Insert(table,user_id,movie_id,rating):
    print ("In Range_Insert Function")

def Delete_Partitions():
    print ("In Delete_Partitions")

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
        cursor.execute("alter table Ratings drop column a, drop column b, drop column c")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgresSQL Connection is close")

if __name__ == '__main__':
    connection = Get_Connection()
    #Load_Ratings("ml-10M100K/ratings.dat", connection)
    #Range_Partition('Ratings',10, connection)

    RoundRobin_Partition('Ratings',10,connection)

    #1::539::5::838984068
    #Range_Insert('Ratings',uid,mid,rating)

    
   
