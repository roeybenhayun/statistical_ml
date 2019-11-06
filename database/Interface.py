
#!/usr/bin/python
import psycopg2
import sys


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
            rating_range = n * (1.0/N)
            #print(rating_range)
            rating_range_list.append(rating_range)

        print(table_list)
        print(rating_range_list)

        command = (
        """
        create table if not exists Ratings (
            UserID int,     
            MovieID int,            
            Rating numeric                    
        )
        """)

        for n in range(0,N):            
            table_name = table_list[n]            
            query = str.replace(command,'Ratings', table_name)
            print(query)
            cursor.execute(query)

        print("Executing command - start")
        command = (
        """
        insert into range_part0
        select userid,movieid,rating
        from Ratings
        where rating >=0 and rating <= 2.0
        """)


        cursor.execute(command)
        print("Executing command - end")
       
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgresSQL Connection is close")


def RoundRobin_Partition(table,N):
    print("In RoundRobin_Partition Function")

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
    #Range_Partition('Ratings',5, connection)
    

