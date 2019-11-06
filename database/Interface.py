
#!/usr/bin/python
import psycopg2
import sys
# format: UserID::MovieID::Rating::Timestamp



def Load_Ratings(path_to_dataset):
    print("In Load_Ratings Function")

def Range_Partition(table,N):
    print("In Range_Partition Function")

def RoundRobin_Partition(table,N):
    print("In RoundRobin_Partition Function")

def RoundRobin_Insert(table,user_id,movie_id,rating):
    print("In RoundRobin_Insert Function")

def Range_Insert(table,user_id,movie_id,rating):
    print ("In Range_Insert Function")

def Delete_Partitions():
    print ("In Delete_Partitions")


def Create_Tables():

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
    connection = psycopg2.connect(user = "roeybenhayun",
                                  password = "",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "postgres")
    connection.autocommit=True
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

        f = open('ml-10M100K/ratings.dat','r')
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
    Create_Tables()
    Load_Ratings("ml-10M100K/ratings.dat")
