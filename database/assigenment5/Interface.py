#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import logging
import time 
import threading
import warnings

l = threading.Lock()
index_ = []
value_ = []




def thread_function(unsorted_list, start_index, batch_size,column_index):
    
    sublist = unsorted_list[start_index:start_index+batch_size]    
    l.acquire()
    
    v_min = sublist[0][column_index]
    v_min_index = 0
    for i in range(1,len(sublist)):
        v = sublist[i][column_index]
        if(v < v_min):
            v_min = v
            v_min_index = i
    
    value_.append(sublist[v_min_index])
    index_.append(start_index+v_min_index)
    l.release()


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    print("Start Parallel Sort")
    global value_
    global index_

    
    save_to_file = True
    number_of_workers = 5
    unsorted_list = []

    if save_to_file == True:
        point__query_file_name='parallel_sort_results.txt'
        f_out = open(point__query_file_name, 'w')
    

    # get the size of the list to be sorted
    cursor = openconnection.cursor()

    command = (
        """
        create table if not exists OutputTable (
            userid int,
            movieid int,
            rating real
        )
        """
    )
    

    # get the table
    command2 = (""" SELECT * FROM InputTable LIMIT 0 """)
    command2 = str.replace(command2,'InputTable', InputTable)
    cursor.execute(command2)
    openconnection.commit()
    column_names = [desc[0] for desc in cursor.description]
    print("LEN = ", len(column_names))
    #raise ValueError(str(len(column_names)))
    print (column_names[0])
    print (column_names[1])
    print (column_names[2])
    
    
    create_table_query = str.replace(command,'OutputTable', OutputTable)    
    print(create_table_query)
    
    

    create_table_query = str.replace(create_table_query,'userid', column_names[0])
    create_table_query = str.replace(create_table_query,'movieid', column_names[1])
    create_table_query = str.replace(create_table_query,'rating', column_names[2])
    cursor.execute(create_table_query)

    
    openconnection.commit()


    insert_command = (""" insert into OutputTable (userid,movieid,rating) values(val1,val2,val3) """)
    insert_query = str.replace(insert_command,'OutputTable', OutputTable)
    insert_query = str.replace(insert_query,'userid', column_names[0])
    insert_query = str.replace(insert_query,'movieid', column_names[1])
    insert_query = str.replace(insert_query,'rating', column_names[2])



    column_id = 0
    for i in range (0,len(column_names)):
        if (column_names[i].lower() == SortingColumnName.lower()):
            #print SortingColumnName.lower()
            column_id = i
            break
    
    print SortingColumnName
    
    command = (""" SELECT * from InputTable """)
    query = str.replace(command,'InputTable', InputTable)
    print(query)
    cursor.execute(query)
    openconnection.commit()

    rows = cursor.fetchall()
    

    # all the rows
    unsorted_list = rows
    batch_size = len(unsorted_list)/number_of_workers
    leftover = len(unsorted_list)%number_of_workers

    sort = True
    threads = list()


    while(sort==True):
        for index in range(0,number_of_workers):
            length = batch_size
            
            if (index == number_of_workers-1):
                length = batch_size+leftover  

            start_index = index * batch_size                        
            x = threading.Thread(target=thread_function, args=(unsorted_list, start_index, length,column_id))
            threads.append(x)
            x.start()
        
        for index, thread in enumerate(threads):
            thread.join()

        min_value = value_[0][column_id]
        min_index = 0
        for i in range (1,number_of_workers):
            temp_min = value_[i][column_id]
            if temp_min < min_value:
                min_value = temp_min
                min_index = i

        if save_to_file == True:
            f_out.write(str(value_[min_index]))
            f_out.write('\n')

        insert_query_ = str.replace(insert_query,'val1', str(value_[min_index][0]))
        insert_query_ = str.replace(insert_query_,'val2', str(value_[min_index][1]))
        insert_query_ = str.replace(insert_query_,'val3', str(value_[min_index][2]))



        print(insert_query_)
        cursor.execute(insert_query_)
        openconnection.commit()

        delete_index = index_[min_index]
        del unsorted_list[delete_index]

        print("batch_size = ", batch_size)
        batch_size = len(unsorted_list)/number_of_workers
        leftover = len(unsorted_list)%number_of_workers

        last = []
        # loop break condition
        if (len(unsorted_list) <= 5):
            print unsorted_list
            print index_
            print("BREAK LOOP.... size = ", len(unsorted_list))
            print("LEFTOVER... size = ", leftover)
            print("COLUMNID... size = ", column_id)
            last = Sort(unsorted_list,column_id)
            print (last)
            sort = False
        
        for i in range(0,len(last)):
            insert_query_ = str.replace(insert_query,'val1', str(last[i][0]))
            insert_query_ = str.replace(insert_query_,'val2', str(last[i][1]))
            insert_query_ = str.replace(insert_query_,'val3', str(last[i][2]))
            cursor.execute(insert_query_)
            openconnection.commit()
            print(insert_query_)
            if save_to_file == True:
                f_out.write(str(last[i]))
                f_out.write('\n')

        index_ = []
        value_ = []
    
    if save_to_file == True:      
        f_out.close()
    
        
    command = (""" SELECT * from OutputTable where movieid = 3527 """)
    select_query_ = str.replace(command,'OutputTable', OutputTable)
    cursor.execute(select_query_)
    openconnection.commit()
    rows = cursor.fetchone()
    print(str(type(rows[0])))
    print(str(type(rows[1])))
    print(str(type(rows[2])))

    print rows


def Sort(sub_li,column):   
    # reverse = None (Sorts in Ascending order) 
    # key is set to sort using second element of  
    # sublist lambda has been used 
    return(sorted(sub_li, key = lambda x: x[column]))     




def join_thread_function(item, table2, start_index, batch_size,table1_join_column, table2_join_column):
    
    sublist1 = table2[start_index:start_index+batch_size]    
    l.acquire()
    #print "DDDDD", table1_join_column = 1, "GGGGGG", table2_join_column = 0
    # item - (17, 'Sense and Sensibility (1995)', 'Drama|Romance')
    # v = (10, 1015, 3.0) - 
    

    for i in range(0,len(sublist1)):
        v = sublist1[i][table1_join_column]
        #print item
        #print sublist1[i]
        if(item[table2_join_column] == v):
            if item[table2_join_column] == 480:
                print "FOUND MOVIE... "
                print "START INDEX = ", start_index, "OFFSET = ", i
            if item[table2_join_column] == 260:
                print "FOUND MOVIE 260... "
                print "START INDEX = ", start_index, "OFFSET = ", i
            #print "gggg", item
            #print "ffff : ", sublist1[i][table1_join_column]
            value_.append(sublist1[i])
            #print "START INDEX = ", start_index, "OFFSET = ", i
            index_.append(start_index+i)
    l.release()

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):

    print "In parallel join"
    global value_
    global index_

    save_to_file = False
    if save_to_file == True:
        point__query_file_name='debug.txt'
        f_out = open(point__query_file_name, 'w')

    #4, 480, 4.0, 480, 'Jurassic Park (1993)', 'Action|Adventure|Sci-Fi') 
    number_of_workers = 5


    cursor = openconnection.cursor()
    #


    # get the input table columns
    command = (""" SELECT * FROM InputTable LIMIT 0 """)
    command = str.replace(command,'InputTable', InputTable1)
    cursor.execute(command)
    openconnection.commit()
    column_names = [desc[0] for desc in cursor.description]
    print("LEN = ", len(column_names))
    print (column_names[0])
    print (column_names[1])
    print (column_names[2])

    command = str.replace(command,InputTable1, InputTable2)
    cursor.execute(command)
    openconnection.commit()
    column_names_ = [desc[0] for desc in cursor.description]
    print("LEN = ", len(column_names_))
    print (column_names_[0])
    print (column_names_[1])
    print (column_names_[2])

    # Create the output table
    command = (
        """
        create table if not exists OutputTable (
            userid int,
            movieid int,
            rating real,
            movieid1 int,
            title varchar(100),
            genre varchar(100)
        )
        """
    )

    command = str.replace(command,"OutputTable", OutputTable)
    cursor.execute(command)
    openconnection.commit()

    # build the insert command
    insert_command = (""" insert into OutputTable (userid,movieid,rating,movieid1,title,genre) values(val1,val2,val3,val4,val5,val6) """)
    insert_query = str.replace(insert_command,'OutputTable', OutputTable)

    column_id = 0
    for i in range (0,len(column_names)):
        if (column_names[i].lower() == Table1JoinColumn.lower()):
            #print SortingColumnName.lower()
            column_id = i
            break

    # get the column indexes
    Table1JoinColumnIndex = column_id

    column_id = 0
    for i in range (0,len(column_names)):
        if (column_names[i].lower() == Table2JoinColumn.lower()):
            #print SortingColumnName.lower()
            column_id = i
            break
    
    # get the column indexes
    Table2JoinColumnIndex = column_id

    # get the tabla data here
    command = (""" SELECT * FROM InputTable """)
    command = str.replace(command,"InputTable", InputTable1)
    cursor.execute(command)
    openconnection.commit()
    rows1 = cursor.fetchall()

    command = (""" SELECT * FROM InputTable """)
    command = str.replace(command,"InputTable", InputTable2)
    cursor.execute(command)
    openconnection.commit()
    rows2 = cursor.fetchall()

    # create output table here
    unsorted_list2 = rows2


    threads = list()
    unsorted_list1 = rows1
    batch_size = len(unsorted_list1)/number_of_workers
    leftover = len(unsorted_list1)%number_of_workers

    print "SIZE = ",batch_size
    print "LENGTH = ", leftover
    index_ = []
    value_ = []

    for i in range(0,len(unsorted_list2)):
        if (unsorted_list2[i][0] == 661):
            print("INDEX = ", i , "VALUES=", unsorted_list2[i])
#
    #for i in range(0,len(unsorted_list1)):
    #    if (unsorted_list1[i][1] == 480):
    #        print("INDEX = ", i , "VALUES=", unsorted_list1[i])

    #return
    deleted_tuples = 0

    temp_list = []
    for i in range(0,len(unsorted_list2)):

        #print "II = ", i
        new_tuple = unsorted_list2[i]

        if (new_tuple[0] == 480):
            print "THIS IS THE MOVIE"
            for i in range(0,len(unsorted_list1)):
                if (unsorted_list1[i][1] == 480):
                    print("INDEX = ", i , "VALUES=", unsorted_list1[i])

        for index in range(0,number_of_workers):
            length = batch_size
            
            if (index == number_of_workers-1):
                length = batch_size+leftover  

            start_index = index * batch_size
            
            str_ = "START= " + str(start_index) + " LENGTH =  " + str(length) + " LEFTOVER = " + str(leftover) +  " TOTAL LENGTH = " + str(len(unsorted_list1)) + " DELETE TUPLES = " + str(deleted_tuples)
            deleted_tuples = 0
            #print str_

            if save_to_file == True:
                f_out.write(str_)
                f_out.write('\n')

            x = threading.Thread(target=join_thread_function, args=(new_tuple,unsorted_list1, start_index, length,Table1JoinColumnIndex,Table2JoinColumnIndex))
            threads.append(x)
            x.start()
        

        for index, thread in enumerate(threads):            
            #print "WAIT FOR THREADS"
            thread.join()
            
        
        threads = list()

        if(len(value_)!=0):
                         
            for i in range(0,len(index_)):
                #if len(value_) == 1:
                #    print "LEN IS 1" 
                # insert here the the joined tabled
                #insert_query = str.replace(insert_command,'OutputTable', OutputTable)
                # VERY UGLY - no time need to submit. doing very quick and dirty
                insert_query_ = str.replace(insert_query,'val1',str(value_[i][0]))
                insert_query_ = str.replace(insert_query_,'val2',str(value_[i][1]))
                insert_query_ = str.replace(insert_query_,'val3',str(value_[i][2]))
                insert_query_ = str.replace(insert_query_,'val4',str(new_tuple[0]))
                new_tuple_str = "'"+ new_tuple[1] + "'"
                insert_query_ = str.replace(insert_query_,'val5',new_tuple_str)
                new_tuple_str = "'"+ new_tuple[2] + "'"
                insert_query_ = str.replace(insert_query_,'val6',new_tuple_str)

                cursor.execute(insert_query_)
                openconnection.commit()
                if unsorted_list1[index_[i]][1] == 480:
                    print unsorted_list1[index_[i]]
                    print "FOUND THE DELETED MOVIEW, in index ->", index_[i]
                    print "VALUE_ : ", value_
                    print "INDEX_ : ", index_[i]
                    #FOUND THE DELETED MOVIEW, in index -> 228
                    # note - 260 is the movie name
                    #VALUE_ :  [(1, 260, 4.0), (3, 260, 5.0), (4, 260, 5.0), (10, 260, 5.0)]
                    #INDEX_ :  [41, 191, 228, 873]
                #del unsorted_list1[index_[i]]
                #deleted_tuples = deleted_tuples + 1

        value_ = []
        index_ = []
        #batch_size = len(unsorted_list1)/number_of_workers
        #leftover = len(unsorted_list1)%number_of_workers
        

        if (len(unsorted_list1) <= 5):
            print "EXIT LOOP"
            value_ = []
            index_ = []
            break
            # leftover
        #print ("LIST1 LENGTH = ", len(unsorted_list1))
        #print ("LIST2 LENGTH = ",len(unsorted_list2))
        #print ("LIST2 INDEX = ", i)
        #del unsorted_list2[i]


    
    print "Outside Loop. No matches"
    print ("LIST1 LENGTH = ", len(unsorted_list1))
    print ("LIST2 LENGTH = ",len(unsorted_list2))

    # handle leftovers here




################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
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
    con.commit()
    con.close()