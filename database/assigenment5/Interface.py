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



#l = threading.Lock()
#index_ = []
#value_ = []


def join_thread_function(item, table2, start_index, batch_size,table1_join_column, table2_join_column):
    
    sublist1 = table2[start_index:start_index+batch_size]    
    l.acquire()
    
    for i in range(0,len(sublist)):
        v = sublist[i][column_index]
        if(item == v):
            v_index = i
            value_.append(sublist[v_index])
            index_.append(start_index+v_index)

    l.release()

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):

    print "In parallel join"
    global value_
    global index_

    number_of_workers = 5
    unsorted_list = []

    cursor = openconnection.cursor()
    #
    list1_ = []
    list2_ = []

    # get the table
    command = (""" SELECT * FROM InputTable LIMIT 0 """)
    command = str.replace(command,'InputTable', InputTable1)
    cursor.execute(command)
    openconnection.commit()
    column_names = [desc[0] for desc in cursor.description]
    print("LEN = ", len(column_names))
    #raise ValueError(str(len(column_names)))
    print (column_names[0])
    print (column_names[1])
    print (column_names[2])

    command = str.replace(command,InputTable1, InputTable2)
    cursor.execute(command)
    openconnection.commit()
    column_names = [desc[0] for desc in cursor.description]
    print("LEN = ", len(column_names))
    #raise ValueError(str(len(column_names)))
    print (column_names[0])
    print (column_names[1])
    print (column_names[2])

    command = (""" SELECT * FROM InputTable """)
    command = str.replace(command,"InputTable", InputTable1)
    cursor.execute(command)
    openconnection.commit()
    rows1 = cursor.fetchall()
    print(rows1)

    command = (""" SELECT * FROM InputTable """)
    command = str.replace(command,"InputTable", InputTable2)
    cursor.execute(command)
    openconnection.commit()
    rows2 = cursor.fetchall()
    print(rows2)


    unsorted_list2 = rows2
    batch_size = len(unsorted_list)/number_of_workers
    leftover = len(unsorted_list)%number_of_workers


    unsorted_list1 = rows1

    for i in range(0,len(unsorted_list1)):un

        new_tuple = unsorted_list1[i]

        for index in range(0,number_of_workers):
            length = batch_size
            
            if (index == number_of_workers-1):
                length = batch_size+leftover  

            start_index = index * batch_size                        
            x = threading.Thread(target=join_thread_function, args=(new_tuple,unsorted_list1, start_index, length,Table1JoinColumn,Table1JoinColumn))
            threads.append(x)
            x.start()
        

    for index, thread in enumerate(threads):
        thread.join()


    # post processing here
    






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