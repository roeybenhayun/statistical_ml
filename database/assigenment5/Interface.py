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

l = threading.Lock()
index_ = []
value_ = []
temp_list = []
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


def foo_function(unsorted_list, start_index, batch_size):
    print "IN FOO"
    print "Start Index = ", start_index
    print "Batch Size = ", batch_size
    sublist = unsorted_list[start_index:start_index+10]

    #for i in range(0,10):
    #    print sublist[i][0]
    
    #print (sublist[:][:])
    #sublist_min = min(sublist)
    v_min = sublist[0][2]
    v_min_index = 0
    for i in range(1,len(sublist)):
        v = sublist[i][2]
        if(v < v_min):
            v_min = v
            v_min_index = i
    
    print "MIN = ", v_min
    print "V_MIN Index = ", v_min_index
    print "TUPLE = ", sublist[v_min_index]



# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    global value_
    global index_
    print("Start Parallel Sort")

    point__query_file_name='parallel_sort_results.txt'
    f_out = open(point__query_file_name, 'w')
    
    number_of_workers = 5
    unsorted_list = []
    # get the size of the list to be sorted
    cursor = openconnection.cursor()
    #command = (""" SELECT SortingColumnName from InputTable """)
    #query = str.replace(command,'SortingColumnName', SortingColumnName)
    # get the table
    #cursor.execute("SELECT * FROM ratings LIMIT 0")
    #colnames = [desc[0] for desc in cursor.description]
    #print colnames
    #print SortingColumnName
    #return
    command = (""" SELECT * from InputTable """)
    query = str.replace(command,'InputTable', InputTable)
    print(query)
    cursor.execute(query)
    rows = cursor.fetchall()

    #print "ROWSTYPE = ",type(rows), "SIZE = ", len(rows)
    #return
    #for row in rows:
    #    val1 = row[0]
        #print (val1)
    #    unsorted_list.append(val1)

    # all the rows
    unsorted_list = rows
    orig_size = len(unsorted_list)
    batch_size = len(unsorted_list)/number_of_workers
    leftover = len(unsorted_list)%number_of_workers
    #print("Batch size = ",batch_size)
    #print("List size", len(unsorted_list))

    # Just to figure out how to sort nested list
    #foo_function(unsorted_list,0,batch_size)




    #return
    #exit
    sort = True
    threads = list()

    while(sort==True):
        # give the start index and size for each thread
        #print("Start...")
        for index in range(0,number_of_workers):
            length = batch_size
            # the last thread should work on the rest of the list which is the batch size + leftover
            if (index == number_of_workers-1):
                length = batch_size+leftover  

            start_index = index * batch_size
            #print("Start Index = ", start_index)
            #print("Length= ", length)
            x = threading.Thread(target=thread_function, args=(unsorted_list, start_index, length,2))
            threads.append(x)
            x.start()
        
        for index, thread in enumerate(threads):
            thread.join()

        # get mean value, get the index. delete this item from the list and save the tuple to file
        
        #for i in range(0,number_of_workers):
        #print(value_)
        #print(index_)

        min_value = value_[0][2]
        min_index = 0
        for i in range (1,number_of_workers):
            temp_min = value_[i][2]
            if temp_min < min_value:
                min_value = temp_min
                min_index = i

        #print value_[min_index]
        f_out.write(str(value_[min_index]))
        f_out.write('\n')


        
        delete_index = index_[min_index]
        del unsorted_list[delete_index]

        print("batch_size = ", batch_size)
        batch_size = len(unsorted_list)/number_of_workers
        leftover = len(unsorted_list)%number_of_workers


        # loop break condition
        if (len(unsorted_list) <= 5):
            print unsorted_list
            print index_
            print("BREAK LOOP.... size = ", len(unsorted_list))
            print("LEFTOVER... size = ", leftover)
            last = Sort(unsorted_list,2)
            print (last)
            sort = False

        # get the tuple and delete the list
        #print(m_l)
        index_ = []
        value_ = []
        #time.sleep(3)

    f_out.close()
        



def Sort(sub_li,column):   
    # reverse = None (Sorts in Ascending order) 
    # key is set to sort using second element of  
    # sublist lambda has been used 
    return(sorted(sub_li, key = lambda x: x[column]))     
  




def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    pass # Remove this once you are done with implementation


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