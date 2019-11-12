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

def thread_function(unsorted_list, start_index, batch_size):
    # ok not under mutex. Each thread is working on different part of the unsorted array
    sublist = unsorted_list[start_index:start_index+batch_size]
    
    l.acquire()
    sublist_min = min(sublist)
    min_index = sublist.index(sublist_min)
    value_.append(sublist_min)
    index_.append(start_index*batch_size + min_index)
    l.release()

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
    command = (""" SELECT SortingColumnName from InputTable """)
    query = str.replace(command,'SortingColumnName', SortingColumnName)
    query = str.replace(query,'InputTable', InputTable)
    print(query)
    cursor.execute(query)
    rows = cursor.fetchall()

    for row in rows:
        val1 = row[0]
        #print (val1)
        unsorted_list.append(val1)

    orig_size = len(unsorted_list)
    batch_size = len(unsorted_list)/number_of_workers
    leftover = len(unsorted_list)%number_of_workers
    #print("Batch size = ",batch_size)
    #print("List size", len(unsorted_list))

    #exit
    sort = True
    threads = list()

    while(sort==True):
        # give the start index and size for each thread
        print("Start...")
        for index in range(0,number_of_workers):
            length = batch_size
            # the last thread should work on the rest of the list which is the batch size + leftover
            if (index == number_of_workers-1):
                length = batch_size+leftover  

            start_index = index * batch_size
            print("Start Index = ", start_index)
            print("Length= ", length)
            x = threading.Thread(target=thread_function, args=(unsorted_list, start_index, length))
            threads.append(x)
            x.start()
        
        for index, thread in enumerate(threads):
            thread.join()

        #print("Joined all threads. Do some post processing here...")
        #for i in range(0, len(index_)):
        #    print("Index = ", index_[i])
        #    print("Value = ", value_[i])
        
        # get mean value, get the index. delete this item from the list and save the tuple to file
        m_l = locate_min(value_)
        current_min = m_l[0]
        current_min_index_list = m_l[1]
        print("M-L = ",m_l)

        for i in range(0,len(current_min_index_list)):
            val = index_[current_min_index_list[i]]
            s1 = str(current_min)+ ','+str(val)
            f_out.write(s1)
            f_out.write('\n')
            # remove the items from the list
            print(" VAL = ", val)
            del unsorted_list[val]
            orig_size = orig_size - 1
            
        batch_size = len(unsorted_list)/number_of_workers
        leftover = len(unsorted_list)%number_of_workers


        print("Number of minimas = ", len(current_min_index_list))
        print("Batch Size = ", batch_size)
        print("Leftovers = ", leftover)
        print("Current size = ", orig_size)

        # loop break condition
        if (orig_size <= 5):
            print("BREAK LOOP.... size = ", orig_size)
            sort = False

        # get the tuple and delete the list
        #print(m_l)
        index_ = []
        value_ = []
        #time.sleep(3)

    f_out.close()
        

def locate_min(a):
    smallest = min(a)
    return smallest, [index for index, element in enumerate(a) 
                      if smallest == element]

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