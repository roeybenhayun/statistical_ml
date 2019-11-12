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

def thread_function(unsorted_list, start_index, batch_size):

    l.acquire()
    # sleep on some shared variable some shared variable
    print("In thread function: Start index = ", start_index, "Batch size = ", batch_size)
    time.sleep(2)
    l.release()
    # notify

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    print("Start Parallel Sort")
    
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

    batch_size = len(unsorted_list)/number_of_workers
    leftover = len(unsorted_list)%number_of_workers
    print("Batch size = ",batch_size)
    print("List size", len(unsorted_list))

    exit

    threads = list()

    # give the start index and size for each thread
    for index in range(0,number_of_workers):
        length = batch_size
        # the last thread should work on the rest of the list which is the batch size + leftover
        if (index == number_of_workers-1):
            length = batch_size+leftover  

        start_index = index * batch_size
        x = threading.Thread(target=thread_function, args=(unsorted_list, start_index, length))
        threads.append(x)
        x.start()
    
    for index, thread in enumerate(threads):
        #print("Main: before joining thread .", index)
        thread.join()
        #print("Main: thread done", index)
    

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