#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    print("****************Do Range Query - start******************")
    # get the meta table
    # store the ranges in a list
    # min and max 
    # 0 - 0-1
    # 1 - 1-2
    # 2 - 2-3
    # 3 - 3-4
    # 4 - 4-5
    # find the number of partitions
    # return the tuples above the min and below the max
    # partition name, userid, movieid,rating
    # first need to get the partitions
    # for all partition - if in range, save the id
    #
    range_rating_table='RangeRatingsPart'
    round_robin_table='RoundRobinRatingsPart'
    range__query_file_name='RangeQueryOut.txt'


    f_out = open(range__query_file_name, 'w')
    
    partition_list = []
    cursor = openconnection.cursor()
    cursor.execute("select * from rangeratingsmetadata")
    rows = cursor.fetchall()
    #cursor.copy_to(f_out, 'rangeratingspart3', sep=",")
    #cursor.copy_to(f_out, 'rangeratingspart2', sep=",")
    id = 0
    for row in rows:
        partition_id = row[0]
        partition_rating_min = row[1]
        partition_rating_max = row[2]

        if (id == 0):
            if (partition_rating_min <= ratingMinValue and ratingMinValue <= partition_rating_max):
                #print("val in range, save the partitions, id = ", partition_id)
                partition_list.append(partition_id)
        else:
            if (partition_rating_min < ratingMinValue and ratingMinValue <=  partition_rating_max):
                #print("val in range, save the partitions, id = ", partition_id)
                partition_list.append(partition_id)

        if (partition_rating_min < ratingMaxValue and ratingMaxValue <= partition_rating_max):
            #print("val in range, save the partitions, id = ", partition_id)
            partition_list.append(partition_id)
        
        id = id +1
    
    for partition in range(partition_list[0],(partition_list[1]+1)):
        print("In partition", partition)
        if partition == partition_list[0]:
            command = (""" select * from _rangeratingspart where rating >= _ratingMinValue""")
            query = str.replace(command,'_rangeratingspart', range_rating_table+str(partition))
            query = str.replace(query,'_ratingMinValue', str(ratingMinValue))
            print(query)
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                val1 = row[0]
                val2 = row[1]
                val3 = row[2]
                #print val1,val2, val3
                s1 = 'RangeRatingsPart'+ str(partition)+ ','+str(val1) + ',' + str(val2) + ',' + str(val3)
                f_out.write(s1)
                f_out.write('\n')
                print s1
                print("first partition")
        elif partition == partition_list[1]:
            command = (""" select * from _rangeratingspart where rating <= _ratingMaxValue""")
            query = str.replace(command,'_rangeratingspart', range_rating_table+str(partition))
            query = str.replace(query,'_ratingMaxValue', str(ratingMaxValue))
            print(query)
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                val1 = row[0]
                val2 = row[1]
                val3 = row[2]
                #print val1,val2, val3
                s1 = 'RangeRatingsPart'+ str(partition)+ ','+str(val1) + ',' + str(val2) + ',' + str(val3)
                f_out.write(s1)
                f_out.write('\n')
                print s1
            print ("last partition")
        else:
            command = (""" select * from _rangeratingspart""")
            query = str.replace(command,'_rangeratingspart', range_rating_table+str(partition))
            print(query)
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                val1 = row[0]
                val2 = row[1]
                val3 = row[2]
                #print val1,val2, val3
                s1 = 'RangeRatingsPart'+ str(partition)+ ','+str(val1) + ',' + str(val2) + ',' + str(val3)
                print s1
                f_out.write(s1)
                f_out.write('\n')
            print ("middle partitions")


    # round robin part
    rounf_robin_partition_list = []
    cursor = openconnection.cursor()
    cursor.execute("select * from roundrobinratingsmetadata")
    
    command = (""" select * from _roundrobinratingspart where rating >= _ratingMinValue and rating <=_ratingMaxValue""")
    query = str.replace(command,'_ratingMinValue', str(ratingMinValue))
    query = str.replace(query,'_ratingMaxValue', str(ratingMaxValue))

    rows = cursor.fetchall()
    for row in rows:
        val1 = row[0]
        val2 = row[1]        
        print ("*******->",val1,val2)

    for partition in range(0,val1):
        query = str.replace(query,'_roundrobinratingspart', 'roundrobinratingspart'+str(partition))
        print(query)
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            val1 = row[0]
            val2 = row[1]
            val3 = row[2]
            s1 = 'RoundRobinRatingsPart'+ str(partition)+ ','+str(val1) + ',' + str(val2) + ',' + str(val3)
            f_out.write(s1)
            f_out.write('\n')
            print (s1)
        
        query = str.replace(query,'roundrobinratingspart'+str(partition),'_roundrobinratingspart')

     
    f_out.close()

def PointQuery(ratingsTableName, ratingValue, openconnection):
    print("Do Point Query")
    
    point__query_file_name='PointQueryOut.txt'
    f_out = open(point__query_file_name, 'w')


    ratingMinValue = ratingValue
    ratingMaxValue = ratingValue
    partition_list = []
    cursor = openconnection.cursor()
    cursor.execute("select * from rangeratingsmetadata")
    rows = cursor.fetchall()

    id = 0
    for row in rows:
        partition_id = row[0]
        partition_rating_min = row[1]
        partition_rating_max = row[2]

        if (id == 0):
            if (partition_rating_min <= ratingMinValue and ratingMinValue <= partition_rating_max):
                #print("val in range, save the partitions, id = ", partition_id)
                partition_list.append(partition_id)
        else:
            if (partition_rating_min < ratingMinValue and ratingMinValue <=  partition_rating_max):
                #print("val in range, save the partitions, id = ", partition_id)
                partition_list.append(partition_id)

        if (partition_rating_min < ratingMaxValue and ratingMaxValue <= partition_rating_max):
            #print("val in range, save the partitions, id = ", partition_id)
            partition_list.append(partition_id)
        
        id = id +1

    print(partition_list[0])
    print(partition_list[1])

    command = (""" select * from _rangeratingspart where rating = _ratingValue""")
    query = str.replace(command,'_rangeratingspart', 'rangeratingspart'+str(partition_list[0]))
    query = str.replace(query,'_ratingValue', str(ratingMinValue))
    print(query)
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        val1 = row[0]
        val2 = row[1]
        val3 = row[2]
        s1 = 'RangeRatingsPart'+ str(partition_list[0])+ ','+str(val1) + ',' + str(val2) + ',' + str(val3)
        f_out.write(s1)
        f_out.write('\n')
        print s1
    
    
    command = (""" select * from _roundrobinratingspart where rating = _ratingValue""")
    query = str.replace(command,'_ratingValue', str(ratingValue))

    cursor.execute("select * from roundrobinratingsmetadata")
    rows = cursor.fetchall()
    for row in rows:
        val1 = row[0]
        val2 = row[1]        
        print ("*******->",val1,val2)

    for partition in range(0,val1):
        query = str.replace(query,'_roundrobinratingspart', 'roundrobinratingspart'+str(partition))
        print(query)
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            val1 = row[0]
            val2 = row[1]
            val3 = row[2]
            s1 = 'RoundRobinRatingsPart'+ str(partition)+ ','+str(val1) + ',' + str(val2) + ',' + str(val3)
            f_out.write(s1)
            f_out.write('\n')
            print (s1)
        
        query = str.replace(query,'roundrobinratingspart'+str(partition),'_roundrobinratingspart')


    f_out.close()
    
    
    # go all over all partitions and find the tuples with the ratingValue
    pass


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()