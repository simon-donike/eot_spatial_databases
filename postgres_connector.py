#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb  7 20:10:25 2021

@author: simondonike
"""

def db_connector(table):
    #table options: exit_points, marketeers, footpaths, utilities
    import psycopg2
    return_ls = []
    try:
       connection = psycopg2.connect(user="bot_annakirmes",
                                      password="XXXXXXXXXXXXXXXXX",
                                      host="85.214.150.208",
                                      port="5432",
                                      database="annakirmes")
       cursor = connection.cursor()
       postgreSQL_select_Query = "select * from "+table
    
       cursor.execute(postgreSQL_select_Query)
       #print("Selecting rows from mobile table using cursor.fetchall")
       mobile_records = cursor.fetchall() 
       
       #print("Print each row and it's columns values")
       for row in mobile_records:
           return_ls.append(list(row))
    
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data from PostgreSQL: ", error)
    """
    finally:
        #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            #print("PostgreSQL connection is closed")
    """
    return return_ls

ls = db_connector("marketeers")
