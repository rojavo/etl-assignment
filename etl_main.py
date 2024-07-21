from pyspark.sql import SparkSession
import sqlite3
import pandas as pd
import os
import os.path

from utils import *
from data_transform import *


if __name__ == "__main__":
    #define the paths
    data_path = 'data/'
    files = ['park-data.csv', 'squirrel-data.csv']
    condition_path = "auxiliary_files/condition_list.txt"
    animal_path = "auxiliary_files/animals_names.txt"
    activities_path = "auxiliary_files/verb_list.txt"
    
    
    #DATA ETRACT
    #create a sapark session and save it in DFs
    spark = SparkSession.builder.appName("ETL").getOrCreate()
    
    #import data from CSVs
    park_df = spark.read.option("header", "true").csv(data_path+files[0]) #data park df
    
    squirrel_df = spark.read.option("header", "true").csv(data_path+files[1]) #squirrel data

    #TRANSFORM THE DATA
    #Open the auxiliary files
    with open(condition_path, 'r') as file:
        lst_conditions = [line.strip() for line in file] #for Park Conditions
    with open(animal_path, 'r') as file:
        lst_animals = [line.strip() for line in file]   #for animal sightings
    with open(activities_path, 'r') as file:
        lst_activities = file.read().splitlines()       #for squirrel activities
    
    #transform the dataframes
    pdf_transformed = park_data_cleaner(park_df, spark, lst_conditions, lst_animals)
    sdf_transformed = squirrel_data_cleaner(squirrel_df, spark, lst_activities)
    
    
    #DATA LOAD
    #convert dataframes to pandas (so can be uploaded with sqlite3 functions)
    pdf_pandas = pdf_transformed.toPandas()
    sdf_pandas = sdf_transformed.toPandas()
    
    #open sqlite connection
    conn = sqlite3.connect('databases/database.db')  
    
    #upload dataframe to sqlite database
    pdf_pandas.to_sql('Park_Data', conn, if_exists='replace', index=False)
    sdf_pandas.to_sql('Squirrel_Data', conn, if_exists='replace', index=False)
    
    spark.stop() #close the connection to spark
    
    
    #PRINT DATA
    #load the queries from the TXTs
    q1 = load_query_from_file('queries/query1.txt')
    q2 = load_query_from_file('queries/query2.txt')
    q3 = load_query_from_file('queries/query3.txt')
    q4 = load_query_from_file('queries/query4.txt')
    q5 = load_query_from_file('queries/query5.txt')
    
    #print the queries
    print('RESULT 1:')
    print(pd.read_sql_query(q1, conn))
    print('RESULT 2:')
    print(pd.read_sql_query(q2, conn))
    print('RESULT 3:')
    print(pd.read_sql_query(q3, conn))
    print('RESULT 4:')
    print(pd.read_sql_query(q4, conn))
    print('RESULT 5:')
    print(pd.read_sql_query(q5, conn))

    conn.close() #close the connection to sql
    

