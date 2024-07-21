#import libraries
from pyspark.sql.functions import col, to_timestamp, when, to_date, udf
from pyspark.sql.types import StringType, TimestampType, IntegerType

from utils import *

def park_data_cleaner(park_df, spark, lst_conditions, lst_animals):
    #create a copy of the original dataset
    df_copy = park_df.alias("df_copy")
    
    #Convert the "Park ID" column into float (is float cause uses "13.1" and "13.2")
    df_copy= df_copy.withColumn("Park ID", col("Park ID").cast("float"))
    
    #Convert the date to datetime
    df_copy = df_copy.withColumn("Date", to_date(df_copy["Date"],"M/d/yy"))
    
    #convert the time in minutes to integer
    df_copy = df_copy.withColumn("Total Time (in minutes, if available)", col("Total Time (in minutes, if available)").cast("integer"))
    
    #Transform the park conditions by using a list of adjectives extracted with nlp (spacy)
    broadcast_lst_conditions = spark.sparkContext.broadcast(lst_conditions)     #brodcast the file with the conditions extracted
    def condition_clean_udf(value):                     #wrap the function "condition cleaner"
        lst_conditions = broadcast_lst_conditions.value
        return condition_clean(value, lst_conditions)
    condition_clean_udf_pyspark = udf(condition_clean_udf, StringType())    #ruen the condition into UDF
    df_copy = df_copy.withColumn("Park Conditions", condition_clean_udf_pyspark(df_copy['Park Conditions']))
    
    
    #Transforma the Other Animal Sightings by comparing the animals to an external list of them
    broadcast_lst_animals = spark.sparkContext.broadcast(lst_animals)     
    def animal_clean_udf(value):                     #wrap the function 
        lst_animals = broadcast_lst_animals.value
        return animal_clean(value, lst_animals)
    animal_clean_udf_pyspark = udf(animal_clean_udf, StringType())
    df_copy = df_copy.withColumn("Other Animal Sightings", animal_clean_udf_pyspark(df_copy['Other Animal Sightings']))
    
    #Transform the Litter amount by using (none, some, abundant)
    litter_clean_udf = udf(litter_clean, StringType())
    df_copy = df_copy.withColumn("Litter", litter_clean_udf(df_copy['Litter']))
    
    #extract the temprature from Temperature & Weather columns
    extract_temp_udf = udf(extract_temp, IntegerType())
    df_copy = df_copy.withColumn("Temperature", extract_temp_udf(df_copy['Temperature & Weather']))
    
    #extract the Weather from Temperature & Weather columns by comparing with the mos used weather terms
    extract_weather_udf = udf(extract_weather, StringType())
    df_copy = df_copy.withColumn("Weather", extract_weather_udf(df_copy['Temperature & Weather']))
    
    return df_copy
    
def squirrel_data_cleaner(squirrel_df, spark, activities_lst):
    df_copy = squirrel_df.alias("df_copy")
    
    #Convert the "Park ID" column into float
    df_copy= df_copy.withColumn("Park ID", col("Park ID").cast("float"))
    
    broadcast_lst_activities = spark.sparkContext.broadcast(activities_lst)     
    def convert_activities_udf(value):                     #wrap the function 
        activities_lst = broadcast_lst_activities.value
        return convert_activities2(value, activities_lst)
    convert_activities_udf_pyspark = udf(convert_activities_udf, StringType())
    df_copy = df_copy.withColumn("Activities", convert_activities_udf_pyspark(df_copy['Activities']))
    
    return df_copy