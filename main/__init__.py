from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName("Airline data mgat")\
        .getOrCreate()