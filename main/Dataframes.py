from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from common.readdatautil import ReadDataUtil



if __name__ == "__main__":
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName("Airline data mgat")\
        .getOrCreate()

    rdu = ReadDataUtil()

#airline Dataframe

    schema1 = StructType([StructField ("airline_id", IntegerType()),
                          StructField ("name",       StringType()),
                          StructField ("alias",      StringType()),
                          StructField ("iata",       StringType()),
                          StructField ("icao",       StringType()),
                          StructField ("callsign",   StringType()),
                          StructField ("country",    StringType()),
                          StructField ("active",     StringType())])



    # def1= spark.read.csv(r"C:\Users\Omkar's WIndows\Desktop\Data cloud\Spark\Project\Airline\airline*",schema=schema1,header=False)

    def1=rdu.readCsv(spark=spark,path=r"C:\Users\Omkar's WIndows\Desktop\Data cloud\Spark\Project\Airline\airline*",schema=schema1)


    airlinedf=def1
    # def1.printSchema()
    # def1.show(truncate=False)

# ----------------------------------------------------------------------------------------------------------------------------------------------------

#2 airport dataframe

    schema2 = StructType([StructField("airport_id", IntegerType()),
                          StructField ("name",      StringType()),
                          StructField ("city",      StringType()),
                          StructField ("country",   StringType()),
                          StructField ("iata",      StringType()),
                          StructField ("icao",      StringType()),
                          StructField ("latitude",  DecimalType(20,5)),
                          StructField ("longitude", DecimalType(20,5)),
                          StructField ("altitude",  IntegerType()),
                          StructField ("timezone",  IntegerType()),
                          StructField ("dst",       StringType()),
                          StructField ("tz",        StringType()),
                          StructField ("type",      StringType()),
                          StructField ("source",    StringType())])

    #
    def2= spark.read.csv(r"C:\Users\Omkar's WIndows\Desktop\Data cloud\Spark\Project\Airline\airport*",schema=schema2,header=False)

    airportdf=def2.cache()
    # def2.show()
    # def2.printSchema()
    # def2.show(truncate=True)

# -------------------------------------------------------------------------------------------------------------------------------------------
#3 plane dataframe

    schema3 = StructType([StructField ("Name",       StringType()),
                          StructField ("iata code",  StringType()),
                          StructField ("icao code",  StringType())])

    def3 = spark.read.csv(r"C:\Users\Omkar's WIndows\Desktop\Data cloud\Spark\Project\Airline\plane*",schema=schema3,sep="",
                          header=True)



    # def3.show(truncate=False)


    plandf=def3.cache()

    # def3.printSchema()
    # def3.show(truncate=False)

# --------------------------------------------------------------------------------------------------------------------------------------------
#4 route dataframe

    schema4 = StructType([StructField ("airline",          StringType()),
                          StructField ("airline_id",       IntegerType()),
                          StructField ("src_airport",      StringType()),
                          StructField ("src_airport_id",   IntegerType()),
                          StructField ("dest_airport",     StringType()),
                          StructField ("dest_airport_id",  IntegerType()),
                          StructField ("codeshare",        StringType()),
                          StructField ("stops",            IntegerType()),
                          StructField ("equipment",        StringType())])

    def4 = spark.read.parquet(r"C:\Users\Omkar's WIndows\Desktop\Data cloud\Spark\Project\Airline\routes.snappy*",schema=schema4 ,
                          header=False)


    routedf=def4.cache()
    # def4.printSchema()
    # routedf.show(truncate=False)

# -----------------------------------------------------------------------------------------------------------------------------------------------
# 'Trasformations'

    # 1. if any input file if you are getting \N or Null values in your column and that column is string type put defult ' \
    # value is "unkown" and if column type is integer then put -1

    df1=airlinedf.na.fill("unkown")
    #
    df=df1.select([col(c).cast("string") for c in df1.columns])
    #
    # # String Column
    # strdef=df.withColumn('iata', translate('iata', '\\N', 'Unkown')).withColumn('alias', translate('alias', '\\N', 'Unkown')).withColumn('callsign', translate('callsign', '\\N', 'Unkown')).withColumn('country', translate('country', '\\N', 'Unkown'))

    # strdef=df.withColumn('iata', regexp_replace('iata', '\\N', 'Unkown')).withColumn('alias', regexp_replace('alias', '\\N', 'Unkown')).withColumn('callsign', regexp_replace('callsign', '\\N', 'Unkown')).withColumn('country', regexp_replace('country', '\\N', 'Unkown'))

    # Integer Column
    # strdef.withColumn('airline_id', regexp_replace('airline_id', "unkown", "-1" )).withColumn('airline_id', regexp_replace('airline_id', '\\N', '-1')).show(truncate=False)


    # airlinedf.show()
    # # String Column
    # strdef=df.withColumn('alias',regexp_replace('alias','\\N','Unkown')).show()
    # df.withColumn("alias",regexp_replace("alias","\\N","unkown")).show()


    # # Integer Column
    # strdef.withColumn('airline_id', regexp_replace('airline_id', "unkown", "-1" )).withColumn('airline_id', regexp_replace('airline_id', '\\N', '-1')).show(truncate=False)
    # airlinedf.withColumn('iata', translate('iata', 'null', 'unkown')) \
    #     .show(truncate=False)
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
#     strdef = df.withColumn('Name', regexp_replace("Name", r"\\N", "unknown")).withColumn('Alias',regexp_replace("Alias", r"\\N","unknown")).withColumn(
#         'callsign', regexp_replace("callsign", r"\\N", "unknown")).withColumn('Country', regexp_replace("Country", r"\\N","unknown"))
#
#     df12 = strdef.withColumn('Name', regexp_replace("Name", r"\\N", "unknown")).withColumn('Alias',regexp_replace("Alias", r"\\N","unknown")).show()
# ---------------------------------------------------------------------------------------------------------------------------------------------------------

    # def streplace(column, value):
    #     return when(column != value, column).otherwise(lit("unkown"))

    # def intreplace(column, value)
    #     return when(column != value, column).otherwise(lit("-1"))
    #
    #
    # df = airlinedf.withColumn("alias", streplace(col("alias"), "\\N")).withColumn("callsign", streplace(col("callsign"), "\\N")).withColumn("country", streplace(col("country"), "null")).withColumn("iata", streplace(col("iata"), "null")).show()

    # airlinedf.withColumn("airline_id", intreplace(col("airline_id"), "-1")).show()
# ------------------------------------------------------------------------------------------------------------------------------------------------------------

    airlinedf.createOrReplaceTempView("airline")
    airportdf.createOrReplaceTempView("airport")
    routedf.createOrReplaceTempView("route")

    #
    # spark.sql("select a,* from  ," +
    #           "case, "+
    #           "when  name= is null or '\\N' then 'Unkown' " +
    #           "when  alias= is null or '\\N' then 'Unkown' " +
    #           "when  iata= is null or '\\N' then 'Unkown' " +
    #           "when  icao= is null or '\\N' then 'Unkown' " +
    #           "when  callsign= is null or '\\N' then 'Unkown' " +
    #           "when  country= is null or '\\N' then 'Unkown' " +
    #           "when  active= is null or '\\N' then 'Unkown"
    #           "End from airline a").show()
    #


    # spark.sql("select iata,case when  iata is null  then 'Unkown' End from airline").show()

    # -------------------------------------------------------------------------------------------------------------------------------------------------
     # '2. find the country name which is having both airlines and airport'
     # airlinedf.show()

    # airportdf.show()
    # joindf=airportdf.join(airlinedf, airportdf.country == airlinedf.country).show()
    # joindf.withColumn("Country in both airline & airport",col("country")).show()


    # joindf=airportdf.join(airlinedf, ["country"])
    # joindf.select(col("country").alias("The Country having both airline & airport")).distinct().show()

    # spark.sql("select distinct(ap.country) from airline ai, " +
    #           "airport ap " +
    #           "where ai.country=ap.country " +
    #           "order by ap.country").show()


# --------------------------------------------------------------------------------------------------------------------------------------------------
    # '3. get the airlines details like name, id,. which is has taken takeoff more than 3 times from same airport'
    # airlinedf.show()
    # airportdf.show()
    # routedf.show()
    #
    # windowSpecAgg = Window.partitionBy(col("src_airport"))
    # df12=routedf.withColumn("count", count(col("src_airport")).over(windowSpecAgg)).distinct().filter(col("count")>3)

    # routedf.groupBy(col("src_airport"),).count()
    # df12.show()
    # print(df12.count())
    #
    #
    # takeoffdf=airlinedf.join(routedf,on="airline_id",how='inner').groupBy("airline_id","name","src_airport")\
    # .agg(count("src_airport").alias("takeoff")).filter(col("takeoff")>3).orderBy(col("takeoff"))

    # print(takeoffdf.count())

    # 'By sql'
    # spark.sql("select ai.airline_id,src_airport,count(*)  from airline ai inner join route ro on ai.airline_id=ro.airline_id "+
    #           "group by ai.airline_id,src_airport having count(*)>3  order by src_airport ").show()

# ---------------------------------------------------------------------------------------------------------------------------------------------------
    # '4. get airport details which has minimum number of takeoffs and landing.'

    # windowSpecAgg = Window.partitionBy((col("src_airport"))).orderBy("airline_id")
    # takedf = routedf.withColumn("count", count(col("airline")).over(windowSpecAgg))

    # mintakedf = takedf.groupBy(col("src_airport")).min("count").distinct()
    # mintakedf1 = mintakedf.orderBy(col("min(count)").asc())

    # ranktake = mintakedf1.withColumn("rank", rank().over(Window.orderBy("min(count)")))
    # ranktake1 = ranktake.filter(col("rank") == 1)

    # ranktake1.show()

    # windowSpecAgg = Window.partitionBy((col("dest_airport"))).orderBy("airline_id")
    # landdf = routedf.withColumn("count", count(col("airline")).over(windowSpecAgg))

    # minlanddf = landdf.groupBy(col("dest_airport")).min("count").distinct()
    # minlanddf1 = minlanddf.orderBy(col("min(count)").asc())

    # rankland=minlanddf1.withColumn("rank",rank().over(Window.orderBy("min(count)")))
    # rankland1=rankland.filter(col("rank")==1)
    # # rankland2.show()
    # ranktake1.join(rankland1).show(10)

# ----------------------------------------------------------------------------------------------------------------------------------
#     takeoff
#     airportdf.show()
#     routedf.show()
#     takeoffdf = airportdf.join(routedf,airportdf.airport_id == routedf.src_airport_id,"inner").groupBy("airport_id","name","src_airport").count()
#
#     minimumtakeoff=takeoffdf.agg(min("count")).take(1)[0][0]
#     take1=takeoffdf.filter(col("count")==minimumtakeoff).show()
    #
#     # #landing

    # landdf = airportdf.join(routedf,airportdf.airport_id == routedf.src_airport_id,"inner").groupBy("airport_id","name","dest_airport").count()
    #
    # minimumlandoff=landdf.agg(min("count")).take(1)[0][0]
    # land1=landdf.filter(col("count")==minimumlandoff)
#
#     take1.join(land1).show()


    # takeoffdf.show()
   # 'By sql'

    # spark.sql("select airport_id,name,src_airport,dest_airport,count(src_airport)  takeoff ,count(dest_airport)  landing "+
    #           "from airport ap " +
    #           "inner join route ro on airport_id=src_airport_id " +
    #           "group by airport_id,name,src_airport,dest_airport " +
    #           "order by takeoff asc,landing asc").show()

# ------------------------------------------------------------------------------------------------------------------------------------------

    # '5. get airport details which is having maximum number of takeoff and landing.'
    # airlinedf.show()
    # airportdf.show()
    # routedf.show()

    # windowSpecAgg  = Window.partitionBy((col("src_airport"))).orderBy("src_airport")
    # takedf=routedf.withColumn("count", count(col("airline")).over(windowSpecAgg))
    #
    # maxtakedf=(takedf.groupBy(col("src_airport"))).max("count")
    # maxtakedf1=maxtakedf.orderBy(col("max(count)").desc())
    # # #
    # ranktake = maxtakedf1.withColumn("rank", rank().over(Window.orderBy(col("max(count)").desc())))
    # ranktake1 = ranktake.filter(col("rank") == 1)
    #
    # windowSpecAgg  = Window.partitionBy((col("dest_airport"))).orderBy("dest_airport")
    # landdf=routedf.withColumn("count", count(col("airline")).over(windowSpecAgg))
    #
    # windowSpecAgg1 = Window.partitionBy((col("dest_airport"))).orderBy(col("dest_airport").desc())
    # landdf.withColumn("row_num", row_number().over(windowSpecAgg1))
    #
    # maxlanddf=landdf.groupBy("dest_airport").max("count")
    # maxlanddf1=maxlanddf.orderBy(col("max(count)").desc())
    #
    # rankland = maxlanddf1.withColumn("rank", rank().over(Window.orderBy(col("max(count)").desc())))
    # rankland1 = rankland.filter(col("rank") == 1)
    # # # #
    # rankland1.join(ranktake1).show()
# ----------------------------------------------------------------------------------------------------------------------------------------------
#     takeoffdf = airportdf.join(routedf, airportdf.airport_id == routedf.src_airport_id, "inner").groupBy("airport_id","name","src_airport").count()
#
#     minimumtakeoff = takeoffdf.agg(max("count")).take(1)[0][0]
#     take1 = takeoffdf.filter(col("count") == minimumtakeoff)
#     #
#     # #landing
#
#     landdf = airportdf.join(routedf, airportdf.airport_id == routedf.src_airport_id, "inner").groupBy("airport_id","name","dest_airport").count()
#
#     minimumlandoff = landdf.agg(max("count")).take(1)[0][0]
#     land1 = landdf.filter(col("count") == minimumlandoff).show()
#
#     take1.join(land1,take1.src_airport==land1.dest_airport,"inner").show()


    spark.sql("select airport_id,name,src_airport,dest_airport,count(src_airport)  takeoff ,count(dest_airport)  landing "+
              "from airport ap " +
              "inner join route ro on airport_id=src_airport_id " +
              "group by airport_id,name,src_airport,dest_airport " +
              "order by takeoff desc,landing desc").show()
# -----------------------------------------------------------------------------------------------------------------------------------

    # 6. Get the airline details, which is having direct flights.
    # details like airline id, name, source airport name, and destination airport name

    # airlinedf.show()

    routedf.show()

    joindf=routedf.where(col("stops")==0)

    # df12=airlinedf.join(joindf,airlinedf.airline_id==joindf.airline_id,"inner").select("airlinedf.airline_id","src_airport","name","dest_airport").orderBy(col("name").asc()).show()
    # print(df12.count())

    df1=joindf.join(airlinedf,["airline_id"]).select("airline_id","name","dest_airport").orderBy(col("name").asc())
    # print(df1.count())

    # ## By SQL
    spark.sql("select ai.airline_id,Name,src_airport,dest_airport,stops " +
              "from airline ai " +
              "inner join route ro on ai.airline_id=ro.airline_id" +
              " where stops=0").show()

# -------------------------------------------------------------------------------------------------------------

















