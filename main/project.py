from pyspark.sql import SparkSession
from common.readdatautil import ReadDataUtil


if __name__ == "__main__":
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName("Airline data mgat")\
        .getOrCreate()

    rdu=ReadDataUtil()

    df=rdu.readCsv(spark=spark,path=r"C:\Users\Omkar's WIndows\PycharmProjects\pythonProject2\spark\dept.csv",header=True,inferschema=True)

    df.show()
    df.printSchema()