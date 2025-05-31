import os
from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.appName("hello spark").master('local[2]').getOrCreate()

    data_list = [
        ('Nivu', 27),
        ('Chandan', 28),
        ('Sahitya', 0)
    ]

    df = spark.createDataFrame(data_list, ["Name", "Age"])
    df.printSchema()
    df.show()

    spark.stop()
    os._exit(0)  # Ensure the script exits cleanly
