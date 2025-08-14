import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("Hello Spark") \
            .master('local[2]') \
            .getOrCreate()
        logging.info("Spark session created successfully.")

        fire_df = spark.read \
                    .format("csv") \
                    .option("header", True) \
                    .option("inferSchema", True) \
                    .load(r"C:\Users\chasurag\Documents\my repos\Fire_Department_and_Emergency_Medical_Services_Dispatched_Calls_for_Service_20250806.csv")

        logging.info("Data loaded from fire.csv successfully.")

        # Display schema and data
        fire_df.printSchema()
        # fire_df.show(5)
        print(fire_df.columns)

        # Count the number of records
        record_count = fire_df.count()
        logging.info("Number of records in fire data: %d", record_count)

        fire_df.createOrReplaceTempView("fire_data")
        logging.info("Temporary view 'fire_data' created.")
        # Example SQL query
        result_df = spark.sql("SELECT * FROM fire_data LIMIT 10")
        logging.info("Executed SQL query on 'fire_data'.")
        result_df.show()

    except Exception as e:
        logging.error("An error occurred: %s", e)
    finally:
        # Ensure Spark session is stopped
        if 'spark' in locals():
            spark.stop()
            logging.info("Spark session stopped.")

if __name__ == '__main__':
    main()
