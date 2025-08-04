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

        # Sample data
        people_data = [
            ('Nivu', 27),
            ('Chandan', 28),
            ('Sahitya', 0)
        ]

        # Create DataFrame
        people_df = spark.createDataFrame(people_data, ["Name", "Age"])
        logging.info("DataFrame created successfully.")

        # Display schema and data
        people_df.printSchema()
        people_df.show()

    except Exception as e:
        logging.error("An error occurred: %s", e)
    finally:
        # Ensure Spark session is stopped
        if 'spark' in locals():
            spark.stop()
            logging.info("Spark session stopped.")

if __name__ == '__main__':
    main()
