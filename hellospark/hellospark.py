from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import create_spark_session, load_survey_df, count_by_country

if __name__ == "__main__":
    
    spark = create_spark_session()

    logger = Log4J(spark)

    logger.info("Starting HelloSpark")

    survey_df = load_survey_df(spark)
    
    partitioned_df = survey_df.repartition(2)
    count_df = count_by_country(partitioned_df)

    logger.info(count_df.collect())

    input("Press Enter")
    logger.info("Finished HelloSpark")
    # spark.stop()