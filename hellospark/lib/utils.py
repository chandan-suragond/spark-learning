import configparser
from pyspark.sql import *
from pyspark import SparkConf
import os

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    conf_path = os.path.join(os.path.dirname(__file__), '..', 'spark.conf')
    conf_path = os.path.abspath(conf_path)
    print("Reading config from:", conf_path)
    config.read(conf_path)

    # print("Sections found:", config.sections())
    if 'SPARK_APP_CONFIGS' not in config.sections():
        raise Exception("Section [SPARK_APP_CONFIGS] not found in spark.conf")

    for (key, value) in config.items('SPARK_APP_CONFIGS'):
        spark_conf.set(key, value)

    return spark_conf

def create_spark_session():
    conf = get_spark_app_config()
    return SparkSession.builder \
        .config(conf=conf) \
        .config(
        "spark.driver.extraJavaOptions",
        "-Dlog4j.configurationFile=file:/C:/Users/chasurag/Documents/my%20repos/hellospark/log4j2.properties") \
        .getOrCreate()

def load_survey_df(spark):
    return spark.read \
        .option("inferSchema", True) \
        .option("header", True) \
        .csv(r"C:\Users\chasurag\Documents\my repos\hellospark\data\sample.csv")

def count_by_country(survey_df):
    return survey_df.where("Age < 40") \
        .select("Age", "Gender", "Country", "State") \
        .groupBy('Country') \
        .count() \
        .orderBy('count')