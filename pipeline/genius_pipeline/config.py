from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import os

def init_spark_session() -> SparkSession:
    spark_config = {
        "spark.executor.instances": "8",
        "spark.executor.cores": "4",
        "spark.executor.memory": "16g",
        # "spark.sql.execution.arrow.enabled": True,
        # "spark.kryoserializer.buffer.max": "1536",
        "spark.debug.maxToStringFields": 100,
        "spark.sql.legacy.parquet.datetimeRebaseModeInRead": "CORRECTED",
        "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "CORRECTED",
        "spark.sql.legacy.parquet.int96RebaseModeInRead": "CORRECTED",
        "spark.sql.legacy.parquet.int96RebaseModeInWrite": "CORRECTED"
    }

    # initialize Spark
    conf = SparkConf().setAppName("nf_playground")
    for key, value in spark_config.items():
        conf.set(key, value)

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    
    return spark

config = {
    "song_lyrics_raw_path": os.path.join('hdfs://aiccluster','user','niklasfischer','data', 'genius_lyrics', 'raw'),
    "song_lyrics_trusted_path": os.path.join('hdfs://aiccluster','user','niklasfischer','data', 'genius_lyrics', 'trusted'),
    "artists_trusted_path": os.path.join('hdfs://aiccluster','user','niklasfischer','data', 'genius_artists', 'trusted')
}

spark = init_spark_session()