from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
import pyspark
import os 

from config import spark, config

from helpers import load_data, save_dataframe

from lyrics_processor import run_transformation as run_lyrics_transformation
from artists_processor import run_transformation as run_artists_transformation

if __name__ == "__main__":
    
    # Prepare song lyrics from raw data
    raw_path = config.get("song_lyrics_raw_path")
    
    df_raw_data = load_data(raw_path)
    
    df_lyrics_transformed = run_lyrics_transformation(df_raw_data)
    
    print("Num of rows: ", df_lyrics_transformed.count())
    # save data
    trusted_path = config.get("song_lyrics_trusted_path")
    save_dataframe(
        df=df_lyrics_transformed,
        path=trusted_path,
        mode="overwrite"
    )
    
    df_artists_transformed = run_artists_transformation(df_lyrics_transformed)
    
    trusted_path = config.get("artists_trusted_path")
    save_dataframe(
        df=df_artists_transformed,
        path=trusted_path,
        mode="overwrite"
    )
    
    
    
    