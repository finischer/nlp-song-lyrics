from pyspark.sql import SQLContext, SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
import pyspark
import os 

from helpers import generate_uuid

# config stuff
START_YEAR = 1960
END_YEAR = 2024
NUM_SONGS_MIN = 15

def clean_features(df: DataFrame):
    df_cleaned_features = (
        df
        # remove '{' and '}'
        .withColumn("features", F.regexp_replace(F.col("features"), r"^\{|\}$", ""))
        # remove '"'
        .withColumn("features", F.regexp_replace(F.col("features"), r'"', ""))
        # split features to cast as array of strings
        .withColumn("features", F.split("features", ","))
    )
    
    return df_cleaned_features


def pre_filter(df: DataFrame):
    selected_cols = ['id', 'title', 'artist', 'year', 'tag', 'language' , 'views', 'features', 'lyrics']
    
    # filter year between 1960 and 2024
    df_filtered = df.select(*selected_cols).where(( F.col("year") > START_YEAR) & ( F.col("year") <= END_YEAR ))
    
    # filter genre 'misc' out
    df_filtered = df_filtered.where(F.col("tag") != "misc")
    
    # drop missing values in lyrics and genre
    df_filtered = df_filtered.dropna(subset=["lyrics", "tag"])
    
    print(f"Num of rows after filter: {df_filtered.count()}")
    
    return df_filtered

def rename_columns(df: DataFrame):
    df_renamed = (
        df
        # tag to genre
        .withColumnRenamed("tag", "genre")
        # artist to artist_name
        .withColumnRenamed("artist", "artist_name")
    )
    
    return df_renamed

def cast_columns(df: DataFrame):
    # cast views to int
    df_casted = df.withColumn("views", F.col("views").cast("int"))
    return df_casted

def add_artist_id(df: DataFrame):
    # add artist id column
    df_artists = df.select("artist_name").distinct()
    df_artists = df_artists.withColumn("artist_id", generate_uuid())
    
    df_lyrics = df.join(df_artists, on="artist_name", how="inner")
    
    return df_lyrics

def post_filter(df: DataFrame):
     # only consider lyrics of artists who has more than 15 lyrics in genius
    df_grouped = df.groupBy("artist_id", "artist_name").agg(F.count("*").alias("num_songs"))
    df_grouped = df_grouped.where(F.col("num_songs") >= NUM_SONGS_MIN)
    
    print(f"Artists who has at least {NUM_SONGS_MIN} songs: {df_grouped.count()}")
    
    df_filtered = df.join(df_grouped, on=["artist_id", "artist_name"], how="inner")
    
    return df_filtered

def run_transformation(df_raw: DataFrame):
    print("Num rows before transformation: ", df_raw.count())
    
    df = pre_filter(df_raw)
    df = rename_columns(df)
    df = cast_columns(df) 
    
    # add decade to dataframe
    df = df.withColumn("decade", (F.floor(F.col("year") / 10)) * 10)
    
    df = add_artist_id(df)
    df = post_filter(df)
    
    print("Num rows after transformation: ", df.count())
    
    df = clean_features(df)
    
    return df
    
    