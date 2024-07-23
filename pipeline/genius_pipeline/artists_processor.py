from pyspark.sql import DataFrame

def run_transformation(df_lyrics: DataFrame):
    df_artists = df_lyrics.select("artist_id", "features")
    
    # filter empty strings elements in array out
    df_filtered = df_artists.where(~F.array_contains(F.col("features"), ""))
    
    # add feature_name
    df_joined = df_filtered.join(df_lyrics, on="artist_id", how="inner")
    
    # combine every feature of one artist in one array
    df_artists_featuers = (df_joined
     .groupBy("artist_id", "artist_name", "num_songs")
     .agg(F.flatten(F.array_distinct(F.collect_list("features"))).alias("features"))
    )
    
    # add size of features array
    df_artists_featuers = df_artists_featuers.withColumn("num_features", F.size(F.col("features")))
    
    
    df_artists_featuers_final = (df_artists_featuers
                                 .withColumn("feature_name", F.explode("features"))
                                 .withColumn("feature_name", F.array_join(F.split("feature_name", ";"), " ")) # F.replace only available in spark 3.5.0 or higher
                                )
    
    
    print("Num of artists: ", df_artists_featuers_final.count())
    return df_artists_featuers_final
    
    
    
    