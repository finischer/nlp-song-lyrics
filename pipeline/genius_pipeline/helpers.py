from config import spark
from pyspark.sql.functions import udf
from pyspark.sql import DataFrame
import uuid

def load_data(path: str):
    return spark.read.parquet(path)

def save_dataframe(df: DataFrame, path: str, mode: str = "append"):
    df.write.mode(mode).parquet(path)

@udf
def generate_uuid():
    return str(uuid.uuid4())