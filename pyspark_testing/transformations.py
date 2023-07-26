from pyspark.sql import DataFrame

class Transformations:
    def __init__(self, spark):
        self.spark = spark

    def double_weight(self, df: DataFrame):
        result = df.withColumn("weight", df["weight"] * 2)
        return result

    def get_kelloggs(self, df: DataFrame):
        result = df.filter(df["mfr"] == "K")
        return result