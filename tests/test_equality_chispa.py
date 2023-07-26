from pyspark_testing.transformations import Transformations
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
#from chispa.column_comparer import assert_column_equality
import pytest
import os

@pytest.fixture
def spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()

@pytest.fixture
def resource_path() -> str:
    return os.getcwd() + '/tests/resource_dir'

def test_double_weight(resource_path, spark):
    transformations = Transformations(spark)
    
    df = spark.read.csv(f'file://{resource_path}/cereal.csv', header='true')
    # excepted_result = spark.read.csv(f'file://{resource_path}/double.csv', header='true')
    excepted_result = spark.read.parquet(f'file://{resource_path}/double.parquet')
    result = transformations.double_weight(df)
    assert_df_equality(result, excepted_result, ignore_row_order=True)