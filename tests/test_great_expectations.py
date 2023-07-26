from pyspark.sql import SparkSession
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
import pytest
import os
from pyspark_testing.transformations import Transformations

@pytest.fixture
def spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()

@pytest.fixture
def resource_path() -> str:
    return os.getcwd() + '/tests/resource_dir'

def test_weight_not_null(resource_path, spark):
    df = spark.read.csv(f'file://{resource_path}/cereal.csv', header='true')
    test_df = SparkDFDataset(df)
    assert test_df.expect_column_values_to_not_be_null("weight").success

def test_name_is_unique(resource_path, spark):
    df = spark.read.csv(f'file://{resource_path}/cereal.csv', header='true')
    test_df = SparkDFDataset(df)
    assert test_df.expect_column_values_to_be_unique("name").success

def test_mfr_is_kelloggs(resource_path, spark):
    transformations = Transformations(spark)
    df = spark.read.csv(f'file://{resource_path}/cereal.csv', header='true')
    filtered_df = transformations.get_kelloggs(df)
    test_df = SparkDFDataset(filtered_df)
    assert test_df.expect_column_values_to_be_in_set("mfr", ["K"]).success