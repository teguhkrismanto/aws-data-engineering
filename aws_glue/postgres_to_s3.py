import sys
import pyspark.sql.functions as functions

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

source = glueContext.create_dynamic_frame.from_catalog(
    database="{GLUE_CATALOG_DATABASE}",
    table_name="{GLUE_CATALOG_TABLE}",
    transformation_ctx="source",
    additional_options = {
        "jobBookmarkKeys":["{TIMESTAMP_UPDATED_COLUMN}"],
        "jobBookmarkKeysSortOrder":"asc",
        "hashfield" : "{PRIMARY_KEY}", # mostly id
        "hashpartitions": "10"
    }
)

spark_df = source.toDF() # convert glue dynamicframe to spark dataframe

partition_df = (
    spark_df.withColumn('year', functions.year(functions.col('{TIMESTAMP_UPDATED_COLUMN}')))
    .withColumn('month', functions.month(functions.col('{TIMESTAMP_UPDATED_COLUMN}')))
    .withColumn('day', functions.dayofmonth(functions.col('{TIMESTAMP_UPDATED_COLUMN}')))
) # partition spark dataframe to year, month, day

glue_df = DynamicFrame.fromDF(partition_df, glueContext, "glue_df") # convert spark dataframe to glue dynamicframe

target = glueContext.write_dynamic_frame.from_options(
    frame=glue_df,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "{S3_BUCKET_PATH}",
        "partitionKeys": ["year", "month", "day"],
    },
    format_options={"compression": "gzip"},
    transformation_ctx="target",
)

job.commit()
