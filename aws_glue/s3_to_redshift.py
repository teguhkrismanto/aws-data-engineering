import sys
import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

now = datetime.datetime.now()
year = now.year

s3_path = "{S3_BUCKET_PATH}"

source = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [s3_path],
        "recurse": True,
    },
    transformation_ctx="source",
)

pre_query = "truncate {REDSHIFT_TARGET_STAGING_TABLE};"
post_query='''
    begin; 
    delete from {REDSHIFT_TARGET_TABLE} using {REDSHIFT_TARGET_STAGING_TABLE} where {REDSHIFT_TARGET_STAGING_TABLE}.{PRIMARY_KEY} = {REDSHIFT_TARGET_TABLE}.{PRIMARY_KEY}; 
    insert into {REDSHIFT_TARGET_TABLE} with latest_rows as (select {PRIMARY_KEY}, max({TIMESTAMP_UPDATED_COLUMN}) as latest_time from {REDSHIFT_TARGET_STAGING_TABLE} group by {PRIMARY_KEY}) select distinct o.* from {REDSHIFT_TARGET_STAGING_TABLE} o inner join latest_rows lr on o.{PRIMARY_KEY}=lr.{PRIMARY_KEY} where o.{TIMESTAMP_UPDATED_COLUMN}=lr.latest_time; 
    end;''' # deduplication query

target = glueContext.write_dynamic_frame.from_catalog(
    frame=source,
    database="{GLUE_CATALOG_DATABSE}",
    table_name="{GLUE_CATALOG_REDSHIFT_TARGET_STAGING_TABLE}",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="target",
    additional_options = {
        "extracopyoptions": "dateformat 'auto' timeformat 'auto' acceptanydate",
        "preactions":pre_query, 
        "postactions":post_query
    }
)

job.commit()
