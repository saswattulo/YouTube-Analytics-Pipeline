# Author: Saswat Tulo
# Date : 01/Feb/2025
# Description: This AWS Glue job is used to read the csv file from the data catalog of cleaned layer and write the data to the analytics layer in parquet format.


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Add the predicate pushdown to filter the data
predicate_pushdown = "region in ('ca','gb','us')"


# Script generated for node Amazon S3
AmazonS3_node1738436100804 = glueContext.create_dynamic_frame.from_catalog(
    database="de_youtube_raw",
    table_name="raw_statistics",
    transformation_ctx="AmazonS3_node1738436100804",
    push_down_predicate=predicate_pushdown,  # Add the predicate pushdown to filter the data
)

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(
    frame=AmazonS3_node1738436100804,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1738432238820",
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"},
)
AmazonS3_node1738436242539 = glueContext.write_dynamic_frame.from_options(
    frame=AmazonS3_node1738436100804,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://saswat-de-on-youtube-cleaned-us-east-1-dev/youtube/raw_statistics/",
        "partitionKeys": ["region"],  # Add the partition keys
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1738436242539",
)

job.commit()
