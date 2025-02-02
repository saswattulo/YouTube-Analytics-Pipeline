# Author: Saswat Tulo
# Date : 01/Feb/2025
# Description: This AWS Glue job is used to read the JSON file from the raw layer and write the data to the cleansed layer in parquet format.



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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1738438789562 = glueContext.create_dynamic_frame.from_catalog(database="de_youtube_cleaned", table_name="raw_statistics", transformation_ctx="AWSGlueDataCatalog_node1738438789562")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1738439231124 = glueContext.create_dynamic_frame.from_catalog(database="de_youtube_raw", table_name="cleaned_statistics_reference_data", transformation_ctx="AWSGlueDataCatalog_node1738439231124")

# Script generated for node Join
Join_node1738439172113 = Join.apply(frame1=AWSGlueDataCatalog_node1738438789562, frame2=AWSGlueDataCatalog_node1738439231124, keys1=["category_id"], keys2=["id"], transformation_ctx="Join_node1738439172113")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1738439172113, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1738438738502", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1738439462751 = glueContext.getSink(path="s3://saswat-de-on-youtube-analytics-us-east-1-dev", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["region", "category_id"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1738439462751")
AmazonS3_node1738439462751.setCatalogInfo(catalogDatabase="db_youtube_analytics",catalogTableName="final_youtube_analytics")
AmazonS3_node1738439462751.setFormat("glueparquet", compression="snappy")
AmazonS3_node1738439462751.writeFrame(Join_node1738439172113)
job.commit()