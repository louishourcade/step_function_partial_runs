import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import logging

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    """Funciton to run SQL queries on Glue dataframes"""
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


# Set logger
MSG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
DATETIME_FORMAT = "%y/%m/%d %H:%M:%S"
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT, level=logging.INFO)
logger = logging.getLogger(__name__)

# Get parameters
args = getResolvedOptions(sys.argv, ["S3Bucket", "GlueDatabase"])
s3_bucket_name = args["S3Bucket"]
glue_database_name = args["GlueDatabase"]

logger.info(f"S3 bucket name: {s3_bucket_name}")
logger.info(f"Glue database name: {glue_database_name}")

# Read data from Glue data catalog
data_velib_silver_ddf = glueContext.create_dynamic_frame.from_catalog(
    database=glue_database_name,
    table_name="velib_data_silver",
    transformation_ctx="data_velib_silverddf",
)

# Run SQL query on the input table
SqlQuery = """
SELECT stationcode
, name
, year
, month
, day
, (cast (numbikesavailable as int) * 100 / cast(capacity as int)) as availability_percentage
FROM data_velib_silver
"""
result_query_ddf = sparkSqlQuery(
    glueContext,
    query=SqlQuery,
    mapping={"data_velib_silver": data_velib_silver_ddf},
    transformation_ctx="result_query_ddf",
)

# Write transformation output in S3 and in the Glue database
output_velib_avail_percent_gold = f"s3://{s3_bucket_name}/velib-avail-percent-gold"
S3bucket_Sink = glueContext.getSink(
    path=output_velib_avail_percent_gold,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["year", "month", "day"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_Sink",
)
S3bucket_Sink.setCatalogInfo(
    catalogDatabase=glue_database_name, catalogTableName=f"velib_avail_percent_gold"
)
S3bucket_Sink.setFormat("glueparquet")
S3bucket_Sink.writeFrame(result_query_ddf)
job.commit()
