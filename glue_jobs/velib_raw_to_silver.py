import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging

from pyspark.sql.functions import *
from awsglue import DynamicFrame

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

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

# Read raw data from S3 and transform it
input_raw_data = f"s3://{s3_bucket_name}/velib-data-raw/*/"
df = spark.read.option("header", "true").csv(input_raw_data)
df_with_timestamp = df.withColumn("timestamp", to_timestamp(col("record_timestamp")))
df_with_dates = (
    df_with_timestamp.withColumn("year", year(col("timestamp")))
    .withColumn("month", month(col("timestamp")))
    .withColumn("day", dayofmonth(col("timestamp")))
    .withColumn("diff", datediff(current_date(), col("timestamp")))
    .where("diff <= 10 and diff >= 0")
)
dateTableddF = DynamicFrame.fromDF(df_with_dates, glueContext, "dateTableddF")

# Write transformation output in S3 and Glue
output_silver_data = f"s3://{s3_bucket_name}/velib-data-silver/"
dateTableddF = dateTableddF.drop_fields(["_c0", "record_timestamp", "diff"])
S3bucket_Sink = glueContext.getSink(
    path=output_silver_data,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["year", "month", "day"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_Sink",
)
S3bucket_Sink.setCatalogInfo(
    catalogDatabase=glue_database_name, catalogTableName=f"velib_data_silver"
)
S3bucket_Sink.setFormat("glueparquet")
S3bucket_Sink.writeFrame(dateTableddF)
job.commit()
