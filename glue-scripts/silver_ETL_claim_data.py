import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import EntityDetector
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *

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
AWSGlueDataCatalog_node1734344525629 = glueContext.create_dynamic_frame.from_catalog(database="insurelake_bronze_db", table_name="bronze_claim_data", transformation_ctx="AWSGlueDataCatalog_node1734344525629")

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(AWSGlueDataCatalog_node1734344525629, ["PERSON_NAME", "EMAIL", "IP_ADDRESS", "PHONE_NUMBER", "USA_SSN"], 1.0, 0.1, "HIGH")

def maskDf(df, keys):
    if not keys:
        return df
    df_to_mask = df.toDF()
    for key in keys:
        df_to_mask = df_to_mask.withColumn(key, lit("*******"))
    return DynamicFrame.fromDF(df_to_mask, glueContext, "updated_masked_df")

DetectSensitiveData_node1734344571052 = maskDf(AWSGlueDataCatalog_node1734344525629, list(classified_map.keys()))

# Script generated for node Change Schema
ChangeSchema_node1734344615714 = ApplyMapping.apply(frame=DetectSensitiveData_node1734344571052, mappings=[("claimno", "long", "claimno", "long"), ("policyid", "long", "policyid", "long"), ("firstname", "string", "firstname", "string"), ("lastname", "string", "lastname", "string"), ("claimsreporteddate", "string", "claimsreporteddate", "string"), ("policyeffectivedate", "string", "policyeffectivedate", "string"), ("claimstatus", "string", "claimstatus", "string"), ("claimdescription", "string", "claimdescription", "string"), ("adjudicationstatus", "string", "adjudicationstatus", "string"), ("adjudicationdescription", "string", "adjudicationdescription", "string"), ("eobgenerated", "string", "eobgenerated", "string"), ("claimamount", "long", "claimamount", "long"), ("paidamount", "long", "paidamount", "long"), ("adjudicationdate", "string", "adjudicationdate", "string"), ("paymentdate", "string", "paymentdate", "string"), ("beneficiaryname", "string", "beneficiaryname", "string"), ("beneficiaryrelationship", "string", "beneficiaryrelationship", "string"), ("beneficiarypct", "long", "beneficiarypct", "long"), ("checkno", "long", "checkno", "long"), ("lastupdated", "string", "lastupdated", "string"), ("partition_0", "string", "year", "string"), ("partition_1", "string", "month", "string"), ("partition_2", "string", "day", "string")], transformation_ctx="ChangeSchema_node1734344615714")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1734344615714, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734344519596", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1734344644431 = glueContext.getSink(path="s3://insurelake-silver-layer/claim-data/", connection_type="s3", updateBehavior="LOG", partitionKeys=["year", "month", "day"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1734344644431")
AmazonS3_node1734344644431.setCatalogInfo(catalogDatabase="insurelake_silver_db",catalogTableName="claim")
AmazonS3_node1734344644431.setFormat("glueparquet", compression="snappy")
AmazonS3_node1734344644431.writeFrame(ChangeSchema_node1734344615714)
job.commit()