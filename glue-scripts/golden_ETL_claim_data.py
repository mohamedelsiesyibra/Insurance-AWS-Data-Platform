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
AWSGlueDataCatalog_node1734344946396 = glueContext.create_dynamic_frame.from_catalog(database="insurelake_silver_db", table_name="claim", transformation_ctx="AWSGlueDataCatalog_node1734344946396")

# Script generated for node Change Schema
ChangeSchema_node1734344958459 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1734344946396, mappings=[("claimno", "long", "claimno", "long"), ("policyid", "long", "policyid", "long"), ("firstname", "string", "firstname", "string"), ("lastname", "string", "lastname", "string"), ("claimsreporteddate", "string", "claimsreporteddate", "string"), ("policyeffectivedate", "string", "policyeffectivedate", "string"), ("claimstatus", "string", "claimstatus", "string"), ("claimdescription", "string", "claimdescription", "string"), ("adjudicationstatus", "string", "adjudicationstatus", "string"), ("adjudicationdescription", "string", "adjudicationdescription", "string"), ("eobgenerated", "string", "eobgenerated", "string"), ("claimamount", "long", "claimamount", "long"), ("paidamount", "long", "paidamount", "long"), ("adjudicationdate", "string", "adjudicationdate", "string"), ("paymentdate", "string", "paymentdate", "string"), ("beneficiaryname", "string", "beneficiaryname", "string"), ("beneficiaryrelationship", "string", "beneficiaryrelationship", "string"), ("beneficiarypct", "long", "beneficiarypct", "long"), ("checkno", "long", "checkno", "long"), ("lastupdated", "string", "lastupdated", "string"), ("year", "string", "year", "string"), ("month", "string", "month", "string"), ("day", "string", "day", "string")], transformation_ctx="ChangeSchema_node1734344958459")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1734344958459, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734344519596", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1734344968062 = glueContext.getSink(path="s3://insurelake-golden-layer/claim-data/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["year", "month", "day"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1734344968062")
AmazonS3_node1734344968062.setCatalogInfo(catalogDatabase="insurelake_golden_db",catalogTableName="claim")
AmazonS3_node1734344968062.setFormat("glueparquet", compression="snappy")
AmazonS3_node1734344968062.writeFrame(ChangeSchema_node1734344958459)
job.commit()