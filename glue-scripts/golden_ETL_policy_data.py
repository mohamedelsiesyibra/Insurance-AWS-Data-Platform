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
AWSGlueDataCatalog_node1734345082447 = glueContext.create_dynamic_frame.from_catalog(database="insurelake_silver_db", table_name="policy", transformation_ctx="AWSGlueDataCatalog_node1734345082447")

# Script generated for node Change Schema
ChangeSchema_node1734345099651 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1734345082447, mappings=[("policyid", "long", "policyid", "long"), ("customerno", "long", "customerno", "long"), ("policyquotationdate", "string", "policyquotationdate", "string"), ("policyeffectivedate", "string", "policyeffectivedate", "string"), ("sex", "string", "sex", "string"), ("firstname", "string", "firstname", "string"), ("lastname", "string", "lastname", "string"), ("customerdob", "string", "customerdob", "string"), ("issuestate", "string", "issuestate", "string"), ("city", "string", "city", "string"), ("country", "string", "country", "string"), ("issueage", "long", "issueage", "long"), ("smokingclass", "string", "smokingclass", "string"), ("uwclass", "long", "uwclass", "long"), ("coverageunit", "long", "coverageunit", "long"), ("prodcode", "string", "prodcode", "string"), ("yearlypremium", "long", "yearlypremium", "long"), ("policystatus", "string", "policystatus", "string"), ("sumassured", "long", "sumassured", "long"), ("nationalid", "string", "nationalid", "string"), ("maritalstatus", "string", "maritalstatus", "string"), ("emailid", "string", "emailid", "string"), ("phoneno", "string", "phoneno", "string"), ("policyterm", "long", "policyterm", "long"), ("newrenew", "string", "newrenew", "string"), ("producer", "string", "producer", "string"), ("policyterminationdate", "string", "policyterminationdate", "string"), ("policyterminationreason", "string", "policyterminationreason", "string"), ("monthlypremium", "double", "monthlypremium", "double"), ("agentcommision", "double", "agentcommision", "double"), ("premiumpaidtilldate", "double", "premiumpaidtilldate", "double"), ("commisionpaidtilldate", "double", "commisionpaidtilldate", "double"), ("beneficiaryname", "string", "beneficiaryname", "string"), ("beneficiaryrelationship", "string", "beneficiaryrelationship", "string"), ("beneficiarypct", "long", "beneficiarypct", "long"), ("lastupdated", "string", "lastupdated", "string"), ("year", "string", "year", "string"), ("month", "string", "month", "string"), ("day", "string", "day", "string")], transformation_ctx="ChangeSchema_node1734345099651")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1734345099651, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734344519596", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1734345109286 = glueContext.getSink(path="s3://insurelake-golden-layer/policy-data/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["year", "month", "day"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1734345109286")
AmazonS3_node1734345109286.setCatalogInfo(catalogDatabase="insurelake_golden_db",catalogTableName="policy")
AmazonS3_node1734345109286.setFormat("glueparquet", compression="snappy")
AmazonS3_node1734345109286.writeFrame(ChangeSchema_node1734345099651)
job.commit()