import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME','database','s3_staging_path','job_connection'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_staging_path = args['s3_staging_path']
database = args['database']
job_connection = args['job_connection']

dbtable_attachments = "strike_off_objection_attachment"
dbtable_objections = "strike_off_objection" 

# Dont Use Bookmarks with this ETL as aws does not upsert instead we have to delete * from table first
preactions_attachments = "delete from strike_off_objection_attachment;"
preactions_objections = "delete from strike_off_objection;"

# TIP - use myDynamicFrame.printSchema() and .show() to log out schema and contents of the dynamic frame
    
# TIP - If you need to perform sql-like queries on the data you can convert the DynamicFrame to a Apache Spark SQL DataFrame using myDynFrame.toDF()
# Then you can import the pyspark.sql functions and use them to manipulate the data
# To convert back to an AWS Glue DynamicFrame use DynamicFrame.fromDF(myDataFrame, glueContext, "<name of resulting DynamicFrame>")

Datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "strike-off-objections-mongo-extract", table_name = "strike_off_objections", transformation_ctx = "Datasource0")

# This will flatten nested schema in a DynamicFrame and pivots out array columns from the flattened frame.
# Also writes temporary files to the staging_path bucket
relationalized_json = Datasource0.relationalize(root_table_name = "root", staging_path = s3_staging_path)

root_df_attachments = relationalized_json.select('root_attachments')
root_df = relationalized_json.select('root')

## Do the Attachments first

# relationalize will create empty attachments record for objections that have no attachments, so filter them out
df_filtered_attachments = Filter.apply(frame = root_df_attachments, f = lambda x: x["attachments.val.id"] != '')

# Make the join between the attachments in df_filtered_attachments and the objections in the root_df
df_attachments_joined = Join.apply(df_filtered_attachments, root_df, 'id', 'attachments')

# Map the attachment field names into what will become the column names in Redshift
df_attachments_mapped = ApplyMapping.apply(frame = df_attachments_joined, mappings = 
[("`attachments.val.id`", "string", "id", "string"),
    ("_id", "string", "strike_off_objection_id", "string"),
    ("`attachments.val.name`", "string", "name", "string"),
    ("`attachments.val.content_type`", "string", "content_type", "string"),
    ("`attachments.val.size`", "int", "size", "int"),
    ("`attachments.val.links.linksMap.self`", "string", "link_self", "string"),
    ("`attachments.val.links.linksMap.download`", "string", "link_download", "string")], transformation_ctx = "df_attachments_mapped")

# If there are any ambiguous data types, "make_cols" will create new columns for them eg fieldname_date or _string
# Hopefully we won't have anything unexpected as the mapping should resolve this above
df_attachments_resolved = ResolveChoice.apply(frame = df_attachments_mapped, choice = "make_cols", transformation_ctx = "df_attachments_resolved")

# If we have any records that are just nulls then drop them
df_attachments_dropped_null_fields = DropNullFields.apply(frame = df_attachments_resolved, transformation_ctx = "df_attachments_dropped_null_fields")

# Write the attachments data in the df_attachments_dropped_null_fields dynamic frame into Redshift
datasink_attachments = glueContext.write_dynamic_frame.from_jdbc_conf(frame = df_attachments_dropped_null_fields, 
    catalog_connection = job_connection, connection_options = {"preactions": preactions_attachments, "dbtable": dbtable_attachments, "database": database}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink_attachments")

## Now do the Objections

# Map the objections field names into what will become the column names in Redshift 
df_obj_mapped = ApplyMapping.apply(frame = root_df, mappings = 
[("_id", "string", "id", "string"),
    ("`created_on.$date`", "string", "created_on", "timestamp"),
    ("`created_by.id`", "string", "created_by_id", "string"),
    ("`created_by.email`", "string", "created_by_email", "string"),
    ("`created_by.full_name`", "string", "created_by_full_name", "string"),
    ("`created_by.share_identity`", "boolean", "created_by_share_identity", "boolean"),
    ("`company_number`", "string", "company_number", "string"),
    ("`status`", "string", "status", "string"),
    ("`status_changed_on.$date`", "string", "status_changed_on", "timestamp"),
    ("`reason`", "string", "reason", "string"),
    ("`action_code`", "int", "action_code", "int"),
    ("`http_request_id`", "string", "http_request_id", "string"),
    ("`links.linksMap.self`", "string", "links_self", "string")], transformation_ctx = "df_obj_mapped")

# If there are any ambiguous data types, "make_cols" will create new columns for them eg fieldname_date or _string
# Hopefully we won't have anything unexpected as the mapping should resolve this above
df_obj_resolved = ResolveChoice.apply(frame = df_obj_mapped, choice = "make_cols", transformation_ctx = "df_obj_resolved")

# If we have any records that are just nulls then drop them
df_obj_dropped_null_fields = DropNullFields.apply(frame = df_obj_resolved, transformation_ctx = "df_obj_dropped_null_fields")

# Write the objections data in the df_obj_dropped_null_fields dynamic frame into Redshift
datasink_objections = glueContext.write_dynamic_frame.from_jdbc_conf(frame = df_obj_dropped_null_fields, catalog_connection = job_connection, connection_options = {"preactions":preactions_objections, "dbtable": dbtable_objections, "database": database}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink_objections")

job.commit()
