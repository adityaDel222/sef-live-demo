##################################################
## Script Name   : DIMENSION.py
## Project Name  : <PROJECT NAME>
## Module Name   : Dimension
## Author        : ZS
## Date Created  : YYYY-MM-DD
## Date Updated  : YYYY-MM-DD
## Updated By    : ZS
## Update Reason : Launch
##################################################

# Import Required Libraries
import re
import sys
import json
import boto3
import pymysql
import datetime
from datetime import datetime
from botocore.client import ClientError

# Import Default Glue Libaries
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

# Fetch Job parameters
args = getResolvedOptions(
  sys.argv, [
    "JOB_NAME",
    "RDS_SECRET",
    "RDS_HOST",
    "RDS_DB_NAME",
    "GLUE_JOB_ID",
    "GLUE_JOB_NAME",
    "GLUE_SERVICE_ROLE",
    "CATALOG_DB_NAME",
    "CURATED_BUCKET",
    "PROCESSED_BUCKET"
  ]
)

# Initiate Glue Job
sc = SparkContext()
# sc.stop()
# conf = (SparkConf().set("spark.sql.legacy.timeParserPolicy", "LEGACY"))
# conf = (SparkConf().set("spark.sql.crossJoin.enabled", "true"))
# conf = (SparkConf().set("spark.sql.shuffle.partitions", "1"))
# conf = (SparkConf().set("spark.sql.autoBroadcastJoinThreshold", "1048576"))
# sc = SparkContext(conf = conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Transformation SQL
transSQL = '''
  SELECT DISTINCT
    HCP.MDM_HCP_ID AS MDM_HCP_ID,
    HCP.MDM_HCP_FNAME AS MDM_HCP_FNAME,
    HCP.MDM_HCP_MNAME AS MDM_HCP_MNAME,
    HCP.MDM_HCP_LNAME AS MDM_HCP_LNAME,
    CAST(HCP.MDM_HCP_NPI AS STRING) AS MDM_HCP_NPI,
    ADDR.VEEVA_HCP_ADDR_LN_1 AS VEEVA_HCP_ADDR_LN_1,
    ADDR.VEEVA_HCP_ADDR_LN_2 AS VEEVA_HCP_ADDR_LN_2,
    ADDR.VEEVA_HCP_CITY AS VEEVA_HCP_CITY,
    ADDR.VEEVA_HCP_ST AS VEEVA_HCP_ST,
    ADDR.VEEVA_HCP_ZIP AS VEEVA_HCP_ZIP
  FROM (
    SELECT DISTINCT MDM_HCP_ID, MDM_HCP_FNAME, MDM_HCP_MNAME, MDM_HCP_LNAME, MDM_HCP_NPI
    FROM STG_MDM_HCP
  ) HCP
  LEFT OUTER JOIN (
    SELECT DISTINCT
      VEEVA_ADDR_ID, VEEVA_HCP_NPI,
      VEEVA_HCP_ADDR_LN_1, VEEVA_HCP_ADDR_LN_2, VEEVA_HCP_CITY, VEEVA_HCP_ST, VEEVA_HCP_ZIP
    FROM STG_VEEVA_ADDR
  ) ADDR
  ON HCP.MDM_HCP_NPI = ADDR.VEEVA_HCP_NPI
'''

# To apply SQL transformation on DynamicFrame
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
  for alias, frame in mapping.items():
    frame.toDF().createOrReplaceTempView(alias)
  result = spark.sql(query)
  return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

# Define Dimension Class
class Dimension(object):

  # Define Dimension Constructor
  def __init__(self):
    print('\n---===<<<BEGIN DIMENSION>>>===---\n')
    self.executionStatus = True
    
    # Connecting to RDS Database
    SMClient = boto3.client('secretsmanager')
    SMResponse = SMClient.get_secret_value(
      SecretId = args['RDS_SECRET']
    )
    RDSCreds = json.loads(SMResponse['SecretString'])
    self.RDSConn = pymysql.connect(
      host = args['RDS_HOST'],
      user = RDSCreds.get('username'),
      password = RDSCreds.get('password'),
      database = args['RDS_DB_NAME'],
      cursorclass = pymysql.cursors.DictCursor
    )
    self.cursor = self.RDSConn.cursor()
    print('Connected to RDS Database')
    
    sql = '''SELECT * FROM CTL_DATASET_MASTER'''
    self.cursor.execute(sql)
    self.CTL_DATASET_MASTER = self.cursor.fetchall()
    print('Dataset Master configuration loaded')
    
    sql = '''SELECT * FROM CTL_DEPENDENCY_MASTER'''
    self.cursor.execute(sql)
    self.CTL_DEPENDENCY_MASTER = self.cursor.fetchall()
    self.CTL_DEPENDENCY_MASTER = list(filter(lambda x: x['ACTIVE_FLAG'] == 'Y', self.CTL_DEPENDENCY_MASTER))
    self.CTL_DEPENDENCY_MASTER = list(filter(lambda x: int(x['JOB_ID']) == int(args['GLUE_JOB_ID']), self.CTL_DEPENDENCY_MASTER))
    print('Dependency Master configuration loaded')
    
    # Set LOG variables for this run
    self.cycleID = datetime.now().date().strftime('%Y%m%d')
    self.loadDate = datetime.now()
    self.glueJobID = args['GLUE_JOB_ID']
    self.glueJobName = args['GLUE_JOB_NAME']
    self.processName = 'DIMENSION'
    self.datasetType = 'PROCESSED'
    
  def executeDimension(self):
    
    # Fetch Run ID for the given dataset from LOG Dataset
    sql = '''select CYCLE_ID, max(RUN_ID) + 1 as MAX_RUN_ID from LOG_DATASET_DTL where GLUE_JOB_ID = {gjID} group by CYCLE_ID'''.format(gjID = int(args['GLUE_JOB_ID']))
    self.cursor.execute(sql)
    runID = self.cursor.fetchone()
    runID = runID['MAX_RUN_ID'] if runID != None else 1
    
    recordCount = 0
    processMessage = ''
    
    # Loading all dependencies
    depNodes = {}
    for dependency in self.CTL_DEPENDENCY_MASTER:
      CTL_DATASET_MASTER_ = list(filter(lambda x: int(x['DATASET_ID']) == int(dependency['SOURCE_DATASET_ID']), self.CTL_DATASET_MASTER))[0]
      self.datasetID = CTL_DATASET_MASTER_['DATASET_ID']
      self.datasetName = CTL_DATASET_MASTER_['DATASET_NAME']
      S3ReadNode = glueContext.create_dynamic_frame.from_options (
        format_options = {
          'withHeader': True,
          'separator': ','
        },
        connection_type = 's3',
        format = 'parquet',
        connection_options = {
          'paths': [ 's3://' + args['CURATED_BUCKET'] + '/' + CTL_DATASET_MASTER_['TARGET_LOCATION'] ]
        },
        transformation_ctx = 'S3ReadNode'
      )
      depNodes[CTL_DATASET_MASTER_['DATASET_NAME']] = S3ReadNode
      print('Dependency fetched: ' + str(CTL_DATASET_MASTER_['DATASET_NAME']))
    
    self.CTL_DATASET_MASTER = list(filter(lambda x: int(x['JOB_ID']) == int(args['GLUE_JOB_ID']), self.CTL_DATASET_MASTER))[0]
    
    # DynamicFrame to apply SQL Transformation
    sqlNode = sparkSqlQuery(
      glueContext,
      query = transSQL,
      mapping = depNodes,
      transformation_ctx = 'sqlNode'
    )
    print('Transformation SQL applied')
    
    # Delete existing file at Publish location
    s3Resource = boto3.resource('s3')
    files = s3Resource.Bucket(args['PROCESSED_BUCKET']).objects.filter(Prefix = self.CTL_DATASET_MASTER['TARGET_LOCATION'])
    for file in files:
      s3Resource.Object(args['PROCESSED_BUCKET'], file.key).delete()
    print('Publish location cleared of existing files')
    
    # DynamicFrame to write target table to Publish location
    S3WriteNodeR = sqlNode.repartition(100)
    S3WriteNode = glueContext.write_dynamic_frame.from_options(
      frame = S3WriteNodeR,
      connection_type = 's3',
      format = 'parquet',
      connection_options = {
        'path': '''s3://{proBucket}/{tgtLoc}'''.format(
          proBucket = args['PROCESSED_BUCKET'],
          tgtLoc = self.CTL_DATASET_MASTER['TARGET_LOCATION']
        ),
        'partitionKeys': [],
      },
      format_options = { 'compression': 'snappy' },
      transformation_ctx = 'S3WriteNode'
    )
    print('Target table written to Publish location')
    
    # DynamicFrame to write target table to Archive location
    S3WriteNode = glueContext.write_dynamic_frame.from_options(
      frame = S3WriteNodeR,
      connection_type = 's3',
      format = 'parquet',
      connection_options = {
        'path': '''s3://{proBucket}/{archLoc}{date}/{datetime}/'''.format(
          proBucket = args['PROCESSED_BUCKET'],
          archLoc = self.CTL_DATASET_MASTER['ARCHIVE_LOCATION'],
          date = datetime.now().strftime('%Y-%m-%d'),
          datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ),
        'partitionKeys': []
      },
      format_options = { 'compression': 'snappy' },
      transformation_ctx = 'S3WriteNode'
    )
    print('Target table written to Archive location')
    
    # Creating and starting crawler for Dimension table
    glueClient = boto3.client('glue')
    try:
      glueClient.delete_table(
        DatabaseName = args['CATALOG_DB_NAME'],
        Name = self.CTL_DATASET_MASTER['TARGET_TABLE_NAME']
      )
      print('Existing Catalog table deleted')
    except:
      pass
    try:
      glueClient.delete_crawler(
        Name = str(args['CATALOG_DB_NAME']).upper() + '_' + str(self.CTL_DATASET_MASTER['TARGET_TABLE_NAME']).upper()
      )
      print('Existing Crawler deleted')
    except:
      pass
    try:
      glueClient.create_crawler(
        Name = str(args['CATALOG_DB_NAME']).upper() + '_' + str(self.CTL_DATASET_MASTER['TARGET_TABLE_NAME']).upper(),
        Role = args['GLUE_SERVICE_ROLE'],
        DatabaseName = args['CATALOG_DB_NAME'],
        Targets = {
          'S3Targets': [{
            'Path': 's3://' + str(args['PROCESSED_BUCKET']) + '/' + str(self.CTL_DATASET_MASTER['TARGET_LOCATION'])
            # 'ConnectionName': args['CATALOG_CONN_NAME'] if args['CATALOG_CONN_NAME'].upper() != 'NONE' else ''
          }]
        },
        SchemaChangePolicy = {
          'UpdateBehavior': 'LOG',
          'DeleteBehavior': 'LOG'
        },
        RecrawlPolicy = {
          'RecrawlBehavior': 'CRAWL_EVERYTHING'
        }
      )
      print('Crawler successfully created')
    except:
      pass
    try:
      glueClient.start_crawler(
        Name = str(args['CATALOG_DB_NAME']).upper() + '_' + str(self.CTL_DATASET_MASTER['TARGET_TABLE_NAME']).upper()
      )
      print('Crawler successfully started')
    except:
      pass
    
    recordCount = sqlNode.toDF().count()
    processStatus = 'SUCCESS'
    
    # Insert entry in LOG Dataset DTL
    sql = '''INSERT INTO LOG_DATASET_DTL values ( '{cID}', {rID}, '{lDt}', {gID}, '{gNM}', {dID}, '{dTP}', '{dNM}', '{pNM}', '{pST}', {rCT}, '{pMG}' )'''.format(
      cID = str(self.cycleID),
      rID = str(runID),
      lDt = str(self.loadDate),
      gID = str(self.glueJobID),
      gNM = str(self.glueJobName),
      dID = str(self.datasetID),
      dTP = str(self.datasetType),
      dNM = str(self.datasetName),
      pNM = str(self.processName),
      pST = str(processStatus),
      rCT = str(recordCount),
      pMG = str(processMessage)
    )
    self.cursor.execute(sql)
    self.RDSConn.commit()
    print('LOG Dataset Detail updated')
    
    # Close RDS Connection and terminate Dimension
    self.RDSConn.close()
    print('\n---===END DIMENSION===---\n')
    return self.executionStatus

if __name__ == '__main__':
    executeDim = Dimension().executeDimension()
    job.commit()
