##################################################
## Script Name   : REPORTING.py
## Project Name  : <PROJECT NAME>
## Module Name   : Reporting
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
    "PROCESSED_BUCKET",
    "RS_CLUSTER",
    "RS_SECRET",
    "RS_SCHEMA",
    "RS_TABLE",
    "TempDir"
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
    ID AS HCP_ID,
    CONCAT_WS(' ', FIRST_NAME, MIDDLE_NAME, LAST_NAME) AS HCP_NAME,
    CAST(NPI AS STRING) AS HCP_NPI,
    CONCAT_WS(',', ADDR_LINE_1, CITY, STATE, ZIP) AS HCP_ADDR
  FROM F_CUSTOMER
'''

# To apply SQL transformation on DynamicFrame
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
  for alias, frame in mapping.items():
    frame.toDF().createOrReplaceTempView(alias)
  result = spark.sql(query)
  return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

# Define Reporting Class
class Reporting(object):
  
  # Define Reporting Constructor
  def __init__(self):
    print('\n---===<<<BEGIN REPORTING>>>===---\n')
    self.executionStatus = True
    
    # Connecting to RDS Database
    self.SMClient = boto3.client('secretsmanager')
    SMResponse = self.SMClient.get_secret_value(
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
    
    sql = '''SELECT * FROM CTL_COLUMN_METADATA'''
    self.cursor.execute(sql)
    self.CTL_COLUMN_METADATA = self.cursor.fetchall()
    print('Column Metadata configuration loaded')
    
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
    self.processName = 'REPORTING'
    self.datasetType = 'PROCESSED'
    
  def executeReporting(self):
    
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
      if CTL_DATASET_MASTER_['DATASET_TYPE'] == 'raw':
        dependencyBucket = args['CURATED_BUCKET']
      else:
        dependencyBucket = args['PROCESSED_BUCKET']
      S3ReadNode = glueContext.create_dynamic_frame.from_options (
        format_options = {
          'withHeader': True,
          'separator': ','
        },
        connection_type = 's3',
        format = 'parquet',
        connection_options = {
          'paths': [ 's3://' + dependencyBucket + '/' + CTL_DATASET_MASTER_['TARGET_LOCATION'] ]
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
    
    # Creating and starting crawler for Fact table
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
    
    # Publish Target table to Redshift
    columnMetadata_ = list(filter(lambda x: int(x['DATASET_ID']) == int(self.CTL_DATASET_MASTER['DATASET_ID']), self.CTL_COLUMN_METADATA))
    columnMetadata_ = sorted(columnMetadata_, key = lambda k: k['COLUMN_SEQUENCE_NUMBER'])
    columnMapping = ''
    for field in columnMetadata_:
      columnMapping += field['COLUMN_NAME'] + ' ' + field['COLUMN_DATA_TYPE'] + ','
    columnMapping = columnMapping[:-1]
    SMResponse = self.SMClient.get_secret_value(
      SecretId = args['RS_SECRET']
    )
    RSCreds = json.loads(SMResponse['SecretString'])
    RSDataClient = boto3.client('redshift-data')
    RSDataResponse = RSDataClient.execute_statement(
      ClusterIdentifier = args['RS_CLUSTER'],
      Database = str(RSCreds.get('dbname')),
      SecretArn = args['RS_SECRET'],
      Sql = '''CREATE SCHEMA IF NOT EXISTS {RSSchema}'''.format(RSSchema = args['RS_SCHEMA']),
      StatementName = 'CreateTable',
      WithEvent = False
    )
    RSDataResponse = RSDataClient.execute_statement(
      ClusterIdentifier = args['RS_CLUSTER'],
      Database = str(RSCreds.get('dbname')),
      SecretArn = args['RS_SECRET'],
      Sql = '''DROP TABLE IF EXISTS {RSSchema}.{RSTable}'''.format(
        RSSchema = args['RS_SCHEMA'],
        RSTable = args['RS_TABLE']
      ),
      StatementName = 'DropTable',
      WithEvent = False
    )
    RSDataResponse = RSDataClient.execute_statement(
      ClusterIdentifier = args['RS_CLUSTER'],
      Database = str(RSCreds.get('dbname')),
      SecretArn = args['RS_SECRET'],
      Sql = '''CREATE TABLE IF NOT EXISTS {RSSchema}.{RSTable} ( {colMap} )'''.format(
        RSSchema = args['RS_SCHEMA'],
        RSTable = args['RS_TABLE'],
        colMap = columnMapping
      ),
      StatementName = 'CreateTable',
      WithEvent = False
    )
    RedshiftPublishNode = glueContext.write_dynamic_frame.from_options (
      frame = sqlNode,
      connection_type = 'redshift',
      connection_options = {
        'url': 'jdbc:redshift://' + str(RSCreds.get('host')) + ':' + str(RSCreds.get('port')) + '/' + str(RSCreds.get('dbname')),
        'user': RSCreds.get('username'),
        'password': RSCreds.get('password'),
        'dbtable': args['RS_SCHEMA'] + '.' + args['RS_TABLE'],
        'preactions': 'truncate table ' + args['RS_SCHEMA'] + '.' + args['RS_TABLE'],
        'redshiftTmpDir': args['TempDir']
      },
      format = None,
      format_options = {},
      transformation_ctx = 'RedshiftPublishNode'
    )
    print('File published to Redshift')
    
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
    
    # Close RDS Connection and terminate Reporting
    self.RDSConn.close()
    print('\n---===END REPORTING===---\n')
    return self.executionStatus

if __name__ == '__main__':
    executeRPT = Reporting().executeReporting()
    job.commit()
