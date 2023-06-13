##################################################
## Script Name   : INGESTION.py
## Project Name  : <PROJECT NAME>
## Module Name   : Ingestion
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
    "RAW_BUCKET",
    "CURATED_BUCKET"
  ]
)

# Initiate Glue Job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# To apply SQL transformation on DynamicFrame
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
  for alias, frame in mapping.items():
    frame.toDF().createOrReplaceTempView(alias)
  result = spark.sql(query)
  return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

# Define Ingestion Class
class Ingestion(object):
  
  # Define Ingestion Constructor
  def __init__(self):
    
    print('\n---===<<<BEGIN INGESTION>>>===---\n')
    self.executionStatus = True
    
    # Connecting to RDS Database
    SMClient = boto3.client('secretsmanager')
    SMResponse = SMClient.get_secret_value(
      SecretId = args['RDS_SECRET']
    )
    self.RDSCreds = json.loads(SMResponse['SecretString'])
    self.RDSConn = pymysql.connect(
      host = args['RDS_HOST'],
      user = self.RDSCreds.get('username'),
      password = self.RDSCreds.get('password'),
      database = args['RDS_DB_NAME'],
      cursorclass = pymysql.cursors.DictCursor
    )
    self.cursor = self.RDSConn.cursor()
    print('Connected to RDS Database')
    
    sql = '''SELECT * FROM CTL_DATASET_MASTER'''
    self.cursor.execute(sql)
    self.CTL_DATASET_MASTER = self.cursor.fetchall()
    self.CTL_DATASET_MASTER = list(filter(lambda x: x['DATASET_TYPE'] == 'raw', self.CTL_DATASET_MASTER))
    self.CTL_DATASET_MASTER = list(filter(lambda x: int(x['JOB_ID']) == int(args['GLUE_JOB_ID']), self.CTL_DATASET_MASTER))
    self.CTL_DATASET_MASTER = list(filter(lambda x: x['ACTIVE_FLAG'] == 'Y', self.CTL_DATASET_MASTER))
    print('Dataset Master configuration loaded\n#Files to be processed: ' + str(len(self.CTL_DATASET_MASTER)))
    
    sql = '''SELECT * FROM CTL_COLUMN_METADATA'''
    self.cursor.execute(sql)
    self.CTL_COLUMN_METADATA = self.cursor.fetchall()
    print('Column Metadata configuration loaded')
    
    sql = '''SELECT * FROM CTL_DQM_MASTER'''
    self.cursor.execute(sql)
    self.CTL_DQM_MASTER = self.cursor.fetchall()
    self.CTL_DQM_MASTER = list(filter(lambda x: x['ACTIVE_FLAG'] == 'Y', self.CTL_DQM_MASTER))
    print('DQM Master configuration loaded')
    
    # Set LOG variables for this run
    self.cycleID = datetime.now().date().strftime('%Y%m%d')
    self.loadDate = datetime.now()
    self.glueJobID = args['GLUE_JOB_ID']
    self.glueJobName = args['GLUE_JOB_NAME']
    self.processName = 'INGESTION'
    self.datasetType = 'RAW'

  def ingestFiles(self):
    
    for dataset in self.CTL_DATASET_MASTER:
      DATASET_ID = dataset['DATASET_ID']
      DATASET_NAME = dataset['DATASET_NAME']
      SELECT_ALL_FIELDS = dataset['SELECT_ALL_FIELDS']
      FILE_NAME_PATTERN = dataset['FILE_NAME_PATTERN']
      FILE_FORMAT = dataset['FILE_FORMAT']
      FILE_DELIMITER = dataset['FILE_DELIMITER']
      HEADER_FLAG = dataset['HEADER_FLAG']
      SOURCE_LOCATION = dataset['SOURCE_LOCATION']
      TARGET_LOCATION = dataset['TARGET_LOCATION']
      ARCHIVE_LOCATION = dataset['ARCHIVE_LOCATION']
      TARGET_TABLE_NAME = dataset['TARGET_TABLE_NAME']
      print(str(DATASET_ID) + ': ' + str(DATASET_NAME) + '>> Starting Ingestion')
      
      # Fetch Run ID for the given dataset from LOG Dataset
      sql = '''select CYCLE_ID, max(RUN_ID) + 1 as MAX_RUN_ID from LOG_DATASET_DTL where DATASET_ID = {dID} group by CYCLE_ID'''.format(dID = int(DATASET_ID))
      self.cursor.execute(sql)
      runID = self.cursor.fetchone()
      runID = runID['MAX_RUN_ID'] if runID != None else 1
      
      recordCount = 0
      processMessage = ''
      
      # Fetch available files at Source location with specified file name format
      s3Client = boto3.client('s3')
      files = s3Client.list_objects_v2(Bucket = args['RAW_BUCKET'], Prefix = SOURCE_LOCATION)['Contents']
      fileNamePatternRegex = re.compile(
        ('^' + str(FILE_NAME_PATTERN) + '.' + str(FILE_FORMAT) + '$')
        .replace('YYYYMMDDHHMMSS', '\d{14}')
        .replace('YYYYMMDD', '\d{8}'),
        re.IGNORECASE
      )
      
      # Calculate latest available file timestamp
      fileDateMatch = []
      for file in files:
        file = file['Key'].split('/')[-1]
        if fileNamePatternRegex.match(file):
          if re.search('\d{14}', file):
            fileDateMatch.append(re.search('\d{14}', file).group())
          elif re.search('\d{8}', file):
            fileDateMatch.append(int(re.search('\d{8}', file).group()))
      maxFileDateMatch = str(max(fileDateMatch))
      
      # Select file with the latest available timestamp
      maxFileMatch = []
      for file in files:
        file_ = file['Key'].split('/')[-1]
        if fileNamePatternRegex.match(file_):
          if re.search(maxFileDateMatch, file_):
            maxFileMatch.append(file)
      latestFile = max(maxFileMatch, key = lambda x: x['LastModified'])['Key']
      print(str(DATASET_ID) + ': ' + str(DATASET_NAME) + '>> Latest File Picked: ' + str(latestFile))
      
      # Fetch file layout from Column Metadata config and generate column map
      if SELECT_ALL_FIELDS != 'Y':
        columnMetadata_ = list(filter(lambda x: int(x['DATASET_ID']) == int(DATASET_ID), self.CTL_COLUMN_METADATA))
        columnMetadata_ = sorted(columnMetadata_, key = lambda k: k['COLUMN_SEQUENCE_NUMBER'])
        columnMapping = []
        for field in columnMetadata_:
          columnMap = tuple([field['SOURCE_COLUMN_NAME'], 'string', field['COLUMN_NAME'], 'string'])
          columnMapping.append(columnMap)
        print(str(DATASET_ID) + ': ' + str(DATASET_NAME) + '>> Column Mapping generated')
      
      # DynamicFrame to read file from Source location
      LandingNode0 = glueContext.create_dynamic_frame.from_options (
        format_options = {
          'withHeader': True if HEADER_FLAG == 'Y' else False,
          'separator': FILE_DELIMITER
        },
        connection_type = 's3',
        format = FILE_FORMAT,
        connection_options = {
          'paths': [ 's3://' + args['RAW_BUCKET'] + '/' + SOURCE_LOCATION + latestFile.split('/')[-1] ]
        },
        transformation_ctx = 'LandingNode0'
      )
      print(str(DATASET_ID) + ': ' + str(DATASET_NAME) + '>> Source file loaded from Raw location')
      
      # DynamicFrame to apply layout mapping
      LandingNode1 = LandingNode0
      if SELECT_ALL_FIELDS != 'Y':
        LandingNode1 = ApplyMapping.apply (
          frame = LandingNode0,
          mappings = columnMapping,
          transformation_ctx = 'LandingNode1'
        )
        print(str(DATASET_ID) + ': ' + str(DATASET_NAME) + '>> Column mapping applied')
      
      # Add Row ID to Landing table
      sql = '''select row_number() over ( order by 1 ) as ROW_ID, * from landingNode'''
      LandingNode2 = sparkSqlQuery (
        glueContext,
        query = sql,
        mapping = { 'landingNode': LandingNode1 },
        transformation_ctx = 'LandingNode2'
      )
      print(str(DATASET_ID) + ': ' + str(DATASET_NAME) + '>> Row ID added')
      
      # Apply DQM checks on the given dataset
      CTL_DQM_MASTER_ = list(filter(lambda x: int(x['DATASET_ID']) == int(DATASET_ID), self.CTL_DQM_MASTER))
      for QC in CTL_DQM_MASTER_:
        if QC['QC_TYPE'].upper() == 'NULL':
          sql = '''
            SELECT
              '{cID}' AS `CYCLE_ID`,
              {rID} AS `RUN_ID`,
              {dID} AS `DATASET_ID`,
              ROW_ID AS `ROW_ID`,
              {qID} AS `QC_ID`,
              '{cNM}' AS `COLUMN_NAME`,
              '{qTP}' AS `QC_TYPE`,
              '{qPM}' AS `QC_PARAM`,
              '{qDC}' AS `QC_DESC`,
              '{CTY}' AS `CRITICALITY`
            FROM landingTable
            WHERE coalesce({cNM}, '') = ''
              OR UPPER(TRIM({cNM})) IN ('NULL', '')
          '''.format(
            cID = self.cycleID,
            rID = runID,
            dID = DATASET_ID,
            qID = QC['QC_ID'],
            cNM = QC['COLUMN_NAME'],
            qTP = QC['QC_TYPE'],
            qPM = QC['QC_PARAM'],
            qDC = QC['COLUMN_NAME'] + ' must not be NULL',
            CTY = QC['CRITICALITY']
          )
          LOGDQMTempNode = sparkSqlQuery (
            glueContext,
            query = sql,
            mapping = { 'landingTable': LandingNode2 },
            transformation_ctx = 'LandingNode2'
          )
          LOGDQMTempDF = LOGDQMTempNode.toDF()
          LOGDQMTempDF.write \
            .mode("append") \
            .format("jdbc") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("url", "jdbc:mysql://" + args['RDS_HOST'] + ":3306/" + args['RDS_DB_NAME']) \
            .option("dbtable", "LOG_DQM_DTL") \
            .option("user", self.RDSCreds.get('username')) \
            .option("password", self.RDSCreds.get('password')) \
            .save()
        
        if QC['QC_TYPE'].upper() == 'LENGTH':
          sql = '''
            SELECT
              '{cID}' AS `CYCLE_ID`,
              {rID} AS `RUN_ID`,
              {dID} AS `DATASET_ID`,
              ROW_ID AS `ROW_ID`,
              {qID} AS `QC_ID`,
              '{cNM}' AS `COLUMN_NAME`,
              '{qTP}' AS `QC_TYPE`,
              '{qPM}' AS `QC_PARAM`,
              '{qDC}' AS `QC_DESC`,
              '{CTY}' AS `CRITICALITY`
            FROM landingTable
            WHERE coalesce({cNM}, '') != ''
              AND UPPER(TRIM({cNM})) NOT IN ('NULL', '')
              AND CASE
                WHEN SUBSTRING(TRIM('{qPM}'), 1, 2) = '<=' THEN
                  CASE WHEN LENGTH({cNM}) <= CAST(SUBSTRING(TRIM('{qPM}'), 3, LENGTH(TRIM('{qPM}'))) AS INT) THEN 1 ELSE 0 END
                WHEN SUBSTRING(TRIM('{qPM}'), 1, 2) = '>=' THEN
                  CASE WHEN LENGTH({cNM}) >= CAST(SUBSTRING(TRIM('{qPM}'), 3, LENGTH(TRIM('{qPM}'))) AS INT) THEN 1 ELSE 0 END
                WHEN SUBSTRING(TRIM('{qPM}'), 1, 1) = '=' THEN
                  CASE WHEN LENGTH({cNM}) = CAST(SUBSTRING(TRIM('{qPM}'), 2, LENGTH(TRIM('{qPM}'))) AS INT) THEN 1 ELSE 0 END
                WHEN SUBSTRING(TRIM('{qPM}'), 1, 1) = '<' THEN
                  CASE WHEN LENGTH({cNM}) < CAST(SUBSTRING(TRIM('{qPM}'), 2, LENGTH(TRIM('{qPM}'))) AS INT) THEN 1 ELSE 0 END
                WHEN SUBSTRING(TRIM('{qPM}'), 1, 1) = '>' THEN
                  CASE WHEN LENGTH({cNM}) > CAST(SUBSTRING(TRIM('{qPM}'), 2, LENGTH(TRIM('{qPM}'))) AS INT) THEN 1 ELSE 0 END
                ELSE 0
              END = 0
          '''.format(
            cID = self.cycleID,
            rID = runID,
            dID = DATASET_ID,
            qID = QC['QC_ID'],
            cNM = QC['COLUMN_NAME'],
            qTP = QC['QC_TYPE'],
            qPM = QC['QC_PARAM'],
            qDC = QC['COLUMN_NAME'] + ' must have a length ' + QC['QC_PARAM'],
            CTY = QC['CRITICALITY']
          )
          LOGDQMTempNode = sparkSqlQuery (
            glueContext,
            query = sql,
            mapping = { 'landingTable': LandingNode2 },
            transformation_ctx = 'LandingNode2'
          )
          LOGDQMTempDF = LOGDQMTempNode.toDF()
          LOGDQMTempDF.write \
            .mode("append") \
            .format("jdbc") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("url", "jdbc:mysql://" + args['RDS_HOST'] + ":3306/" + args['RDS_DB_NAME']) \
            .option("dbtable", "LOG_DQM_DTL") \
            .option("user", self.RDSCreds.get('username')) \
            .option("password", self.RDSCreds.get('password')) \
            .save()
        
        if QC['QC_TYPE'].upper() == 'DOMAIN':
          sql = '''
            SELECT
              '{cID}' AS `CYCLE_ID`,
              {rID} AS `RUN_ID`,
              {dID} AS `DATASET_ID`,
              ROW_ID AS `ROW_ID`,
              {qID} AS `QC_ID`,
              '{cNM}' AS `COLUMN_NAME`,
              '{qTP}' AS `QC_TYPE`,
              '{qPM1}' AS `QC_PARAM`,
              '{qDC}' AS `QC_DESC`,
              '{CTY}' AS `CRITICALITY`
            FROM landingTable
            WHERE coalesce({cNM}, '') != ''
              AND UPPER(TRIM({cNM})) NOT IN ('NULL', '')
              AND UPPER(TRIM({cNM})) NOT IN ({qPM2})
          '''.format(
            cID = self.cycleID,
            rID = runID,
            dID = DATASET_ID,
            qID = QC['QC_ID'],
            cNM = QC['COLUMN_NAME'],
            qTP = QC['QC_TYPE'],
            qPM1 = QC['QC_PARAM'],
            qDC = QC['COLUMN_NAME'] + ' must be one of: ' + QC['QC_PARAM'],
            qPM2 = '\'' + (','.join([x.strip() for x in str(QC['QC_PARAM']).split(',')])).replace(',', '\',\'') + '\'',
            CTY = QC['CRITICALITY']
          )
          LOGDQMTempNode = sparkSqlQuery (
            glueContext,
            query = sql,
            mapping = { 'landingTable': LandingNode2 },
            transformation_ctx = 'LandingNode2'
          )
          LOGDQMTempDF = LOGDQMTempNode.toDF()
          LOGDQMTempDF.write \
            .mode("append") \
            .format("jdbc") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("url", "jdbc:mysql://" + args['RDS_HOST'] + ":3306/" + args['RDS_DB_NAME']) \
            .option("dbtable", "LOG_DQM_DTL") \
            .option("user", self.RDSCreds.get('username')) \
            .option("password", self.RDSCreds.get('password')) \
            .save()
        
        if QC['QC_TYPE'].upper() == 'CUSTOM':
          sql = '''
            SELECT
              '{cID}' AS `CYCLE_ID`,
              {rID} AS `RUN_ID`,
              {dID} AS `DATASET_ID`,
              ROW_ID AS `ROW_ID`,
              {qID} AS `QC_ID`,
              '{cNM}' AS `COLUMN_NAME`,
              '{qTP}' AS `QC_TYPE`,
              '{qPM}' AS `QC_PARAM`,
              '{qDC}' AS `QC_DESC`,
              '{CTY}' AS `CRITICALITY`
            FROM ( {qPM} )
          '''.format(
            cID = self.cycleID,
            rID = runID,
            dID = DATASET_ID,
            qID = QC['QC_ID'],
            cNM = QC['COLUMN_NAME'],
            qTP = QC['QC_TYPE'],
            qPM = QC['QC_PARAM'],
            qDC = QC['COLUMN_NAME'] + ' must satisfy custom check',
            CTY = QC['CRITICALITY']
          )
          LOGDQMTempNode = sparkSqlQuery (
            glueContext,
            query = sql,
            mapping = { 'landingTable': LandingNode2 },
            transformation_ctx = 'LandingNode2'
          )
          LOGDQMTempDF = LOGDQMTempNode.toDF()
          LOGDQMTempDF.write \
            .mode("append") \
            .format("jdbc") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("url", "jdbc:mysql://" + args['RDS_HOST'] + ":3306/" + args['RDS_DB_NAME']) \
            .option("dbtable", "LOG_DQM_DTL") \
            .option("user", self.RDSCreds.get('username')) \
            .option("password", self.RDSCreds.get('password')) \
            .save()
      
      self.RDSConn.commit()
      sql = '''SELECT DISTINCT * FROM `LOG_DQM_DTL` WHERE `DATASET_ID` = {dID} AND `CYCLE_ID` = '{cID}' AND `RUN_ID` = {rID}'''.format(
        dID = str(DATASET_ID),
        cID = str(self.cycleID),
        rID = str(runID)
      )
      self.cursor.execute(sql)
      LOG_DQM_DTL_ = self.cursor.fetchall()
      LOGDQMDTLSchema = StructType([
        StructField('CYCLE_ID', StringType(), True),
        StructField('RUN_ID', IntegerType(), True),
        StructField('DATASET_ID', IntegerType(), True),
        StructField('ROW_ID', IntegerType(), True),
        StructField('QC_ID', IntegerType(), True),
        StructField('COLUMN_NAME', StringType(), True),
        StructField('QC_TYPE', StringType(), True),
        StructField('QC_PARAM', StringType(), True),
        StructField('QC_DESC', StringType(), True),
        StructField('CRITICALITY', StringType(), True)
      ])
      LOG_DQM_DTL_ = spark.createDataFrame(LOG_DQM_DTL_, LOGDQMDTLSchema)
      LOGDQMDTLNode = DynamicFrame.fromDF(LOG_DQM_DTL_, glueContext, 'LOGDQMDTLNode')
      print(str(DATASET_ID) + ': ' + str(DATASET_NAME) + '>> DQM checks applied')

      sql = '''
        select
          cast(D.QC_ID as string) as QC_ID,
          cast(D.QC_DESC as string) as QC_DESC,
          case when cast(D.CRITICALITY as string) like '%CRITICAL%' then 'Y' else 'N' end as CRITICALITY_FLAG, L.*
        from ( select * from landingTBL ) L
        left outer join ( select ROW_ID, collect_set(QC_ID) as QC_ID, collect_set(QC_DESC) as QC_DESC, collect_set(CRITICALITY) as CRITICALITY from dqmLOG group by ROW_ID ) D
        on cast(L.ROW_ID as int) = cast(D.ROW_ID as int)
      '''
      StagingNode = sparkSqlQuery (
        glueContext,
        query = sql,
        mapping = { 'landingTBL': LandingNode2, 'dqmLOG': LOGDQMDTLNode },
        transformation_ctx = 'StagingNode'
      )
      print(str(DATASET_ID) + ': ' + str(DATASET_NAME) + '>> Staging table generated')
                
      # Deleting existing file at staging location
      s3Resource = boto3.resource('s3')
      files = s3Resource.Bucket(args['CURATED_BUCKET']).objects.filter(Prefix = TARGET_LOCATION)
      for file in files:
        s3Resource.Object(args['CURATED_BUCKET'], file.key).delete()
      print(str(DATASET_ID) + ': ' + str(DATASET_NAME) + '>> Staging location cleared of existing file')
      
      # DynamicFrame to write target file to Staging location
      WriteStagingNodeR = StagingNode.repartition(100)
      WriteStagingNode = glueContext.write_dynamic_frame.from_options (
        frame = WriteStagingNodeR,
        connection_type = 's3',
        format = 'parquet',
        connection_options = {
          'path': 's3://' + args['CURATED_BUCKET'] + '/' + TARGET_LOCATION,
          'partitionKeys': [],
        },
        format_options = { 'compression': 'snappy' },
        transformation_ctx = 'WriteStagingNode',
      )
      print(str(DATASET_ID) + ': ' + str(DATASET_NAME) + '>> Target file written to Staging location')
      
      # DynamicFrame to write target file to Archive location
      WriteStagingNode = glueContext.write_dynamic_frame.from_options (
        frame = WriteStagingNodeR,
        connection_type = 's3',
        format = 'parquet',
        connection_options = {
          'path': 's3://'+args['CURATED_BUCKET']+'/'+ARCHIVE_LOCATION+datetime.now().strftime('%Y-%m-%d')+'/'+datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'/',
          'partitionKeys': [],
        },
        format_options = { 'compression': 'snappy' },
        transformation_ctx = 'WriteStagingNode'
      )
      print(str(DATASET_ID) + ': ' + str(DATASET_NAME) + '>> Target file written to Archive location')
      
      # Creating and starting crawler for ingested Staging table
      glueClient = boto3.client('glue')
      try:
        glueClient.delete_table(
          DatabaseName = args['CATALOG_DB_NAME'],
          Name = TARGET_TABLE_NAME
        )
        print(str(DATASET_ID) + ': ' + str(DATASET_NAME) + '>> Existing Catalog table deleted')
      except:
        pass
      try:
        glueClient.delete_crawler(
          Name = str(args['CATALOG_DB_NAME']).upper() + '_' + str(TARGET_TABLE_NAME).upper()
        )
        print(str(DATASET_ID) + ': ' + str(DATASET_NAME) + '>> Existing Crawler deleted')
      except:
        pass
      try:
        glueClient.create_crawler(
          Name = str(args['CATALOG_DB_NAME']).upper() + '_' + str(TARGET_TABLE_NAME).upper(),
          Role = args['GLUE_SERVICE_ROLE'],
          DatabaseName = args['CATALOG_DB_NAME'],
          Targets = {
            'S3Targets': [{
              'Path': 's3://' + args['CURATED_BUCKET'] + '/' + TARGET_LOCATION
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
        print(str(DATASET_ID) + ': ' + str(DATASET_NAME) + '>> Crawler successfully created')
      except:
        pass
      glueClient.start_crawler(
        Name = str(args['CATALOG_DB_NAME']).upper() + '_' + str(TARGET_TABLE_NAME).upper()
      )
      print(str(DATASET_ID) + ': ' + str(DATASET_NAME) + '>> Crawler successfully started')
      
      recordCount = StagingNode.toDF().count()
      processStatus = 'SUCCESS'
      
      # Insert entry in LOG Dataset DTL
      sql = '''INSERT INTO `LOG_DATASET_DTL` values ( '{cID}', {rID}, '{lDt}', {gID}, '{gNM}', {dID}, '{dTP}', '{dNM}', '{pNM}', '{pST}', {rCT}, '{pMG}' )'''.format(
        cID = str(self.cycleID),
        rID = str(runID),
        lDt = str(self.loadDate),
        gID = str(self.glueJobID),
        gNM = str(self.glueJobName),
        dID = str(DATASET_ID),
        dTP = str(self.datasetType),
        dNM = str(DATASET_NAME),
        pNM = str(self.processName),
        pST = str(processStatus),
        rCT = str(recordCount),
        pMG = str(processMessage)
      )
      self.cursor.execute(sql)
      self.RDSConn.commit()
      print(str(DATASET_ID) + ': ' + str(DATASET_NAME) + '>> LOG Dataset Detail updated')
      
      # Generate DQM Summary DTL
      sql = '''
        INSERT INTO `LOG_DQM_SUMMARY`
        SELECT
          `CYCLE_ID`, `RUN_ID`, `DATASET_ID`, `QC_ID`, `COLUMN_NAME`, `QC_TYPE`, `QC_PARAM`, `QC_DESC`, `CRITICALITY`,
          COUNT(*) AS `ERROR_COUNT`,
          CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END AS `QC_RESULT`
        FROM `LOG_DQM_DTL`
        GROUP BY `CYCLE_ID`, `RUN_ID`, `DATASET_ID`, `QC_ID`, `COLUMN_NAME`, `QC_TYPE`, `QC_PARAM`, `QC_DESC`, `CRITICALITY`
      '''
      self.cursor.execute(sql)
      self.RDSConn.commit()
      print(str(DATASET_ID) + ': ' + str(DATASET_NAME) + '>> LOG DQM Summary generated')
    
    # Close RDS connection and terminate Ingstion
    self.RDSConn.close()
    print('\n---===END INGESTION===---\n')
    return self.executionStatus

if __name__ == '__main__':
    executeIngestion = Ingestion().ingestFiles()
    job.commit()
