import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format
import pyspark.sql.functions as F
import sys
from datetime import datetime,timedelta

def src_to_raw():
    try:
        src_check = '(SELECT * FROM {}) sample'.format(src_table)
        df = spark.read.jdbc(url=url, table=src_check, properties=prop)
        df_ = df.withColumn('{column_name}', df.{df_column_name} + F.expr('INTERVAL 14 HOURS'))\
            .withColumn('{column_name}', df.{df_column_name} + F.expr('INTERVAL 14 HOURS'))\
            .withColumn('{column_name}', F.lit({literal_string}))\
            .withColumn('{column_name}', F.lit({literal_string}))\
            .withColumn('{column_name}', F.lit({literal_string}))
        df_.createOrReplaceTempView('temp')
        spark.sql('TRUNCATE TABLE {}'.format(raw_tbl))
        q = f'{your_query}'
        spark.sql(q)
    except Exception as e:
        print('Job Error. Check the application for further information.')
        print(str(e))
        spark.stop()
        sys.exit(1)

def raw_to_hist():
    try:
        q = f'{your_query}'
        spark.sql(q)
    except Exception as e:
        print(str(e))
        spark.stop()
        sys.exit(1)

if __name__ == '__main__':
    arg1 = sys.argv[1]
    arg2 = sys.argv[2]
    arg3 = sys.argv[3]
    arg4 = sys.argv[4]
    arg5 = sys.argv[5]
    arg6 = sys.argv[6]
    arg7 = sys.argv[7]
    arg8 = sys.argv[8]
    arg9 = sys.argv[9]
    arg10 = sys.argv[10]
    arg11 = sys.argv[11]
    
    url = 'jdbc:mysql://{0}:{1}/{2}'.format(host, port, schema)
    prop = {'user': user, 'password': password, 'driver': 'com.mysql.jdbc.Driver','serverTimezone':'Asia/Jakarta'}
        
    src_table = f'{name}'
    raw_tbl = f'{name}'
    hist_tbl = f'{name}'

    spark = SparkSession.builder.appName('{spark_app_name}')\
        .config('spark.jars.packages','mysql:mysql-connector-java:8.0.17')\
        .config("parquet.compression", "SNAPPY")\
        .config('spark.executor.memory', '24g')\
        .config('hive.exec.dynamic.partition.mode','nonstrict')\
        .enableHiveSupport()\
        .getOrCreate()
    
    raw_to_hist()
    src_to_raw()    
    spark.stop()
