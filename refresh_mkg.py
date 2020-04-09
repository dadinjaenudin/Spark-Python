import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import functions as sf
from pyspark.sql.functions import collect_list, col

sparkMaster = "spark://spark-master:7077"
hiveMetastore = "thrift://localhost:9083"

def create_spark_session(app_name="Refresh MKG"):
    spark_session = SparkSession \
        .builder \
        .master("local") \
        .appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()

    spark_session.conf.set("spark.sql.shuffle.partitions", 800)
    spark_session.conf.set("spark.driver.memory", "10g")
    # spark_session.conf.set("spark.executor.instances", "100")
    spark_session.conf.set("spark.executor.memory", "100g")
    spark_session.conf.set("spark.executor.cores", "64")
    spark_session.conf.set("hive.exec.dynamic.partition", "true")
    spark_session.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark_session.conf.set("hive.enforce.bucketing", "true")
    spark_session.conf.set("hive.exec.max.dynamic.partitions.pernode", "400")
    spark_session.conf.set("hive.metastore.uris", hiveMetastore)

    spark_session.sparkContext.setLogLevel("WARN")
    return spark_session

def main():
    # ss = SparkSession
    SS = create_spark_session()
    SS.sql("USE bigdata_prd")

    # Parameter spark=submit year month
    year  = sys.argv[1]
    month = sys.argv[2]

    # ------------------------------------------------------------------------------#
    # MKG_TRN
    # ------------------------------------------------------------------------------#
    sql = '''
            ALTER TABLE fact_mkgtrn_%s DROP IF EXISTS PARTITION (year = '%s', month='%s')  
          ''' % (year,year,month)
    SS.sql(sql)

    sql = ''' 
            INSERT  INTO fact_mkgtrn_%s partition (year,month)
            SELECT 	 CONCAT(TRIM(store_code), '_',FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(date_trn),'dd/mm/yy'),'yyyymmdd'), '_',TRIM(termnmbr),'_',TRIM(transnmbr)) postra_id
                    ,trim(store_code) store_code
                    ,to_date(from_unixtime(unix_timestamp(trim(date_trn),'dd/mm/yy'),'yyyy-mm-dd')) date_trn                 
                    ,trim(termnmbr)  termnmbr 
                    ,trim(transnmbr) transnmbr 
                    ,trim(art_code) art_code
                    ,trim(pludesc) pludesc
                    ,regexp_replace(division, '\\s+', '') division
                    ,regexp_replace(skunmbr, '\\s+', '')  skunmbr
                    ,gross    
                    ,qty      
                    ,disc     
                    ,sp
                    ,year
                    ,month       
               FROM fs_mkgtrn 
              WHERE year = '%s'
                AND month = '%s'
                AND LENGTH(trim(art_code))>13
          ''' % (year,year,month)

    df = SS.sql(sql)
    df.show()

    # ------------------------------------------------------------------------------#
    # MKG_TOTAL
    #  EDIT B3SOK FILE NGACO                AND to_date(from_unixtime(unix_timestamp(trim(date_trn),'dd/mm/yy'),'yyyy-mm-dd')) =
    #store | date | termnmbr | transnmbr | amnt | posgrpnmbr
    #-------+----------+----------+-----------+-----------+------------
    #118 | 01 / 01 / 01 | 59 | 67601 | 1 | 1
    #118 | 01 / 01 / 01 | 59 | 67603 | 75900 | 1
    #118 | 03 / 02 / 17 | 2 | 1112 | 135800 | 2
    # ------------------------------------------------------------------------------#
    sql = '''
            ALTER TABLE fact_mkgtotal_%s DROP IF EXISTS PARTITION (year = '%s', month='%s')
          ''' % (year,year,month)
    SS.sql(sql)

    sql = ''' 
            INSERT  INTO fact_mkgtotal_%s partition (year,month)
            SELECT 	 CONCAT(TRIM(store_code), '_',FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(date_trn),'dd/mm/yy'),'yyyymmdd'), '_',TRIM(termnmbr),'_',TRIM(transnmbr)) postra_id
                    ,trim(store_code) store_code
                    ,to_date(from_unixtime(unix_timestamp(trim(date_trn),'dd/mm/yy'),'yyyy-mm-dd')) date_trn                 
                    ,trim(termnmbr)  termnmbr 
                    ,trim(transnmbr) transnmbr 
                    ,amt    
                    ,trim(posgrpnmbr) posgrpnmbr
                    ,year
                    ,month       
               FROM fs_mkgtotal 
              WHERE year  = '%s'
                AND month = '%s'   
                AND LENGTH(trim(store_code))=3
          ''' % (year,year, month)

    df1 = SS.sql(sql)
    df1.show()

    # ------------------------------------------------------------------------------#
    # MKG_MED
    # ------------------------------------------------------------------------------#
    sql = '''
            ALTER TABLE fact_mkgmed_%s DROP IF EXISTS PARTITION (year = '%s', month='%s')
          ''' % (year,year,month)
    SS.sql(sql)

    sql = ''' 
            INSERT  INTO fact_mkgmed_%s partition (year,month)
            SELECT 	 CONCAT(TRIM(store_code), '_',FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(date_trn),'dd/mm/yy'),'yyyymmdd'), '_',TRIM(termnmbr),'_',TRIM(transnmbr)) postra_id
                    ,trim(store_code) store_code
                    ,to_date(from_unixtime(unix_timestamp(trim(date_trn),'dd/mm/yy'),'yyyy-mm-dd')) date_trn                 
                    ,trim(termnmbr)  termnmbr 
                    ,trim(transnmbr) transnmbr 
                    ,trim(mdesc)     mdesc    
                    ,mediaamnt  
                    ,trim(accountnmbr) accountnmbr
                    ,year
                    ,month       
               FROM fs_mkgmed 
              WHERE year  = '%s'
                AND month = '%s'   
                AND LENGTH(trim(store_code))=3                           
          ''' % (year,year,month)

    df2 = SS.sql(sql)
    df2.show()

    # ------------------------------------------------------------------------------#
    # MKG_MEM
    # ------------------------------------------------------------------------------#
    sql = '''
            ALTER TABLE fact_mkgmem_%s DROP IF EXISTS PARTITION (year = '%s', month='%s')
          ''' % (year,year,month)
    SS.sql(sql)

    sql = ''' 
            INSERT  INTO fact_mkgmem_%s partition (year,month)
            SELECT 	 CONCAT(TRIM(store_code), '_',FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(date_trn),'dd/mm/yy'),'yyyymmdd'), '_',TRIM(termnmbr),'_',TRIM(transnmbr)) postra_id
                    ,trim(store_code) store_code
                    ,to_date(from_unixtime(unix_timestamp(trim(date_trn),'dd/mm/yy'),'yyyy-mm-dd')) date_trn                 
                    ,trim(termnmbr)  termnmbr 
                    ,trim(transnmbr) transnmbr 
                    ,trim(member)    member    
                    ,trim(cshrnmbr)  cshrnmbr
                    ,trim(empname)   empname                    
                    ,year
                    ,month       
               FROM fs_mkgmem
              WHERE year  = '%s'
                AND month = '%s'   
                AND LENGTH(trim(store_code))=3                           
          ''' % (year,year, month)

    df3 = SS.sql(sql)
    df3.show()

    # ------------------------------------------------------------------------------#
    # MKG_SUPINT
    # ------------------------------------------------------------------------------#
    sql = '''
            ALTER TABLE fact_mkgsupint_%s DROP IF EXISTS PARTITION (year = '%s', month='%s')
          ''' % (year,year,month)
    SS.sql(sql)

    sql = ''' 
            INSERT  INTO fact_mkgsupint_%s partition (year,month)
            SELECT 	 CONCAT(TRIM(store_code), '_',FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(date_trn),'dd/mm/yy'),'yyyymmdd'), '_',TRIM(termnmbr),'_',TRIM(transnmbr)) postra_id
                    ,trim(store_code) store_code
                    ,to_date(from_unixtime(unix_timestamp(trim(date_trn),'dd/mm/yy'),'yyyy-mm-dd')) date_trn                 
                    ,trim(termnmbr)  termnmbr 
                    ,trim(transnmbr) transnmbr 
                    ,trim(supervisor)   supervisor    
                    ,trim(cshrnmbr)     cshrnmbr
                    ,trim(funcdesc)     funcdesc                    
                    ,amount
                    ,trim(spvname)      spvname                 
                    ,year
                    ,month       
               FROM fs_mkgsupint
              WHERE year  = '%s'
                AND month = '%s'
                AND LENGTH(trim(store_code))=3                              
          ''' % (year,year, month)

    df4 = SS.sql(sql)
    df4.show()

    # Stop spark Session
    SS.stop()

if __name__ == '__main__':
    main()

# run spark on Yarn
# export HADOOP_USER_NAME=hive
# spark-submit --master yarn-cluster connect_hive.py
# spark-submit --master yarn-client connect_hive.py

