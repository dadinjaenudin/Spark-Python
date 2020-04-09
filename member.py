from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext

sparkMaster = "spark://spark-master:7077"
hiveMetastore = "thrift://localhost:9083"

def create_spark_session(app_name="Sales_Gold_Application"):
    spark_session = SparkSession \
        .builder \
        .master("local") \
        .appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()

    spark_session.conf.set("spark.sql.shuffle.partitions", 6)
    # spark_session.conf.set("spark.driver.memory", "1g")
    spark_session.conf.set("spark.executor.memory", "10g")
    spark_session.conf.set("spark.executor.cores", "1")
    spark_session.conf.set("hive.exec.dynamic.partition", "true")
    spark_session.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark_session.conf.set("hive.enforce.bucketing", "true")
    spark_session.conf.set("hive.metastore.uris", hiveMetastore)

    spark_session.sparkContext.setLogLevel("WARN")
    return spark_session

def main():
    # ss = SparkSession
    SS = create_spark_session()

    '''
    DROP TABLE fs_ymc_card;
    
    CREATE EXTERNAL TABLE IF NOT EXISTS fs_ymc_card(
    MEMBER_ID       string,
    FULLNAME        string,
    CARD_ID         string,
    EFFECTIVE_DATE  date,
    EXPIRED_DATE    date
    )
    ROW FORMAT DELIMITED
     FIELDS TERMINATED BY '|'
     STORED AS TEXTFILE
     LOCATION '/yogya-bigdata-prd/fact_ymc_card'
     tblproperties ("skip.header.line.count"="1");
    
    DROP TABLE fs_cust360;
    
    CREATE EXTERNAL TABLE IF NOT EXISTS fs_cust360(
        isedb            string,    
        Member_ID          string,
        FullName            string,
        Issue_Date          date,
        Joined_Store_Code   string,
        Joined_Store_Name   string,
        HP                  string,
        Tlp                 string,
        Street              string,
        Kelurahan           string,
        Kecamatan           string,
        City                string,
        Province            string,
        Postcode            string,
        Email               string,
        Marital_Status      string,
        Religion            string,
        Occupation          string,
        DOB                 date,
        Gender              string,
        Effective_Date      date,
        Expiry_Date         date,
        Card_Status         string,
        No_of_Sons          string,
        No_of_Daughters     string
    )
    ROW FORMAT DELIMITED
     FIELDS TERMINATED BY '|'
     STORED AS TEXTFILE
     LOCATION '/yogya-bigdata-prd/cust360'
     tblproperties ("skip.header.line.count"="1");
    
    DROP TABLE fact_ymc_card;
    
    CREATE TABLE fact_ymc_card ( 
    MEMBER_ID       string,
    FULLNAME        string,
    CARD_ID         string,
    EFFECTIVE_DATE  date,
    EXPIRED_DATE    date
    )
    CLUSTERED BY(MEMBER_ID) INTO 3 BUCKETS
    STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');
    
    '''

    SS.sql("USE bigdata_prd")

    SS.sql("TRUNCATE TABLE fact_ymc_card")
    sql = ''' INSERT INTO  fact_ymc_card
                   SELECT *
                     FROM  fs_ymc_card
          '''
    df = SS.sql(sql)

    # Stop spark Session
    SS.stop()

if __name__ == '__main__':
    main()

# run spark on Yarn
# export HADOOP_USER_NAME=hive
# spark-submit --master yarn-cluster connect_hive.py
# spark-submit --master yarn-client connect_hive.py
