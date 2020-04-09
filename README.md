This Scripts created when I was working on Oracle Big data Cloud to transform data from csv file into hdfs and then hive and Oracle Data Visualization

# Spark Python, hdfs, Hive


### Transfer local file  to Cloud

```
#!/bin/sh

. ~/.profile

############################################################################
# Upload MKG to cloud
# 00 21 * * * /u07/mkg/upload_mkg.sh >>/u07/mkg/upload_mkg.sh.log
############################################################################


# In order to "zip" a directory, the correct command would be
# tar -cvf archive.tar directory/
# gzip archive.tar
# To decompress and unpack the archive into the current directory you would use
# tar -zxvf archive.tar.gz

LOG_FILE=/u07/mkg/log_hdfs/log-upload-hdfs-`date +"%Y%m%d"`.log
PTH=/data/yogyadata/mkg

echo "Started upload to cloud  `date '+%Y-%m-%d %H:%M:%S'`   "
echo "Started upload to cloud  `date '+%Y-%m-%d %H:%M:%S'`   " >> $LOG_FILE

# YEAR & MONTH needed by partiion
# full year 2018
YEAR_FULL=`TZ=aaa24 date +%Y`
# year=18
YEAR=`TZ=aaa24 date +%y`
MONTH=`TZ=aaa24 date +%m`
YESTERDAY=`TZ=aaa24 date +%d%m%Y`
echo $YEAR
echo $MONTH
cd /u07/mkg
tar -cvf mkg_${YEAR}${MONTH}.tar ${YEAR}${MONTH}/
gzip -f mkg_${YEAR}${MONTH}.tar
scp -r -q mkg_${YEAR}${MONTH}.tar.gz  hdfs@129.154.85.159:${PTH}/

ssh  hdfs@129.154.85.159 << EOF
     cd ${PTH}
     tar -zxvf mkg_${YEAR}${MONTH}.tar.gz
     rm mkg_${YEAR}${MONTH}.tar.gz
     cd ${YEAR}${MONTH}
     hdfs dfs -put -f MkgMed* /yogya-bigdata-prd/fact_mkgmed/${YEAR_FULL}/${MONTH}
     hdfs dfs -put -f MkgMem* /yogya-bigdata-prd/fact_mkgmem/${YEAR_FULL}/${MONTH}
     hdfs dfs -put -f MkgSupInt* /yogya-bigdata-prd/fact_mkgsupint/${YEAR_FULL}/${MONTH}
     hdfs dfs -put -f MkgTot* /yogya-bigdata-prd/fact_mkgtotal/${YEAR_FULL}/${MONTH}
     hdfs dfs -put -f MkgTrn* /yogya-bigdata-prd/fact_mkgtrn/${YEAR_FULL}/${MONTH}
     /var/lib/hadoop-hdfs/spark-python/./run_refresh_mkg.sh ${YEAR_FULL} ${MONTH}
EOF
echo "Finish upload to cloud  `date '+%Y-%m-%d %H:%M:%S'`   "
echo "Finish upload to cloud  `date '+%Y-%m-%d %H:%M:%S'`   " >> $LOG_FILE
```


###  We can also query from Oracle to tranfer file into Cloud

```

#!/usr/bin/env bash

# ###############################################################
# Purpose       : Upload file Master to Bigdata Cloud
# Directory     : /arch/batch/BIG_DATA
# Crontab       : 00 01 * * * /arch/batch/BIG_DATA/upload_master.sh
# ###############################################################

#!/bin/sh
. ~/.profile

#generate File Article
if sqlplus -s $USERID @/arch/batch/BIG_DATA/gen_article.sql; then
    echo "OK"
else
    echo "NOK"
fi

LOG_FILE=/home/oracle/log/art_master-`date +"%Y%m%d"`.log;
DIR=/arch/batch/BIG_DATA/ART;
PTH=/data/yogyadata/article;

FILENAME=ART_MASTER.TXT

cd ${DIR}/
gzip -f $FILENAME
echo "Started upload fle ${DIR}/$FILENAME  `date '+%Y-%m-%d %H:%M:%S'`   " >> $LOG_FILE
scp -rp -q ${DIR}/$FILENAME.gz  hdfs@129.154.85.159:${PTH}/

ssh  hdfs@129.154.85.159 << EOF
   gunzip -f ${PTH}/$FILENAME.gz  >> /var/lib/hadoop-hdfs/log/article.log 2>&1
   hdfs dfs -put -f ${PTH}/$FILENAME /yogya-bigdata-prd/dim_article >> /var/lib/hadoop-hdfs/log/master.log 2>&1
   /var/lib/hadoop-hdfs/spark-python/./run_master.sh
EOF

echo "Finish upload fle ${DIR}/$FILENAME  `date '+%Y-%m-%d %H:%M:%S'`   " >> $LOG_FILE


```


###  Hive Create Table

```
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
    
```

###  Spark Python to insert data to Hive
```
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

```


### Thank you! :sheep: 


