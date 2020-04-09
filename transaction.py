import sys
from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import functions as sf
from pyspark.sql.functions import collect_list, col

sparkMaster = "spark://spark-master:7077"
hiveMetastore = "thrift://localhost:9083"

def create_spark_session(app_name="Jumlah Transaksi"):
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

    year  = sys.argv[1]
    month = sys.argv[2]

    dt = datetime.strptime(year, '%Y')
    year_prev = dt.year - 1

    print year
    print year_prev

    sql = '''  
            SELECT  a.date_trn,
                    a.store_code,
                    a.posgrpnmbr grp_kode,
                    amt jml_trans,
                    jml_bs,
                    a.year,
                    a.month
              From
                    (
                    SELECT  store_code,
                            postra_id,
                            date_trn,
                            posgrpnmbr,
                            year,
                            month,
                            count(*) amt
                     FROM fact_MkgTotal
                    WHERE year  = '%s'
                      AND substring(date_trn,1,4) = '%s'    
                      AND month = '%s'
                    GROUP BY store_code, postra_id, date_trn, posgrpnmbr, year,month
                     )  a
                     JOIN 
                    (
                    SELECT  store_code,
                            postra_id,    
                            date_trn,
                            year,
                            month,
                            sum(gross-disc)  jml_bs
                     FROM fact_MkgTrn
                    WHERE year  = '%s'
                      AND substring(date_trn,1,4) = '%s'    
                      AND month = '%s'   
                    GROUP BY store_code, postra_id, date_trn, year, month
                    ) b
                    ON ( a.store_code   = b.store_code AND
                         a.date_trn     = b.date_trn   AND
                         a.postra_id    = b.postra_id
                      )
            ''' % (year,year,month, year,year,month)

    # this query is current Month
    df_year = SS.sql(sql)
    df_year.createOrReplaceTempView("df_transaction_year")
    df_year.cache()
    df_year.show(10)


    sql = '''  
            SELECT  a.date_trn,
                    a.store_code,
                    a.posgrpnmbr grp_kode,
                    amt jml_trans,
                    jml_bs,
                    a.year,
                    a.month
              From
                    (
                    SELECT  store_code,
                            postra_id,
                            date_trn,
                            posgrpnmbr,
                            year,
                            month,
                            count(*) amt
                     FROM fact_MkgTotal
                    WHERE year  = '%s'
                      AND substring(date_trn,1,4) = '%s'    
                      AND month = '%s'
                    GROUP BY store_code, postra_id, date_trn, posgrpnmbr, year,month
                     )  a
                     JOIN 
                    (
                    SELECT  store_code,
                            postra_id,    
                            date_trn,
                            year,
                            month,
                            sum(gross-disc)  jml_bs
                     FROM fact_MkgTrn
                    WHERE year  = '%s'
                      AND substring(date_trn,1,4) = '%s'    
                      AND month = '%s'   
                    GROUP BY store_code, postra_id, date_trn, year, month
                    ) b
                    ON ( a.store_code   = b.store_code AND
                         a.date_trn     = b.date_trn   AND
                         a.postra_id    = b.postra_id
                      )
            ''' % (year_prev,year_prev,month, year_prev,year_prev,month)

    # this query is current Month
    df_year_prev = SS.sql(sql)
    df_year_prev.createOrReplaceTempView("df_transaction_year_prev")
    df_year_prev.cache()
    df_year_prev.show(10)


    sql = '''  
                with 
                transaksi_current as ( 
                                  SELECT  date_trn 
                                    ,store_code
                                    ,grp_kode
                                    ,sum(jml_trans) jml_trans
                                    ,sum(jml_bs)    jml_bs
                                    ,year
                                    ,month
                                  FROM df_transaction_year
                                 WHERE year  = '%s'
                                   AND month = '%s'  
                              GROUP BY date_trn,
                                       store_code,
                                       grp_kode,
                                       year,
                                       month
                                   ),
                transaksi_prev as ( 
                                  SELECT  date_trn 
                                    ,store_code
                                    ,grp_kode
                                    ,sum(jml_trans) jml_trans_dtd
                                    ,sum(jml_bs)    jml_bs_dtd
                                  FROM df_transaction_year_prev
                                 WHERE year  = '%s'
                                   AND month = '%s'   
                              GROUP BY date_trn,
                                       store_code,
                                       grp_kode
                                   )                                      
                select a.date_trn 
                      ,a.store_code
                      ,a.grp_kode
                      ,b.jml_trans_dtd
                      ,b.jml_bs_dtd
                      ,a.jml_trans
                      ,a.jml_bs
                      ,a.year
                      ,a.month
                  from transaksi_current a FULL OUTER JOIN 
                       transaksi_prev b
                ON a.store_code = b.store_code
                  AND substring(a.date_trn,6,5) = substring(b.date_trn,6,5)
                  AND a.grp_kode   = b.grp_kode                  
            ''' % (year,month,year_prev,month)

    # this query is current Month
    df = SS.sql(sql)
    df.createOrReplaceTempView("df_jml_transaksi_temp")
    df.cache()
    df.show(10)

# ------------------------------------------------------------------------------#
    # fact_JmlTrans_byDate_
    # using where in case in file mkg there is wrong year in the file
    # ------------------------------------------------------------------------------#
    sql = '''
            ALTER TABLE fact_Transaction_byDate_%s DROP IF EXISTS PARTITION (year = '%s', month='%s')  
          ''' % (year,year,month)
    SS.sql(sql)

    sql = ''' 
            INSERT  INTO fact_Transaction_byDate_%s partition (year,month)
            SELECT 
                     date_trn
                    ,store_code
                    ,grp_kode
                    ,sum(jml_trans_dtd)
                    ,sum(jml_bs_dtd)
                    ,sum(jml_trans)
                    ,sum(jml_bs)
                    ,year
                    ,month
              FROM df_jml_transaksi_temp
              WHERE year = '%s'
             GROUP BY date_trn,
                      store_code,
                      grp_kode,
                      year, 
                      month
          ''' % (year,year)

    df1 = SS.sql(sql)
    df1.show(20)

    # ------------------------------------------------------------------------------#
    # fact_JmlTrans_byMonth
    # ------------------------------------------------------------------------------#
    sql = '''
            ALTER TABLE fact_Transaction_byMonth_%s DROP IF EXISTS PARTITION (year = '%s', month='%s')  
          ''' % (year,year,month)
    SS.sql(sql)

    sql = ''' 
            INSERT  INTO fact_Transaction_byMonth_%s partition (year,month)
            SELECT 
                    concat(substring(date_trn,1,4),substring(date_trn,6,2)) month_trn
                    ,store_code
                    ,grp_kode
                    ,sum(jml_trans_dtd)
                    ,sum(jml_bs_dtd)
                    ,sum(jml_trans)
                    ,sum(jml_bs)
                    ,year
                    ,month
              FROM df_jml_transaksi_temp
             WHERE year = '%s'
             GROUP BY concat(substring(date_trn,1,4),substring(date_trn,6,2)),
                      store_code,
                      grp_kode,
                      year, 
                      month                      
          ''' % (year,year)

    df2 = SS.sql(sql)
    df2.show(20)

    # ------------------------------------------------------------------------------#
    # fact_JmlTrans_byYear
    # Must be processed from fact_JmlTrans_byMonth
    # ------------------------------------------------------------------------------#
    sql = '''
            ALTER TABLE fact_Transaction_byYear_%s DROP IF EXISTS PARTITION (year = '%s')  
          ''' % (year,year)
    SS.sql(sql)

    sql = ''' 
            INSERT  INTO fact_Transaction_byYear_%s partition (year)
            SELECT 
                    substring(month_trn,1,4) year_trn
                    ,store_code
                    ,grp_kode
                    ,sum(jml_trans_mtd)
                    ,sum(jml_bs_mtd)
                    ,sum(jml_trans)
                    ,sum(jml_bs)
                    ,year
              FROM fact_Transaction_byMonth 
             WHERE year  = '%s'
             GROUP BY substring(month_trn,1,4),
                      store_code,
                      grp_kode,
                      year                        
          '''% (year,year)

    df3 = SS.sql(sql)
    df3.show(20)

    # Stop spark Session
    SS.stop()

if __name__ == '__main__':
    main()

# run spark on Yarn
# export HADOOP_USER_NAME=hive
# spark-submit --master yarn-cluster connect_hive.py
# spark-submit --master yarn-client connect_hive.py

