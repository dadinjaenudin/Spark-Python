import sys
from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import functions as sf
from pyspark.sql.functions import collect_list, col

sparkMaster = "spark://spark-master:7077"
hiveMetastore = "thrift://localhost:9083"

def create_spark_session(app_name="Service Level"):
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


    ''''
    NOTE : Where Kondisi
         -- INITIAL - USE LAST_DAY                     
            AND b.ord_date    <= LAST_DAY(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(concat('2017','-','01','-','01') ,'yyyy-MM-dd'),'yyyy-MM-dd')))
          --DAILY   - USE CURRENT
          --AND b.ord_date    <= TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(concat('2017','-','01','-',substring(CURRENT_DATE(),9,2)) ,'yyyy-MM-dd'),'yyyy-MM-dd')) 
    '''
    sql = '''  
                WITH transaksi as (
                SELECT  '1' date_year,
                        b.store_code,
                        a.Directorate ,
                        a.division    ,
                        a.category    ,
                        a.sub_category ,
                        a.brand,
                        b.supp_code,
                        b.princ_code,  
                        sum(po_qty) po_qty,
                        sum(po_rp)  po_rp,
                        sum(rec_qty) rec_qty,
                        sum(rec_rp) rec_rp
                FROM dim_article a, fs_po b 
                WHERE b.ord_date    >= TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(concat('%s','-','%s','-','01') ,'yyyy-MM-dd'),'yyyy-MM-dd')) 
                  AND b.ord_date    <= LAST_DAY(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(concat('%s','-','%s','-','01') ,'yyyy-MM-dd'),'yyyy-MM-dd')))
                  AND a.article_code = b.art_code
                GROUP BY b.store_code,
                        a.Directorate ,
                        a.division    ,
                        a.category    ,
                        a.sub_category ,
                        a.brand,
                        b.supp_code,
                        b.princ_code  
                UNION ALL
                -- PREV YEAR
                SELECT  '2' date_year,
                        b.store_code,
                        a.Directorate ,
                        a.division    ,
                        a.category    ,
                        a.sub_category ,
                        a.brand,
                        b.supp_code,
                        b.princ_code,  
                        sum(po_qty) po_qty,
                        sum(po_rp)  po_rp,
                        sum(rec_qty) rec_qty,
                        sum(rec_rp) rec_rp
                FROM dim_article a, fs_po b 
                WHERE b.ord_date    >= TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(concat('%s','-','%s','-','01') ,'yyyy-MM-dd'),'yyyy-MM-dd')) 
                  AND b.ord_date    <= LAST_DAY(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(concat('%s','-','%s','-','01') ,'yyyy-MM-dd'),'yyyy-MM-dd')))
                  AND a.article_code = b.art_code
                GROUP BY b.store_code,
                        a.Directorate ,
                        a.division    ,
                        a.category    ,
                        a.sub_category ,
                        a.brand,
                        b.supp_code,
                        b.princ_code  
                )
                SELECT  store_code,
                        Directorate ,
                        division    ,
                        category    ,
                        sub_category ,
                        brand,
                        supp_code,
                        princ_code,  
                        max(case when date_year=1 then po_qty end) po_qty ,
                        max(case when date_year=1 then po_rp  end) po_rp ,
                        max(case when date_year=1 then rec_qty end) rec_qty ,
                        max(case when date_year=1 then rec_rp  end) rec_rp ,
                        max(case when date_year=2 then po_qty end) po_qty_prev ,
                        max(case when date_year=2 then po_rp  end) po_rp_prev ,
                        max(case when date_year=2 then rec_qty end) rec_qty_prev ,
                        max(case when date_year=2 then rec_rp  end) rec_rp_prev,
                        max('%s')   year_current,
                        max('%s')   month_current
                  FROM transaksi
                GROUP BY 
                        store_code,
                        Directorate ,
                        division,
                        category,
                        sub_category,
                        brand,
                        supp_code,
                        princ_code
            ''' % (year, month, year, month, year_prev, month, year_prev, month, year, month)

    # this query is current Month
    df = SS.sql(sql)
    df.createOrReplaceTempView("df_jml_transaksi_temp")
    df.cache()
    df.show(10)

    # ------------------------------------------------------------------------------#
    # fact_JmlTrans_byDate_
    # using where in case in file mkg there is wrong year in the file
    # ------------------------------------------------------------------------------#

    # sql = '''
    #         ALTER TABLE fact_po_rec_bydate_%s DROP IF EXISTS PARTITION (year = '%s', month='%s')
    #       ''' % (year,year,month)
    # SS.sql(sql)
    #
    # sql = '''
    #         INSERT  INTO fact_po_rec_bydate_%s partition (year,month)
    #         SELECT
    #              store_code
    #             ,Directorate
    #             ,division
    #             ,category
    #             ,sub_category
    #             ,brand
    #             ,supp_code
    #             ,princ_code
    #             ,ord_date
    #             ,sum(po_qty) po_qty
    #             ,sum(po_rp)  po_rp
    #             ,sum(rec_qty) rec_qty
    #             ,sum(rec_rp) rec_rp
    #             ,sum(po_qty_dtd)    po_qty_dtd
    #             ,sum(po_rp_dtd)     po_rp_dtd
    #             ,sum(rec_qty_dtd)   rec_qty_dtd
    #             ,sum(rec_rp_dtd)    rec_rp_dtd
    #             ,year
    #             ,month
    #           FROM df_jml_transaksi_temp
    #           WHERE year = '%s'
    #          GROUP BY store_code
    #             ,Directorate
    #             ,division
    #             ,category
    #             ,sub_category
    #             ,brand
    #             ,supp_code
    #             ,princ_code
    #             ,ord_date
    #             ,year
    #             ,month
    #       ''' % (year,year)
    #
    # df1 = SS.sql(sql)
    # df1.show(20)

    # ------------------------------------------------------------------------------#
    # fact_JmlTrans_byMonth
    # ------------------------------------------------------------------------------#
    sql = '''
            ALTER TABLE fact_po_rec_bymonth_%s DROP IF EXISTS PARTITION (year = '%s', month='%s')  
          ''' % (year,year,month)
    SS.sql(sql)

    sql = ''' 
            INSERT  INTO fact_po_rec_bymonth_%s partition (year,month)
            SELECT 
                 store_code
                ,Directorate 
                ,division 
                ,category
                ,sub_category
                ,brand
                ,supp_code
                ,princ_code          
                ,concat(substring(year_current,1,4),substring(month_current,1,2)) month_trn
                ,sum(po_qty) po_qty
                ,sum(po_rp)  po_rp
                ,sum(rec_qty) rec_qty
                ,sum(rec_rp) rec_rp  
                ,sum(po_qty_prev)    po_qty_dtd
                ,sum(po_rp_prev)     po_rp_dtd
                ,sum(rec_qty_prev)   rec_qty_dtd
                ,sum(rec_rp_prev)    rec_rp_dtd  
                ,max(year_current)
                ,max(month_current)
              FROM df_jml_transaksi_temp
             GROUP BY store_code
                ,Directorate 
                ,division   
                ,category
                ,sub_category
                ,brand
                ,supp_code
                ,princ_code  
                ,concat(substring(year_current,1,4),substring(month_current,1,2))        
          ''' % (year)

    df2 = SS.sql(sql)
    df2.show(20)

    # ------------------------------------------------------------------------------#
    # fact_JmlTrans_byYear
    # Must be processed from fact_JmlTrans_byMonth
    # ------------------------------------------------------------------------------#
    sql = '''
            ALTER TABLE fact_po_rec_byyear_%s DROP IF EXISTS PARTITION (year = '%s')  
          ''' % (year,year)
    SS.sql(sql)

    sql = ''' 
            INSERT  INTO fact_po_rec_byyear_%s partition (year)
            SELECT 
                 store_code
                ,Directorate 
                ,division    
                ,category
                ,sub_category
                ,brand
                ,supp_code
                ,princ_code       
                ,substring(year_current,1,4) year_trn
                ,sum(po_qty) po_qty
                ,sum(po_rp)  po_rp
                ,sum(rec_qty) rec_qty
                ,sum(rec_rp) rec_rp  
                ,sum(po_qty_prev)    po_qty_dtd
                ,sum(po_rp_prev)     po_rp_dtd
                ,sum(rec_qty_prev)   rec_qty_dtd
                ,sum(rec_rp_prev)    rec_rp_dtd  
                ,max(year_current)
              FROM df_jml_transaksi_temp
             GROUP BY store_code
                ,Directorate 
                ,division 
                ,category
                ,sub_category
                ,brand
                ,supp_code
                ,princ_code                             
                ,supp_code
                ,substring(year_current,1,4)
          '''% (year)

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


