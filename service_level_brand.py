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

    # RECREATE TABLE dim_supplier
    sql = ''' TRUNCATE TABLE dim_supplier '''
    df  = SS.sql(sql)

    sql = '''  
                INSERT  INTO dim_supplier
                SELECT  distinct
                        supp_code,
                        supp_desc
                   FROM fs_po
            '''
    df = SS.sql(sql)

    # Remove Space From Field
    reg  = '''\\\s+'''
    sp   = ''''''
    sql = '''
            SELECT  b.store_code,
                    a.Directorate ,
                    a.division    ,
                    a.category    ,
                    a.sub_category ,
                    REGEXP_REPLACE(a.brand, '%s', '%s') brand,
                    b.supp_code,
                    b.princ_code,  
                    b.ord_date,
                    sum(po_qty) po_qty,
                    sum(po_rp)  po_rp,
                    sum(rec_qty) rec_qty,
                    sum(rec_rp) rec_rp,
                    b.year,
                    b.month
            FROM dim_article a, fs_po b 
            WHERE b.year  = '%s'
              AND b.month = '%s'
              AND a.article_code = b.art_code
            GROUP BY b.store_code,
                    a.Directorate ,
                    a.division    ,
                    a.category    ,
                    a.sub_category ,
                    REGEXP_REPLACE(a.brand, '%s', '%s'),
                    b.supp_code,
                    b.princ_code,  
                    b.ord_date,
                    b.year,
                    b.month    
        ''' % (reg,sp,year,month,reg,sp)

    # this query is current Month
    df_year = SS.sql(sql)
    df_year.createOrReplaceTempView("df_this_year")
    df_year.cache()
    df_year.show(10)


    sql = '''
            SELECT  b.store_code,
                    a.Directorate ,
                    a.division    ,
                    a.category    ,
                    a.sub_category ,
                    REGEXP_REPLACE(a.brand, '%s', '%s')  brand,
                    b.supp_code,
                    b.princ_code,  
                    b.ord_date,
                    sum(po_qty) po_qty,
                    sum(po_rp)  po_rp,
                    sum(rec_qty) rec_qty,
                    sum(rec_rp) rec_rp
            FROM dim_article a, fs_po b 
            WHERE b.year  = '%s'
              AND b.month = '%s'
              AND a.article_code = b.art_code
            GROUP BY b.store_code,
                    a.Directorate ,
                    a.division    ,
                    a.category    ,
                    a.sub_category ,
                    REGEXP_REPLACE(a.brand, '%s', '%s') ,
                    b.supp_code,
                    b.princ_code,  
                    b.ord_date
        ''' % (reg,sp,year_prev,month,reg,sp)

    # this query is current Month
    df_year_prev = SS.sql(sql)
    df_year_prev.createOrReplaceTempView("df_prev_year")
    df_year_prev.cache()
    df_year_prev.show(10)

    sql = '''  
                WITH 
                transaksi_current as ( 
                                SELECT  store_code,
                                        Directorate ,
                                        division    ,
                                        category    ,
                                        sub_category ,
                                        brand,
                                        supp_code,
                                        princ_code,  
                                        ord_date,
                                        sum(po_qty) po_qty,
                                        sum(po_rp)  po_rp,
                                        sum(rec_qty) rec_qty,
                                        sum(rec_rp) rec_rp,
                                        year,
                                        month
                                FROM df_this_year 
                                GROUP BY store_code,
                                        Directorate ,
                                        division    ,
                                        category    ,
                                        sub_category ,
                                        brand,
                                        supp_code,
                                        princ_code,  
                                        ord_date,
                                        year,
                                        month
                                   ),
                transaksi_prev as ( 
                                    SELECT  store_code,
                                            Directorate ,
                                            division    ,
                                            category    ,
                                            sub_category ,
                                            brand,
                                            ord_date,                                            
                                            sum(po_qty) po_qty_dtd,
                                            sum(po_rp)  po_rp_dtd,
                                            sum(rec_qty) rec_qty_dtd,
                                            sum(rec_rp) rec_rp_dtd
                                    FROM df_prev_year 
                                    GROUP BY store_code,
                                            Directorate ,
                                            division    ,
                                            category    ,
                                            sub_category ,
                                            brand,
                                            ord_date  
                                   )                                      
                SELECT   a.store_code
                        ,a.Directorate 
                        ,a.division    
                        ,a.category    
                        ,a.sub_category 
                        ,a.brand
                        ,a.supp_code
                        ,a.princ_code
                        ,a.ord_date  
                        ,a.po_qty    po_qty
                        ,a.po_rp     po_rp
                        ,a.rec_qty   rec_qty
                        ,a.rec_rp    rec_rp
                        ,b.po_qty_dtd    po_qty_dtd
                        ,b.po_rp_dtd     po_rp_dtd
                        ,b.rec_qty_dtd   rec_qty_dtd
                        ,b.rec_rp_dtd    rec_rp_dtd                          
                        ,a.year
                        ,a.month
                  from transaksi_current a FULL OUTER JOIN 
                       transaksi_prev b
                   ON (a.store_code  = b.store_code
                  AND substring(a.ord_date,6,5) = substring(b.ord_date,6,5)
                  AND a.Directorate = b.Directorate
                  AND a.division    = b.division    
                  AND a.category    = b.category    
                  AND a.sub_category= b.sub_category
                  AND a.brand       = b.brand )                   
            '''

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
                ,concat(substring(ord_date,1,4),substring(ord_date,6,2)) month_trn
                ,sum(po_qty) po_qty
                ,sum(po_rp)  po_rp
                ,sum(rec_qty) rec_qty
                ,sum(rec_rp) rec_rp  
                ,sum(po_qty_dtd)    po_qty_dtd
                ,sum(po_rp_dtd)     po_rp_dtd
                ,sum(rec_qty_dtd)   rec_qty_dtd
                ,sum(rec_rp_dtd)    rec_rp_dtd  
                ,year
                ,month
              FROM df_jml_transaksi_temp
             GROUP BY store_code
                ,Directorate 
                ,division    
                ,category    
                ,sub_category 
                ,brand
                ,supp_code
                ,princ_code  
                ,concat(substring(ord_date,1,4),substring(ord_date,6,2))
                ,year 
                ,month
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
                ,substring(ord_date,1,4) year_trn
                ,sum(po_qty) po_qty
                ,sum(po_rp)  po_rp
                ,sum(rec_qty) rec_qty
                ,sum(rec_rp) rec_rp  
                ,sum(po_qty_dtd)    po_qty_dtd
                ,sum(po_rp_dtd)     po_rp_dtd
                ,sum(rec_qty_dtd)   rec_qty_dtd
                ,sum(rec_rp_dtd)    rec_rp_dtd  
                ,year
              FROM df_jml_transaksi_temp
             GROUP BY store_code
                ,Directorate 
                ,division    
                ,category    
                ,sub_category 
                ,brand
                ,supp_code
                ,princ_code  
                ,substring(ord_date,1,4)
                ,year 
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


