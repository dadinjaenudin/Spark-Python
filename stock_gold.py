import sys
from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import functions as sf

sparkMaster = "spark://spark-master:7077"
hiveMetastore = "thrift://localhost:9083"

def create_spark_session(app_name="Stock Gold"):
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

    # ------------------------------------------------------------------------------#
    # Get Data form fs_fact_sales_gold
    # hdfs dfs -ls /apps/hive/warehouse/bigdata_prd.db/fact_sales_gold_2016
    # ------------------------------------------------------------------------------#
    # Parameter spark=submit year month
    year  = sys.argv[1]
    month = sys.argv[2]

    sql_date = '''  
           SELECT DISTINCT TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(date_trn,'ddMMyyyy'),'yyyy-MM-dd')) date_trn
             FROM fs_sales_gold             
            WHERE year    = '%s' 
              AND month   = '%s'
              AND site <> 'SITE'
            ''' % (year, month)

    # this query is current Month
    df_date_current = SS.sql(sql_date)
    df_date_current.createOrReplaceTempView("df_sales_date")
    df_date_current.cache()
    df_date_current.show(31)

    sql_date = '''  
           SELECT max(date_trn) date_trn
             FROM fs_stock_gold             
            WHERE year    = '%s' 
              AND month   = '%s'
            ''' % (year, month)

    # this query is current Month
    df_date_prev = SS.sql(sql_date)
    df_date_prev.createOrReplaceTempView("df_stock_date")
    df_date_prev.cache()
    df_date_prev.show(31)

    sql_daily = '''  
    WITH                  
        transaksi as (
                SELECT  '1' source,
                        c.date_trn, 
                        b.store_code,
                        a.Directorate ,
                        a.division    ,
                        a.category    ,
                        a.sub_category ,
                        a.class,
                        a.brand,
                        b.princ_code,  
                        b.art_type,
                        sum(qty)   qty,                        
                        sum(sales) sales,
                        sum(cogs)  cogs,
                        sum(ppn)   ppn
                FROM dim_article a, fs_sales_gold b, df_sales_date c            
                WHERE b.year    = '%s' 
                  AND b.month   = '%s'
                  AND TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(b.date_trn,'ddMMyyyy'),'yyyy-MM-dd')) = c.date_trn                  
                  AND a.article_code = b.art_code
                  AND b.site <> 'SITE'
                  AND b.art_type='PUTUS'
                GROUP BY b.store_code,
                        a.Directorate ,
                        a.division    ,
                        a.category    ,
                        a.sub_category ,
                        a.class,
                        a.brand,
                        b.art_type,
                        b.princ_code,
                        c.date_trn
                UNION ALL
                SELECT  '2' source,
                        c.date_trn date_trn, 
                        b.store_code,
                        a.Directorate ,
                        a.division    ,
                        a.category    ,
                        a.sub_category ,
                        a.class,
                        a.brand,
                        d.princ_code,  
                        b.art_type,
                        sum(qty)   qty,                        
                        sum(sales) sales,
                        sum(costp) cogs,
                        sum(vat)   ppn
                FROM dim_article a, fs_stock_gold b, df_stock_date c, dim_principle_article d                               
                WHERE b.year     = '%s' 
                  AND b.month    = '%s'
                  AND b.date_trn = c.date_trn
                  AND a.article_code = b.art_code
                  AND d.article_code = b.art_code                  
                GROUP BY b.store_code,
                        a.Directorate ,
                        a.division    ,
                        a.category    ,
                        a.sub_category ,
                        a.class,
                        a.brand,
                        b.art_type,
                        d.princ_code,
                        c.date_trn
                )
                SELECT  store_code,
                        Directorate,
                        division   ,
                        category   ,
                        sub_category,
                        class,
                        brand,
                        art_type,
                        princ_code,  
                        max(case when source=1 then qty         end) qty,
                        max(case when source=1 then sales       end) sales,
                        max(case when source=1 then cogs        end) cogs,
                        max(case when source=1 then ppn         end) ppn,
                        max(case when source=2 then qty         end) qty_stock ,
                        max(case when source=2 then sales       end) sales_stock ,
                        max(case when source=2 then cogs        end) cogs_stock,
                        max(case when source=2 then ppn         end) ppn_stock,                                     
                        date_trn, 
                        max('%s')      year_current,
                        max('%s')      month_current
                  FROM transaksi 
                GROUP BY 
                        store_code,
                        Directorate ,
                        division,
                        category,
                        sub_category,
                        class,
                        brand,
                        art_type,
                        princ_code,
                        date_trn
            ''' % (year, month, year, month, year, month)

    print sql_daily

    df = SS.sql(sql_daily)
    df.createOrReplaceTempView("fs_stock_gold_temp")
    df.cache()
    df.show(30)

    ''' ===========================================================
        # fact_stock_gold_byDate
        # Tidak di perlukan cukup per month dengan data diambil 
        # sesuai tanggal terakhir dalam satu bulan
        ===========================================================
    '''

    # sql = '''
    #         ALTER TABLE fact_stock_gold_bydate_%s DROP IF EXISTS PARTITION (year = '%s', month='%s')
    #       ''' % (year,year,month)
    # SS.sql(sql)
    #
    # sql = '''
    #         INSERT  INTO fact_stock_gold_bydate_%s partition (year,month)
    #          SELECT
    #                 store_code
    #                 ,date_trn
    #                 ,Directorate
    #                 ,division
    #                 ,category
    #                 ,sub_category
    #                 ,class
    #                 ,brand
    #                 ,art_type
    #                 ,princ_code
    #                 ,sum(sales)
    #                 ,sum(cogs)
    #                 ,sum(ppn)
    #                 ,sum(qty)
    #                 ,sum(sales_stock)
    #                 ,sum(cogs_stock)
    #                 ,sum(ppn_stock)
    #                 ,sum(qty_stock)
    #                 ,((sum(cogs_stock)/sum(sales) ) *
    #                                         ( SELECT  count(*) as jml_hari
    #                                             FROM df_sales_date
    #                                         )
    #                 )
    #                 ,year_current
    #                 ,month_current
    #            FROM fs_stock_gold_temp
    #        GROUP BY
    #                  store_code
    #                 ,date_trn
    #                 ,Directorate
    #                 ,division
    #                 ,category
    #                 ,sub_category
    #                 ,class
    #                 ,brand
    #                 ,art_type
    #                 ,princ_code
    #                 ,year_current
    #                 ,month_current
    #         ''' %(year)
    # df2 = SS.sql(sql)
    # print 'fact_stock_gold_byDate'
    # df2.show(20)


    ''' ===========================================================
        # fact_stock_gold_byMonth
        ===========================================================
    '''
    sql = '''
            ALTER TABLE fact_stock_gold_byMonth_%s DROP IF EXISTS PARTITION (year = '%s', month='%s')  
          ''' % (year,year,month)
    SS.sql(sql)

    sql = ''' 
            INSERT  INTO fact_stock_gold_byMonth_%s partition (year,month)   
             SELECT   
                    store_code
                    ,concat(substring(year_current,1,4),substring(month_current,1,2)) month_trn
                    ,Directorate
                    ,division
                    ,category
                    ,sub_category
                    ,class
                    ,brand
                    ,art_type
                    ,princ_code
                    ,sum(sales) 
                    ,sum(cogs)  
                    ,sum(ppn)   
                    ,sum(qty)   
                    ,sum(sales_stock) 
                    ,sum(cogs_stock)  
                    ,sum(ppn_stock)   
                    ,sum(qty_stock)   
                    ,( SELECT  count(*) as jml_hari 
                        FROM df_sales_date 
                    )  jml_hari 
                    ,year_current
                    ,month_current        
               FROM fs_stock_gold_temp
           GROUP BY 
                     store_code
                    ,concat(substring(year_current,1,4),substring(month_current,1,2))    
                    ,Directorate
                    ,division
                    ,category
                    ,sub_category
                    ,class
                    ,brand
                    ,art_type
                    ,princ_code
                    ,year_current
                    ,month_current
            ''' %(year)
    df3 = SS.sql(sql)

    print 'fact_stock_gold_byMonth'
    df3.show(5)

    ''' ===========================================================
        # fact_stock_gold_byYear
        ===========================================================
    '''
    sql = '''
            ALTER TABLE fact_stock_gold_byYear_%s DROP IF EXISTS PARTITION (year = '%s')  
          ''' % (year,year)
    SS.sql(sql)

    sql = ''' 
            INSERT  INTO fact_stock_gold_byYear_%s partition (year)   
             SELECT   
                    store_code
                    ,year_current
                    ,Directorate
                    ,division
                    ,category
                    ,sub_category
                    ,class
                    ,brand
                    ,art_type
                    ,princ_code
                    ,sum(sales)
                    ,sum(cogs)
                    ,sum(ppn)
                    ,sum(qty)
                    ,sum(sales_stock)
                    ,sum(cogs_stock)
                    ,sum(ppn_stock)
                    ,sum(qty_stock)
                    ,( SELECT  count(*) as jml_hari 
                        FROM df_sales_date 
                    )  jml_hari 
                    ,year_current
               FROM fs_stock_gold_temp
           GROUP BY 
                     store_code
                    ,Directorate
                    ,division
                    ,category
                    ,sub_category
                    ,class
                    ,brand
                    ,art_type
                    ,princ_code
                    ,year_current
            ''' %(year)
    df4 = SS.sql(sql)

    print 'fact_stock_gold_byYear'
    df4.show(5)

    # Stop spark Session
    SS.stop()

if __name__ == '__main__':
    main()

# run spark on Yarn
# export HADOOP_USER_NAME=hive
# spark-submit --master yarn-cluster connect_hive.py
# spark-submit --master yarn-client connect_hive.py
