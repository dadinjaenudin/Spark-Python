from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import functions as sf

sparkMaster = "spark://spark-master:7077"
hiveMetastore = "thrift://localhost:9083"

def create_spark_session(app_name="Gold_Application"):
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
    # Master Article to Join with Category
    # ------------------------------------------------------------------------------#
    sql = ''' SELECT    DISTINCT 
                        article_code        ,
                        article_desc        ,
                        brand               ,
                        article_type        ,
                        directorate         ,
                        directorate_desc    ,
                        division            ,
                        division_desc       ,
                        category            ,
                        category_desc       ,
                        sub_category        ,
                        sub_category_desc   ,
                        class               ,
                        class_desc          ,
                        sub_class           ,
                        sub_class_desc      
                FROM dim_article
          '''
    df_article = SS.sql(sql)
    # df_article.show()
    df_article.createOrReplaceTempView("article_temp")
    df_article.cache()

    # ------------------------------------------------------------------------------#
    # Get Data form fs_fact_sales_gold
    # hdfs dfs -ls /apps/hive/warehouse/bigdata_prd.db/fact_sales_gold_2016
    # ------------------------------------------------------------------------------#
    year  = '2018'
    #month = '01'
    sql_initial = '''  SELECT  site            
                        ,art_code        
                        ,art_desc        
                        ,subclass        
                        ,brand_code      
                        ,brand_desc      
                        ,princ_code      
                        ,princ_desc      
                        ,priv_label      
                        ,art_type        
                        ,sales           
                        ,cogs            
                        ,ppn             
                        ,qty             
                        ,store_code      
                        ,date_trn
                        ,year
                        ,month    
                 FROM  fs_sales_gold  
                WHERE SITE <> 'SITE'
                  AND year  = '%s'
            ''' % (year)

    #df = SS.sql(sql_daily)
    df = SS.sql(sql_initial)
    df.createOrReplaceTempView("fs_sales_gold_temp")
    df.cache()
    df.show(10)

    print 'queyr'
    print sql_initial

    ''' ===========================================================
        # fact_sales_gold
        ===========================================================
    '''
    sql = '''
            ALTER TABLE fact_sales_gold_%s DROP IF EXISTS PARTITION (year = '%s')  
          ''' % (year,year)
    SS.sql(sql)

    sql = ''' INSERT INTO  fact_sales_gold_%s partition (year,month)
                SELECT  store_code      ,
                        date_trn        ,
                        site            ,
                        art_code        ,
                        art_desc        ,
                        subclass        ,
                        brand_code      ,
                        brand_desc      ,
                        princ_code      ,
                        princ_desc      ,
                        priv_label      ,
                        art_type        ,
                        sales           ,
                        cogs            ,
                        ppn             ,
                        qty             ,
                        year            ,
                        month
                 FROM  fs_sales_gold_temp
            ''' %(year)

    print 'insert into '
    print sql
    df1 = SS.sql(sql)

    ''' ===========================================================
        # fact_sales_gold_byDate
        ===========================================================
    '''
    sql = '''
            ALTER TABLE fact_sales_gold_bydate_%s DROP IF EXISTS PARTITION (year = '%s')  
          ''' % (year,year)
    SS.sql(sql)

    sql = ''' 
            INSERT  INTO fact_sales_gold_bydate_%s partition (year,month)   
            SELECT   store_code
                    ,date_trn
                    ,art_type
                    ,SUM(sales) sales 
                    ,SUM(cogs) cogs
                    ,SUM(ppn)  ppn
                    ,SUM(qty)  qty
                    ,year
                    ,month        
               FROM fs_sales_gold_temp
           GROUP BY year,month,store_code,date_trn,art_type
            ''' %(year)
    df2 = SS.sql(sql)

    print 'fact_sales_gold_byDate'
    df2.show(5)

    ''' ===========================================================
        # fact_sales_gold_byMonth
        ===========================================================
    '''
    sql = '''
            ALTER TABLE fact_sales_gold_byMonth_%s DROP IF EXISTS PARTITION (year = '%s')  
          ''' % (year,year)
    SS.sql(sql)

    sql = ''' 
            INSERT  INTO fact_sales_gold_byMonth_%s partition (year,month)   
            SELECT  store_code
                    ,concat(substring(date_trn,5,4),substring(date_trn,3,2))  month_trn   
                    ,art_type	
                    ,SUM(sales) sales 
                    ,SUM(cogs) cogs
                    ,SUM(ppn)  ppn
                    ,SUM(qty)  qty        
                    ,year
                    ,month        
               FROM fs_sales_gold_temp 
             GROUP BY 
                      year, 
                      month, 
                      concat(substring(date_trn,5,4),substring(date_trn,3,2)),
                      store_code,
                      art_type
            ''' %(year)
    df3 = SS.sql(sql)

    print 'fact_sales_gold_byMonth'
    df3.show(5)

    ''' ===========================================================
        # fact_sales_gold_byYear
        ===========================================================
    '''
    sql = '''
            ALTER TABLE fact_sales_gold_byYear_%s DROP IF EXISTS PARTITION (year = '%s')  
          ''' % (year,year)
    SS.sql(sql)

    sql = ''' 
            INSERT  INTO fact_sales_gold_byYear_%s partition (year)   
            SELECT  store_code
                    ,substring(month_trn,1,4)  year_trn   
                    ,art_type	
                    ,SUM(sales) sales 
                    ,SUM(cogs) cogs
                    ,SUM(ppn)  ppn
                    ,SUM(qty)  qty        
                    ,year
               FROM fact_sales_gold_byMonth_%s 
             GROUP BY 
                      year, 
                      substring(month_trn,1,4),
                      store_code,
                      art_type
            ''' %(year,year)
    df4 = SS.sql(sql)

    print 'fact_sales_gold_byYear'
    df4.show(5)


    ''' ===========================================================
        # fact_sales_gold_bydate_Dir
        ===========================================================
    '''
    sql = '''
            ALTER TABLE fact_sales_gold_bydate_Dir_%s DROP IF EXISTS PARTITION (year = '%s')  
          ''' % (year,year)
    SS.sql(sql)

    sql = ''' 
            INSERT  INTO fact_sales_gold_bydate_Dir_%s partition (year,month)   
                SELECT  store_code
                        ,date_trn
                        ,art_type	
                        ,Directorate
                        ,SUM(sales) sales
                        ,SUM(cogs) cogs
                        ,SUM(ppn)  ppn
                        ,SUM(qty)  qty        
                        ,year
                        ,month
                FROM article_temp a, fs_sales_gold_temp b
                WHERE a.article_code = b.art_code
                GROUP BY  year
                         ,month
                         ,date_trn
                         ,store_code
                         ,art_type
                         ,Directorate
            ''' %(year)
    df8 = SS.sql(sql)

    print 'fact_sales_gold_bydate_Dir'
    df8.show(5)

    ''' ===========================================================
        # fact_sales_gold_bydate_Cat
        ===========================================================
    '''
    sql = '''
            ALTER TABLE fact_sales_gold_bydate_Cat_%s DROP IF EXISTS PARTITION (year = '%s')  
          ''' % (year,year)
    SS.sql(sql)

    sql = ''' 
            INSERT  INTO fact_sales_gold_bydate_Cat_%s partition (year,month)   
                SELECT  store_code
                        ,date_trn
                        ,art_type	
                        ,category
                        ,SUM(sales) sales
                        ,SUM(cogs) cogs
                        ,SUM(ppn)  ppn
                        ,SUM(qty)  qty        
                        ,year
                        ,month
                FROM article_temp a, fs_sales_gold_temp b
                WHERE a.article_code = b.art_code
                GROUP BY  year
                         ,month
                         ,date_trn
                         ,store_code
                         ,art_type
                         ,category
            ''' %(year)
    df9 = SS.sql(sql)

    print 'fact_sales_gold_bydate_Cat'
    df9.show(5)

    ''' ===========================================================
        # fact_sales_gold_bydate_Cat
        ===========================================================
    '''
    sql = '''
            ALTER TABLE fact_sales_gold_bydate_Subcat_%s DROP IF EXISTS PARTITION (year = '%s')  
          ''' % (year,year)
    SS.sql(sql)

    sql = ''' 
            INSERT  INTO fact_sales_gold_bydate_Subcat_%s partition (year,month)   
                SELECT  store_code
                        ,date_trn
                        ,art_type	
                        ,sub_category
                        ,SUM(sales) sales
                        ,SUM(cogs) cogs
                        ,SUM(ppn)  ppn
                        ,SUM(qty)  qty        
                        ,year
                        ,month
                FROM article_temp a, fs_sales_gold_temp b
                WHERE a.article_code = b.art_code
                GROUP BY  year
                         ,month
                         ,date_trn
                         ,store_code
                         ,art_type
                         ,sub_category
            ''' %(year)
    df10 = SS.sql(sql)

    print 'fact_sales_gold_bydate_Subcat'
    df10.show(5)

    ''' ===========================================================
        # fact_sales_gold_bydate_Div
        ===========================================================
    '''
    sql = '''
            ALTER TABLE fact_sales_gold_bydate_Div_%s DROP IF EXISTS PARTITION (year = '%s')  
          ''' % (year,year)
    SS.sql(sql)

    sql = ''' 
            INSERT  INTO fact_sales_gold_bydate_Div_%s partition (year,month)   
                SELECT  store_code
                        ,date_trn
                        ,art_type	
                        ,Division
                        ,SUM(sales) sales
                        ,SUM(cogs) cogs
                        ,SUM(ppn)  ppn
                        ,SUM(qty)  qty        
                        ,year
                        ,month
                FROM article_temp a, fs_sales_gold_temp b
                WHERE a.article_code = b.art_code
                GROUP BY  year
                         ,month
                         ,date_trn
                         ,store_code
                         ,art_type
                         ,Division
            ''' %(year)
    df11 = SS.sql(sql)

    print 'fact_sales_gold_bydate_Div'
    df11.show(5)

    # Stop spark Session
    SS.stop()

if __name__ == '__main__':
    main()

# run spark on Yarn
# export HADOOP_USER_NAME=hive
# spark-submit --master yarn-cluster connect_hive.py
# spark-submit --master yarn-client connect_hive.py
