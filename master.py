
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import SQLContext

sparkMaster = "spark://spark-master:7077"
hiveMetastore = "thrift://localhost:9083"

def create_spark_session(app_name="Article_Application"):

    spark_session = SparkSession\
    .builder\
    .master("local")\
    .appName(app_name)\
    .enableHiveSupport()\
    .getOrCreate()

    spark_session.conf.set("spark.sql.shuffle.partitions", 6)
    spark_session.conf.set("spark.driver.memory", "10g")
    spark_session.conf.set("spark.executor.memory", "10g")
    spark_session.conf.set("hive.metastore.uris", hiveMetastore)

    spark_session.sparkContext.setLogLevel("WARN")
    return spark_session

def main():
    # ss = SparkSession
    SS      = create_spark_session()

    '''
    =====================================
    Create Talbe Master
    =====================================
    '''

    '''
DROP TABLE fs_article;

CREATE EXTERNAL TABLE IF NOT EXISTS fs_article(
    article_code        string,
    article_desc        string,
    brand               string,
    article_type        string,
    directorate         string,
    directorate_desc    string,
    division            string,
    division_desc       string,
    category            string,
    category_desc       string,    
    sub_category        string,
    sub_category_desc   string,
    class               string,
    class_desc          string,
    sub_class           string,
    sub_class_desc      string
  )
ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '|'
 STORED AS TEXTFILE
 LOCATION '/yogya-bigdata-prd/dim_article'
 tblproperties ("skip.header.line.count"="1");
    
    DROP TABLE fs_store;
    
    CREATE EXTERNAL TABLE IF NOT EXISTS fs_store(
        store_code              string,
        store_abbreviation      string,
        store_description       string,
        store_group             string,
        express                 string,
        selling_area            string,
        opening_date            string,    
        store_address           string,
        kelurahan               string,
        kecamatan               string,
        city                    string,
        province                string,
        coordinate              string,
        store_regional_code     string
     )
     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY '|'
     STORED AS TEXTFILE
     LOCATION '/yogya-bigdata-prd/dim_store'
     tblproperties ("skip.header.line.count"="1");
     
    
    DROP TABLE fs_user_auth;
    
    CREATE EXTERNAL TABLE IF NOT EXISTS fs_user_auth(
    login_name          string,    
    store_code          string,
    division            string,
    category            string,
    subcategory         string
     )
     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY '|'
     STORED AS TEXTFILE
     LOCATION '/yogya-bigdata-prd/dim_user_auth'
     tblproperties ("skip.header.line.count"="1");
    
    
    DROP TABLE fs_time;
    
    CREATE EXTERNAL TABLE IF NOT EXISTS fs_time(
    dates           date,
    day_key         string,
    week            string,
    weekday         string,
    weekday_name    string,
    months          string,
    cal_month       int,
    month_name      string,
    quarter         string,
    quarter_name    string,
    halfyear        string,
    halfyear_name   string,
    years           string,
    time_c1         string,
    time_c2         string,
    time_c3         string,
    time_c4         string,
    time_c5         string,    
    time_c6         string,    
    time_c7         string,    
    time_c8         string,
    time_c9         string,
    time_c10        string
    )
    ROW FORMAT DELIMITED
     FIELDS TERMINATED BY '|'
     STORED AS TEXTFILE
     LOCATION '/yogya-bigdata-prd/dim_time'
     tblproperties ("skip.header.line.count"="1");
     
    DROP TABLE dim_article;
    
    CREATE TABLE dim_article ( 
        article_code        string,
        article_desc        string,
        directorate         string,
        directorate_desc    string,
        division            string,
        division_desc       string,
        category            string,
        category_desc       string,    
        sub_category        string,
        sub_category_desc   string,
        brand               string
    )
    CLUSTERED BY(article_code) INTO 3 BUCKETS
    STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');
    
    DROP TABLE dim_category_stru;
    
    CREATE TABLE dim_category_stru ( 
        directorate         string,
        directorate_desc    string,
        division            string,
        division_desc       string,
        category            string,
        category_desc       string,    
        sub_category        string,
        sub_category_desc   string
    )
    CLUSTERED BY(directorate) INTO 3 BUCKETS
    STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');
    
    DROP TABLE dim_Subcat;
    
    CREATE TABLE dim_Subcat ( 
        sub_category        string,
        sub_category_desc   string
    )
    CLUSTERED BY(sub_category) INTO 3 BUCKETS
    STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');
    
    DROP TABLE dim_cat;
    
    CREATE TABLE dim_cat ( 
        category        string,
        category_desc   string
    )
    CLUSTERED BY(category) INTO 3 BUCKETS
    STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');
    
    DROP TABLE dim_div;
    
    CREATE TABLE dim_div ( 
        division        string,
        division_desc   string
    )
    CLUSTERED BY(division) INTO 3 BUCKETS
    STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');
    
    DROP TABLE dim_dir;
    
    CREATE TABLE dim_dir ( 
        directorate        string,
        directorate_desc   string
    )
    CLUSTERED BY(directorate) INTO 3 BUCKETS
    STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');
    
    DROP TABLE dim_class;
    
    CREATE TABLE dim_class ( 
        class        string,
        class_desc   string
    )
    CLUSTERED BY(class) INTO 3 BUCKETS
    STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');
    
    DROP TABLE dim_subclass;
    
    CREATE TABLE dim_subclass ( 
        subclass        string,
        subclass_desc   string
    )
    CLUSTERED BY(subclass) INTO 3 BUCKETS
    STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');
    
    DROP TABLE dim_brand;
    
    CREATE TABLE dim_brand ( 
        brand        string
    )
    CLUSTERED BY(brand) INTO 3 BUCKETS
    STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');
    
    DROP TABLE dim_store;
    
    CREATE TABLE dim_store ( 
        store_code              string,
        store_abbreviation      string,
        store_description       string,
        store_group             string,
        express                 string,
        selling_area            string,
        opening_date            string,    
        store_address           string,
        kelurahan               string,
        kecamatan               string,
        city                    string,
        province                string,
        coordinate              string,
        store_regional_code     string
    )
    CLUSTERED BY(store_code) INTO 3 BUCKETS
    STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');
    
    DROP TABLE dim_time;
    
    CREATE TABLE dim_time ( 
    dates           date,
    day_key         string,
    week            string,
    weekday         string,
    weekday_name    string,
    months          string,
    month_name      string,
    quarter         string,
    quarter_name    string,
    halfyear        string,
    halfyear_name   string,
    years           string,
    time_c1         string,
    time_c2         string,
    time_c3         string,
    time_c4         string,
    time_c5         string,    
    time_c6         string,    
    time_c7         string,    
    time_c8         string,
    time_c9         string,
    time_c10        string
    )
    STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');
         
    '''

    SS.sql("USE bigdata_prd")
    SS.sql("TRUNCATE TABLE dim_article")
    SS.sql("TRUNCATE TABLE dim_category_stru")
    SS.sql("TRUNCATE TABLE dim_dir")
    SS.sql("TRUNCATE TABLE dim_div")
    SS.sql("TRUNCATE TABLE dim_cat")
    SS.sql("TRUNCATE TABLE dim_Subcat")
    SS.sql("TRUNCATE TABLE dim_class")
    SS.sql("TRUNCATE TABLE dim_subclass")
    SS.sql("TRUNCATE TABLE dim_brand")
    SS.sql("TRUNCATE TABLE dim_store")
    SS.sql("TRUNCATE TABLE dim_time")
    SS.sql("TRUNCATE TABLE dim_month")
    SS.sql("TRUNCATE TABLE dim_year")

    sql = """ INSERT INTO  dim_article   
                SELECT  DISTINCT
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
                    sub_class_desc      ,
                    article_c1          ,
                    article_c2          ,
                    article_c3          ,
                    article_c4          ,
                    article_c5          ,
                    article_c6          ,
                    article_c7          ,
                    article_c8          ,
                    article_c9          ,
                    article_c10         
                FROM fs_article
               WHERE substring(article_code,1,2) = '00'
          """

    df_select = SS.sql(sql)

    #===========================================#
    # OverWriter table dim_category_stru
    # ===========================================#
    sql = """ INSERT INTO  dim_category_stru   
                SELECT  DISTINCT
                    directorate        ,
                    directorate_desc   ,
                    division           ,
                    division_desc      ,
                    category           ,
                    category_desc      ,
                    sub_category       ,
                    sub_category_desc  
                FROM fs_article
          """
    df_select = SS.sql(sql)


    #===========================================#
    # OverWriter table dim_dir
    # ===========================================#
    sql = """ INSERT INTO  dim_dir 
                SELECT  DISTINCT
                    directorate        ,
                    directorate_desc   
                FROM fs_article
          """
    df_select = SS.sql(sql)

    #===========================================#
    # OverWriter table dim_div
    # ===========================================#
    sql = """ INSERT INTO  dim_div 
                SELECT  DISTINCT
                    division        ,
                    division_desc   
                FROM fs_article
          """
    df_select = SS.sql(sql)

    #===========================================#
    # OverWriter table dim_cat
    # ===========================================#
    sql = """ INSERT INTO  dim_cat   
                SELECT  DISTINCT
                    category ,
                    category_desc   
                FROM fs_article
          """
    df_select = SS.sql(sql)

    #===========================================#
    # OverWriter table dim_Subcat
    # ===========================================#
    sql = """ INSERT INTO  dim_Subcat   
                SELECT  DISTINCT
                    sub_category        ,
                    sub_category_desc   
                FROM fs_article
          """
    df_select = SS.sql(sql)

    #===========================================#
    # OverWriter table dim_class
    # ===========================================#
    sql = """ INSERT INTO  dim_class 
                SELECT  DISTINCT
                    class        ,
                    class_desc   
                FROM fs_article
          """
    df_select = SS.sql(sql)

    #===========================================#
    # OverWriter table dim_subclass
    # ===========================================#
    sql = """ INSERT INTO  dim_subclass 
                SELECT  DISTINCT
                    sub_class        ,
                    sub_class_desc   
                FROM fs_article
          """
    df_select = SS.sql(sql)

    #===========================================#
    # OverWriter table dim_brand
    # ===========================================#
    sql = """ INSERT INTO  dim_brand 
                SELECT  DISTINCT
                        brand 
                FROM fs_article
          """
    df_select = SS.sql(sql)

    #===========================================#
    # OverWriter table dim_store
    # ===========================================#
    sql = """ INSERT INTO  dim_store
                SELECT   DISTINCT
                         store_code 
                        ,store_abbreviation
                        ,store_description 
                        ,store_group       
                        ,express           
                        ,selling_area      
                        ,opening_date          
                        ,store_address     
                        ,kelurahan         
                        ,kecamatan         
                        ,city              
                        ,province          
                        ,coordinate
                        ,store_regional_code                        
                  FROM fs_store
                WHERE store_code <> 'store_code'
          """
    df_select = SS.sql(sql)

    #===========================================#
    # OverWriter table dim_time
    # ===========================================#
    sql = """INSERT INTO dim_time
             SELECT DISTINCT
                    dates,
                    day_key,
                    week,
                    weekday,
                    weekday_name,
                    months,
                    month_name,
                    quarter,
                    quarter_name,
                    halfyear,
                    halfyear_name,
                    years,
                    time_c1,
                    time_c2,
                    time_c3,
                    time_c4,
                    time_c5,
                    time_c6,
                    time_c7,
                    time_c8,
                    time_c9,
                    time_c10
                FROM fs_time
              WHERE dates <> 'dates'  
          """
    df_select = SS.sql(sql)

    #===========================================#
    # OverWriter table dim_month
    # ===========================================#
    sql = """
            INSERT INTO  dim_month
            SELECT  distinct
                    months          ,
                    month_name      ,
                    years                               
            FROM fs_time
            WHERE months <> 'month'
          """
    df_select = SS.sql(sql)

    #===========================================#
    # OverWriter table dim_year
    # ===========================================#
    sql = """
           INSERT INTO  dim_year
                SELECT  years           
                  FROM fs_time
                WHERE years <> 'year'
                GROUP BY years
          """
    df_select = SS.sql(sql)
    df_select.show(10)

if __name__ == '__main__':
    main()

# run spark on Yarn
# export HADOOP_USER_NAME=hive
# spark-submit --master yarn-cluster connect_hive.py
# spark-submit --master yarn-client connect_hive.py

