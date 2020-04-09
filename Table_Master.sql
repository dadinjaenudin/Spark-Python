--//////////////////////////////////////////////
-- CREATE HDFS ARTICLE AND DIMESION ARTICLE
--//////////////////////////////////////////////
/*
$hdfs dfs -put time_article.csv /yogya-bigdata-prd/dim_article
$hdfs dfs -chmod 777 /yogya-bigdata-prd/dim_article
$hdfs dfs -ls /yogya-bigdata-prd/dim_article
#Remove direktory
$hdfs dfs -rm -r /yogya-bigdata-prd/dim_article
# delete directory / file
#hdfs dfs -rm -R /apps/hive/warehouse/bigdata_prd.db/dim_article
# Drop table permission Denied
hdfs dfs -chown -R hive:hadoop /apps/hive/warehouse/bigdata_prd.db/dim_article
*/

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
    sub_class_desc      string,
    article_c1          string,
    article_c2          string,
    article_c3          string,
    article_c4          string,
    article_c5          string,
    article_c6          string,
    article_c7          string,
    article_c8          string,
    article_c9          string,
    article_c10         string
  )
ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '|'
 STORED AS TEXTFILE
 LOCATION '/yogya-bigdata-prd/dim_article'
 tblproperties ("skip.header.line.count"="1");

DROP TABLE dim_article;

CREATE TABLE dim_article (
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
CLUSTERED BY(article_code) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

==========================
Dimention sub_category
==========================

DROP TABLE dim_Subcat;

CREATE TABLE dim_Subcat (
    sub_category        string,
    sub_category_desc   string
)
CLUSTERED BY(sub_category) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');


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

==========================
Dimention Directorate
==========================

DROP TABLE dim_dir;

CREATE TABLE dim_dir (
    directorate        string,
    directorate_desc   string
)
CLUSTERED BY(directorate) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

==========================
Dimention Class
==========================

DROP TABLE dim_class;

CREATE TABLE dim_class (
    class        string,
    class_desc   string
)
CLUSTERED BY(class) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

==========================
Dimention sub_Class
==========================

DROP TABLE dim_subclass;

CREATE TABLE dim_subclass (
    subclass        string,
    subclass_desc   string
)
CLUSTERED BY(subclass) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

==========================
Dimention brand
==========================

DROP TABLE dim_brand;

CREATE TABLE dim_brand (
    brand        string
)
CLUSTERED BY(brand) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');


==========================
Dimention princ_code
==========================

DROP TABLE dim_principle;

CREATE TABLE dim_principle (
    princ_code        string,
    princ_desc        string
)
CLUSTERED BY(princ_code) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

==========================
Dimension Supplier
==========================

DROP TABLE dim_supplier;

CREATE TABLE dim_supplier (
    supp_code        string,
    supp_desc        string
)
CLUSTERED BY(supp_code) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');


==========================
Dimension Supplier_by_article
==========================

DROP TABLE dim_supplier_article;

CREATE TABLE dim_supplier_article (
    supp_code        string,
    supp_desc        string,
    article_code     string
)
CLUSTERED BY(supp_code) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');


==========================
Dimension Principle_by_article
==========================

DROP TABLE dim_principle_article;

CREATE TABLE dim_principle_article (
    princ_code        string,
    princ_desc        string,
    article_code     string
)
CLUSTERED BY(princ_code) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

