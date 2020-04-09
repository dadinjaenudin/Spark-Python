    '''
        hdfs dfs -mkdir /yogya-bigdata-prd/fact_stock_gold
        hdfs dfs -mkdir /yogya-bigdata-prd/fact_stock_gold/2018
        hdfs dfs -mkdir /yogya-bigdata-prd/fact_stock_gold/2018/01
        hdfs dfs -mkdir /yogya-bigdata-prd/fact_stock_gold/2018/02
        hdfs dfs -mkdir /yogya-bigdata-prd/fact_stock_gold/2018/03
        hdfs dfs -mkdir /yogya-bigdata-prd/fact_stock_gold/2018/04
        hdfs dfs -mkdir /yogya-bigdata-prd/fact_stock_gold/2018/05
        hdfs dfs -mkdir /yogya-bigdata-prd/fact_stock_gold/2018/06
        hdfs dfs -mkdir /yogya-bigdata-prd/fact_stock_gold/2018/07
        hdfs dfs -mkdir /yogya-bigdata-prd/fact_stock_gold/2018/08
        hdfs dfs -mkdir /yogya-bigdata-prd/fact_stock_gold/2018/09
        hdfs dfs -mkdir /yogya-bigdata-prd/fact_stock_gold/2018/10
        hdfs dfs -mkdir /yogya-bigdata-prd/fact_stock_gold/2018/11
        hdfs dfs -mkdir /yogya-bigdata-prd/fact_stock_gold/2018/12

    ALTER TABLE fs_stock_gold ADD PARTITION (year='2018',month='01') LOCATION '/yogya-bigdata-prd/fact_stock_gold/2018/01';
    ALTER TABLE fs_stock_gold ADD PARTITION (year='2018',month='02') LOCATION '/yogya-bigdata-prd/fact_stock_gold/2018/02';
    ALTER TABLE fs_stock_gold ADD PARTITION (year='2018',month='03') LOCATION '/yogya-bigdata-prd/fact_stock_gold/2018/03';
    ALTER TABLE fs_stock_gold ADD PARTITION (year='2018',month='04') LOCATION '/yogya-bigdata-prd/fact_stock_gold/2018/04';
    ALTER TABLE fs_stock_gold ADD PARTITION (year='2018',month='05') LOCATION '/yogya-bigdata-prd/fact_stock_gold/2018/05';
    ALTER TABLE fs_stock_gold ADD PARTITION (year='2018',month='06') LOCATION '/yogya-bigdata-prd/fact_stock_gold/2018/06';
    ALTER TABLE fs_stock_gold ADD PARTITION (year='2018',month='07') LOCATION '/yogya-bigdata-prd/fact_stock_gold/2018/07';
    ALTER TABLE fs_stock_gold ADD PARTITION (year='2018',month='08') LOCATION '/yogya-bigdata-prd/fact_stock_gold/2018/08';
    ALTER TABLE fs_stock_gold ADD PARTITION (year='2018',month='09') LOCATION '/yogya-bigdata-prd/fact_stock_gold/2018/09';
    ALTER TABLE fs_stock_gold ADD PARTITION (year='2018',month='10') LOCATION '/yogya-bigdata-prd/fact_stock_gold/2018/10';
    ALTER TABLE fs_stock_gold ADD PARTITION (year='2018',month='11') LOCATION '/yogya-bigdata-prd/fact_stock_gold/2018/11';
    ALTER TABLE fs_stock_gold ADD PARTITION (year='2018',month='12') LOCATION '/yogya-bigdata-prd/fact_stock_gold/2018/12';

    CREATE EXTERNAL TABLE IF NOT EXISTS fs_stock_gold(
        date_trn    date,
        store_code  string,
        art_code    string,
        qty         float,
        art_type    string,
        costp       float,
        sales       float,
        vat         string
    ) PARTITIONED BY (year string, month string)
    ROW FORMAT DELIMITED
     FIELDS TERMINATED BY '|'
     STORED AS TEXTFILE
     LOCATION '/yogya-bigdata-prd/fact_stock_gold'
     tblproperties ("skip.header.line.count"="1");

    DROP VIEW fact_stock_gold_bydate;
    DROP TABLE fact_stock_gold_bydate_2018;
    DROP VIEW fact_stock_gold_byMonth;
    DROP TABLE fact_stock_gold_byMonth_2018;
    DROP VIEW fact_stock_gold_byYear;
    DROP TABLE fact_stock_gold_byYear_2018;

    CREATE TABLE fact_stock_gold_bydate_2018 (
        store_code      string,
        date_trn        string,
        Directorate     string,
        division        string,
        category        string,
        sub_category    string,
        class           string,
        brand           string,
        art_type        string,
        princ_code      string,
        sales	        float,
        cogs	        float,
        ppn	            float,
        qty             float,
        sales_stock	    float,
        cogs_stock	    float,
        ppn_stock	    float,
        qty_stock       float,
        skp             float
    )
    PARTITIONED BY (year string, month string)
    CLUSTERED BY(date_trn) INTO 10 BUCKETS
    STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

    CREATE VIEW fact_stock_gold_bydate
    PARTITIONED ON (year,month)
    AS
    SELECT
             store_code
            ,date_trn
            ,Directorate
            ,division
            ,category
            ,sub_category
            ,class
            ,brand
            ,art_type
            ,princ_code
            ,sales
            ,cogs
            ,ppn
            ,qty
            ,sales_stock
            ,cogs_stock
            ,ppn_stock
            ,qty_stock
            ,skp
            ,concat(year,month) month_trn
            ,year
            ,month
       FROM fact_stock_gold_bydate_2018


    CREATE TABLE fact_stock_gold_byMonth_2018 (
        store_code      string,
        month_trn       string,
        Directorate     string,
        division        string,
        category        string,
        sub_category    string,
        class           string,
        brand           string,
        art_type        string,
        princ_code      string,
        sales	        float,
        cogs	        float,
        ppn	            float,
        qty             float,
        sales_stock	    float,
        cogs_stock	    float,
        ppn_stock	    float,
        qty_stock       float,
        skp             float
    )
    PARTITIONED BY (year string, month string)
    CLUSTERED BY(store_code) INTO 3 BUCKETS
    STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

    CREATE VIEW fact_stock_gold_bymonth
    PARTITIONED ON (year,month)
    AS
    SELECT
             store_code
            ,month_trn
            ,Directorate
            ,division
            ,category
            ,sub_category
            ,class
            ,brand
            ,art_type
            ,princ_code
            ,sales
            ,cogs
            ,ppn
            ,qty
            ,sales_stock
            ,cogs_stock
            ,ppn_stock
            ,qty_stock
            ,skp
            ,year
            ,month
       FROM fact_stock_gold_bymonth_2018


    CREATE TABLE fact_stock_gold_byYear_2018 (
        store_code      string,
        year_trn        string,
        Directorate     string,
        division        string,
        category        string,
        sub_category    string,
        class           string,
        brand           string,
        art_type        string,
        princ_code      string,
        sales	        float,
        cogs	        float,
        ppn	            float,
        qty             float,
        sales_stock	    float,
        cogs_stock	    float,
        ppn_stock	    float,
        qty_stock       float,
        skp             float
    )
    PARTITIONED BY (year string)
    CLUSTERED BY(store_code) INTO 3 BUCKETS
    STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');


    CREATE VIEW fact_stock_gold_byyear
    PARTITIONED ON (year)
    AS
    SELECT
            store_code
            ,year_trn
            ,Directorate
            ,division
            ,category
            ,sub_category
            ,class
            ,brand
            ,art_type
            ,princ_code
            ,sales
            ,cogs
            ,ppn
            ,qty
            ,sales_stock
            ,cogs_stock
            ,ppn_stock
            ,qty_stock
            ,skp
            ,year
       FROM fact_stock_gold_byyear_2018
