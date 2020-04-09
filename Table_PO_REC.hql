hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2017
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2017/01
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2017/02
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2017/03
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2017/04
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2017/05
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2017/06
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2017/07
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2017/08
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2017/09
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2017/10
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2017/11
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2017/12

hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2018
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2018/01
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2018/02
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2018/03
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2018/04
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2018/05
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2018/06
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2018/07
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2018/08
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2018/09
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2018/10
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2018/11
hdfs dfs -mkdir /yogya-bigdata-prd/fact_po/2018/12

hdfs dfs -chmod -R 777 /yogya-bigdata-prd/fact_po
hdfs dfs -put * /yogya-bigdata-prd/fact_po

- droping table sometimes need drop from metastore hive
hdfs dfs -rmr /apps/hive/warehouse/bigdata_prd.db/fact_po_rec_byarticle

drop table fs_po;

CREATE EXTERNAL TABLE IF NOT EXISTS fs_po(
store_code  string,
supp_code   string,
supp_desc   string,
comm_cont   string,
type_supp   string,
add_ch      string,
princ_code  string,
princ_desc  string,
art_code    string,
po_no       string,
ord_date    date,
po_qty      float,
po_rp       float,
rec_no      string,
rec_date    date,
rec_qty     float,
rec_rp      float
) PARTITIONED BY (year string, month string)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '/yogya-bigdata-prd/fact_po'
  tblproperties ("skip.header.line.count"="1");


ALTER TABLE fs_po ADD PARTITION (year='2017',month='01') LOCATION '/yogya-bigdata-prd/fact_po/2017/01';
ALTER TABLE fs_po ADD PARTITION (year='2017',month='02') LOCATION '/yogya-bigdata-prd/fact_po/2017/02';
ALTER TABLE fs_po ADD PARTITION (year='2017',month='03') LOCATION '/yogya-bigdata-prd/fact_po/2017/03';
ALTER TABLE fs_po ADD PARTITION (year='2017',month='04') LOCATION '/yogya-bigdata-prd/fact_po/2017/04';
ALTER TABLE fs_po ADD PARTITION (year='2017',month='05') LOCATION '/yogya-bigdata-prd/fact_po/2017/05';
ALTER TABLE fs_po ADD PARTITION (year='2017',month='06') LOCATION '/yogya-bigdata-prd/fact_po/2017/06';
ALTER TABLE fs_po ADD PARTITION (year='2017',month='07') LOCATION '/yogya-bigdata-prd/fact_po/2017/07';
ALTER TABLE fs_po ADD PARTITION (year='2017',month='08') LOCATION '/yogya-bigdata-prd/fact_po/2017/08';
ALTER TABLE fs_po ADD PARTITION (year='2017',month='09') LOCATION '/yogya-bigdata-prd/fact_po/2017/09';
ALTER TABLE fs_po ADD PARTITION (year='2017',month='10') LOCATION '/yogya-bigdata-prd/fact_po/2017/10';
ALTER TABLE fs_po ADD PARTITION (year='2017',month='11') LOCATION '/yogya-bigdata-prd/fact_po/2017/11';
ALTER TABLE fs_po ADD PARTITION (year='2017',month='12') LOCATION '/yogya-bigdata-prd/fact_po/2017/12';

ALTER TABLE fs_po ADD PARTITION (year='2018',month='01') LOCATION '/yogya-bigdata-prd/fact_po/2018/01';
ALTER TABLE fs_po ADD PARTITION (year='2018',month='02') LOCATION '/yogya-bigdata-prd/fact_po/2018/02';
ALTER TABLE fs_po ADD PARTITION (year='2018',month='03') LOCATION '/yogya-bigdata-prd/fact_po/2018/03';
ALTER TABLE fs_po ADD PARTITION (year='2018',month='04') LOCATION '/yogya-bigdata-prd/fact_po/2018/04';
ALTER TABLE fs_po ADD PARTITION (year='2018',month='05') LOCATION '/yogya-bigdata-prd/fact_po/2018/05';
ALTER TABLE fs_po ADD PARTITION (year='2018',month='06') LOCATION '/yogya-bigdata-prd/fact_po/2018/06';
ALTER TABLE fs_po ADD PARTITION (year='2018',month='07') LOCATION '/yogya-bigdata-prd/fact_po/2018/07';
ALTER TABLE fs_po ADD PARTITION (year='2018',month='08') LOCATION '/yogya-bigdata-prd/fact_po/2018/08';
ALTER TABLE fs_po ADD PARTITION (year='2018',month='09') LOCATION '/yogya-bigdata-prd/fact_po/2018/09';
ALTER TABLE fs_po ADD PARTITION (year='2018',month='10') LOCATION '/yogya-bigdata-prd/fact_po/2018/10';
ALTER TABLE fs_po ADD PARTITION (year='2018',month='11') LOCATION '/yogya-bigdata-prd/fact_po/2018/11';
ALTER TABLE fs_po ADD PARTITION (year='2018',month='12') LOCATION '/yogya-bigdata-prd/fact_po/2018/12';

hdfs dfs -put **012018.TXT /yogya-bigdata-prd/fact_po/2018/01
hdfs dfs -put **022018.TXT /yogya-bigdata-prd/fact_po/2018/02
hdfs dfs -put **032018.TXT /yogya-bigdata-prd/fact_po/2018/03
hdfs dfs -put **042018.TXT /yogya-bigdata-prd/fact_po/2018/04
hdfs dfs -put **052018.TXT /yogya-bigdata-prd/fact_po/2018/05
hdfs dfs -put **062018.TXT /yogya-bigdata-prd/fact_po/2018/06
hdfs dfs -put **072018.TXT /yogya-bigdata-prd/fact_po/2018/07
hdfs dfs -put **082018.TXT /yogya-bigdata-prd/fact_po/2018/08
hdfs dfs -put **092018.TXT /yogya-bigdata-prd/fact_po/2018/09
hdfs dfs -put **102018.TXT /yogya-bigdata-prd/fact_po/2018/10
hdfs dfs -put **112018.TXT /yogya-bigdata-prd/fact_po/2018/11
hdfs dfs -put **122018.TXT /yogya-bigdata-prd/fact_po/2018/12


--/////////////////////////////////////
--CREATE dim_principle
--/////////////////////////////////////

DROP TABLE dim_principle;

CREATE TABLE dim_principle (
princ_code  string,
princ_desc  string
)
CLUSTERED BY(princ_code) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');


--/////////////////////////////////////
--CREATE dim_supplier
--/////////////////////////////////////

DROP TABLE dim_supplier;

CREATE TABLE dim_supplier (
supp_code   string,
supp_desc   string
)
CLUSTERED BY(supp_code) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP TABLE fact_po_rec_byarticle_2017;

CREATE TABLE fact_po_rec_byarticle_2017 (
store_code      string,
Directorate     string,
division        string,
category        string,
sub_category    string,
brand           string,
supp_code       string,
princ_code      string,
art_code        string,
ord_date        date,
po_qty          float,
po_rp           float,
rec_qty         float,
rec_rp          float
)
PARTITIONED BY (year string, month string)
CLUSTERED BY(supp_code) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP TABLE fact_po_rec_bydate_2017;

CREATE TABLE fact_po_rec_bydate_2017 (
store_code      string,
Directorate     string,
division        string,
category        string,
sub_category    string,
supp_code       string,
ord_date        date,
po_qty          float,
po_rp           float,
rec_qty         float,
rec_rp          float,
po_qty_dtd      float,
po_rp_dtd       float,
rec_qty_dtd     float,
rec_rp_dtd      float
)
PARTITIONED BY (year string, month string)
CLUSTERED BY(supp_code) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');


DROP TABLE fact_po_rec_bydate_2018;

CREATE TABLE fact_po_rec_bydate_2018 (
store_code      string,
Directorate     string,
division        string,
category        string,
sub_category    string,
supp_code       string,
ord_date        date,
po_qty          float,
po_rp           float,
rec_qty         float,
rec_rp          float,
po_qty_dtd      float,
po_rp_dtd       float,
rec_qty_dtd     float,
rec_rp_dtd      float
)
PARTITIONED BY (year string, month string)
CLUSTERED BY(supp_code) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');


DROP TABLE fact_po_rec_bymonth_2017;

CREATE TABLE fact_po_rec_bymonth_2017 (
store_code      string,
Directorate     string,
division        string,
category        string,
sub_category    string,
brand           string,
supp_code       string,
princ_code      string,
month_trn	    string,
po_qty          float,
po_rp           float,
rec_qty         float,
rec_rp          float,
po_qty_mtd      float,
po_rp_mtd       float,
rec_qty_mtd     float,
rec_rp_mtd      float
)
PARTITIONED BY (year string, month string)
CLUSTERED BY(store_code) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP TABLE fact_po_rec_bymonth_2018;

CREATE TABLE fact_po_rec_bymonth_2018 (
store_code      string,
Directorate     string,
division        string,
category        string,
sub_category    string,
brand           string,
supp_code       string,
princ_code      string,
month_trn	    string,
po_qty          float,
po_rp           float,
rec_qty         float,
rec_rp          float,
po_qty_mtd      float,
po_rp_mtd       float,
rec_qty_mtd     float,
rec_rp_mtd      float
)
PARTITIONED BY (year string, month string)
CLUSTERED BY(supp_code) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP TABLE fact_po_rec_byyear_2017;

CREATE TABLE fact_po_rec_byyear_2017 (
store_code      string,
Directorate     string,
division        string,
category        string,
sub_category    string,
brand           string,
supp_code       string,
princ_code      string,
year_trn	    string,
po_qty          float,
po_rp           float,
rec_qty         float,
rec_rp          float,
po_qty_ytd      float,
po_rp_ytd       float,
rec_qty_ytd     float,
rec_rp_ytd      float
)
PARTITIONED BY (year string)
CLUSTERED BY(supp_code) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP TABLE fact_po_rec_byyear_2018;

CREATE TABLE fact_po_rec_byyear_2018 (
store_code      string,
Directorate     string,
division        string,
category        string,
sub_category    string,
brand           string,
supp_code       string,
princ_code      string,
year_trn	    string,
po_qty          float,
po_rp           float,
rec_qty         float,
rec_rp          float,
po_qty_ytd      float,
po_rp_ytd       float,
rec_qty_ytd     float,
rec_rp_ytd      float
)
PARTITIONED BY (year string)
CLUSTERED BY(supp_code) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

#######################################
CREATE UNION
#######################################

DROP VIEW fact_po_rec_bydate;

CREATE VIEW fact_po_rec_bydate
PARTITIONED ON (year,month)
AS
SELECT  store_code
        ,Directorate
        ,division
        ,supp_code
        ,ord_date
        ,po_qty
        ,po_rp
        ,rec_qty
        ,rec_rp
        ,po_qty_dtd
        ,po_rp_dtd
        ,rec_qty_dtd
        ,rec_rp_dtd
        ,year
        ,month
FROM fact_po_rec_bydate_2017
UNION ALL
SELECT  store_code
        ,Directorate
        ,division
        ,supp_code
        ,ord_date
        ,po_qty
        ,po_rp
        ,rec_qty
        ,rec_rp
        ,po_qty_dtd
        ,po_rp_dtd
        ,rec_qty_dtd
        ,rec_rp_dtd
        ,year
        ,month
FROM fact_po_rec_bydate_2018;

DROP VIEW fact_po_rec_bymonth;

CREATE VIEW fact_po_rec_bymonth
PARTITIONED ON (year,month)
AS
SELECT  store_code
        ,Directorate
        ,division
        ,category
        ,sub_category
        ,brand
        ,supp_code
        ,princ_code
        ,month_trn
        ,po_qty
        ,po_rp
        ,rec_qty
        ,rec_rp
        ,po_qty_mtd
        ,po_rp_mtd
        ,rec_qty_mtd
        ,rec_rp_mtd
        ,year
        ,month
FROM fact_po_rec_bymonth_2017
UNION ALL
SELECT  store_code
        ,Directorate
        ,division
        ,category
        ,sub_category
        ,brand
        ,supp_code
        ,princ_code
        ,month_trn
        ,po_qty
        ,po_rp
        ,rec_qty
        ,rec_rp
        ,po_qty_mtd
        ,po_rp_mtd
        ,rec_qty_mtd
        ,rec_rp_mtd
        ,year
        ,month
FROM fact_po_rec_bymonth_2018;

DROP VIEW fact_po_rec_byyear;

CREATE VIEW fact_po_rec_byyear
PARTITIONED ON (year)
AS
SELECT  store_code
        ,Directorate
        ,division
        ,category
        ,sub_category
        ,brand
        ,supp_code
        ,princ_code
        ,year_trn
        ,po_qty
        ,po_rp
        ,rec_qty
        ,rec_rp
        ,po_qty_ytd
        ,po_rp_ytd
        ,rec_qty_ytd
        ,rec_rp_ytd
        ,year
FROM fact_po_rec_byyear_2017
UNION ALL
SELECT  store_code
        ,Directorate
        ,division
        ,category
        ,sub_category
        ,brand
        ,supp_code
        ,princ_code
        ,year_trn
        ,po_qty
        ,po_rp
        ,rec_qty
        ,rec_rp
        ,po_qty_ytd
        ,po_rp_ytd
        ,rec_qty_ytd
        ,rec_rp_ytd
        ,year
FROM fact_po_rec_byyear_2018;

