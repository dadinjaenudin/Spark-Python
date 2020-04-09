===========================================================
CREATE TABLE GroupMkg
===========================================================

-- Create ORC Table
CREATE TABLE dim_mkggroup (
    grp_kode    int,
    grp_desc    string
)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '\t'
 STORED AS ORC tblproperties ("orc.compress"="ZLIB");

insert into dim_mkggroup values(1,'FASHION');
insert into dim_mkggroup values(2,'SUPERMARKET');
insert into dim_mkggroup values(3,'MART');
insert into dim_mkggroup values(4,'MO');
insert into dim_mkggroup values(5,'GTMS');
insert into dim_mkggroup values(6,'Cafe');
insert into dim_mkggroup values(7,'Exp');
insert into dim_mkggroup values(8,'');
insert into dim_mkggroup values(9,'Yomart');

===========================================================
CREATE TABLE fact_Transaction_byDate
===========================================================

DROP TABLE fact_Transaction_byDate_2016;

CREATE TABLE fact_Transaction_byDate_2016 (
date_trn    date,
store_code  string,
grp_kode    int,
jml_trans_dtd   float,
jml_bs_dtd      float,
jml_trans   float,
jml_bs      float
)
PARTITIONED BY (year string, month string)
CLUSTERED BY(date_trn) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP TABLE fact_Transaction_byDate_2017;

CREATE TABLE fact_Transaction_byDate_2017 (
date_trn    date,
store_code  string,
grp_kode    int,
jml_trans_dtd   float,
jml_bs_dtd      float,
jml_trans   float,
jml_bs      float
)
PARTITIONED BY (year string, month string)
CLUSTERED BY(date_trn) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP TABLE fact_Transaction_byDate_2018;

CREATE TABLE fact_Transaction_byDate_2018 (
date_trn    date,
store_code  string,
grp_kode    int,
jml_trans_dtd   float,
jml_bs_dtd      float,
jml_trans   float,
jml_bs      float
)
PARTITIONED BY (year string, month string)
CLUSTERED BY(date_trn) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP VIEW fact_Transaction_byDate;

CREATE VIEW fact_Transaction_byDate
PARTITIONED ON (year,month)
AS
SELECT
    date_trn,
    store_code,
    grp_kode,
    jml_trans_dtd,
    jml_bs_dtd,
    jml_trans,
    jml_bs,
    year,
    month
FROM  fact_Transaction_byDate_2016
UNION ALL
SELECT
    date_trn,
    store_code,
    grp_kode,
    jml_trans_dtd,
    jml_bs_dtd,
    jml_trans,
    jml_bs,
    year,
    month
FROM  fact_Transaction_byDate_2017
UNION ALL
SELECT
    date_trn,
    store_code,
    grp_kode,
    jml_trans_dtd,
    jml_bs_dtd,
    jml_trans,
    jml_bs,
    year,
    month
FROM  fact_Transaction_byDate_2018;

===========================================================
CREATE TABLE fact_jmltrans_bymonth
===========================================================

DROP TABLE fact_Transaction_bymonth_2016;

CREATE TABLE fact_Transaction_bymonth_2016 (
month_trn    string,
store_code  string,
grp_kode    int,
jml_trans_mtd    float,
jml_bs_mtd       float,
jml_trans    float,
jml_bs       float
)
PARTITIONED BY (year string, month string)
CLUSTERED BY(month_trn) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP TABLE fact_Transaction_bymonth_2017;

CREATE TABLE fact_Transaction_bymonth_2017 (
month_trn    string,
store_code  string,
grp_kode    int,
jml_trans_mtd    float,
jml_bs_mtd       float,
jml_trans    float,
jml_bs       float
)
PARTITIONED BY (year string, month string)
CLUSTERED BY(month_trn) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP TABLE fact_Transaction_bymonth_2018;

CREATE TABLE fact_Transaction_bymonth_2018 (
month_trn   string,
store_code  string,
grp_kode    int,
jml_trans_mtd    float,
jml_bs_mtd       float,
jml_trans   float,
jml_bs      float
)
PARTITIONED BY (year string, month string)
CLUSTERED BY(month_trn) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP VIEW fact_Transaction_bymonth;

CREATE VIEW fact_Transaction_bymonth
PARTITIONED ON (year,month)
AS
SELECT
    month_trn,
    store_code,
    grp_kode,
    jml_trans_mtd,
    jml_bs_mtd,
    jml_trans,
    jml_bs,
    year,
    month
FROM  fact_Transaction_bymonth_2016
UNION ALL
SELECT
    month_trn,
    store_code,
    grp_kode,
    jml_trans_mtd,
    jml_bs_mtd,
    jml_trans,
    jml_bs,
    year,
    month
FROM  fact_Transaction_bymonth_2017
UNION ALL
SELECT
    month_trn,
    store_code,
    grp_kode,
    jml_trans_mtd,
    jml_bs_mtd,
    jml_trans,
    jml_bs,
    year,
    month
FROM  fact_Transaction_bymonth_2018;

===================================================
CREATE TABLE fact_Transaction_byyear
===================================================

DROP TABLE fact_Transaction_byyear_2016;

CREATE TABLE fact_Transaction_byyear_2016 (
year_trn    string,
store_code  string,
grp_kode    int,
jml_trans_ytd    float,
jml_bs_ytd       float,
jml_trans    float,
jml_bs       float
)
PARTITIONED BY (year string)
CLUSTERED BY(year_trn) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP TABLE fact_Transaction_byyear_2017;

CREATE TABLE fact_Transaction_byyear_2017 (
year_trn    string,
store_code  string,
grp_kode    int,
jml_trans_ytd    float,
jml_bs_ytd       float,
jml_trans    float,
jml_bs       float
)
PARTITIONED BY (year string)
CLUSTERED BY(year_trn) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP TABLE fact_Transaction_byyear_2018;

CREATE TABLE fact_Transaction_byyear_2018 (
year_trn    string,
store_code  string,
grp_kode    int,
jml_trans_ytd    float,
jml_bs_ytd       float,
jml_trans    float,
jml_bs       float
)
PARTITIONED BY (year string)
CLUSTERED BY(year_trn) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP VIEW fact_Transaction_byyear;

CREATE VIEW fact_Transaction_byyear
PARTITIONED ON (year)
AS
SELECT
    year_trn,
    store_code,
    grp_kode,
    jml_trans_ytd,
    jml_bs_ytd,
    jml_trans,
    jml_bs,
    year
FROM  fact_Transaction_byyear_2016
UNION ALL
SELECT
    year_trn,
    store_code,
    grp_kode,
    jml_trans_ytd,
    jml_bs_ytd,
    jml_trans,
    jml_bs,
    year
FROM  fact_Transaction_byyear_2017
UNION ALL
SELECT
    year_trn,
    store_code,
    grp_kode,
    jml_trans_ytd,
    jml_bs_ytd,
    jml_trans,
    jml_bs,
    year
FROM  fact_Transaction_byyear_2018;