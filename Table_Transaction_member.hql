DROP TABLE dim_generation;
DROP TABLE fact_Mbr_Mobility_byMonth_2016;
DROP TABLE fact_Mbr_Mobility_byMonth_2017;
DROP TABLE fact_Mbr_Mobility_byMonth_2018;
DROP VIEW fact_Mbr_Mobility_byMonth;
DROP TABLE fact_Mbr_Mobility_byYear_2016;
DROP TABLE fact_Mbr_Mobility_byYear_2017;
DROP TABLE fact_Mbr_Mobility_byYear_2018;
DROP VIEW  fact_Mbr_Mobility_byYear;

-- Create ORC Table
CREATE TABLE dim_generation (
    from_year    int,
    to_year      int,
    Description  string
)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '\t'
 STORED AS ORC tblproperties ("orc.compress"="ZLIB");

insert into dim_generation values(1946,1964,'Baby Boomers');
insert into dim_generation values(1965,1979,'Gen X');
insert into dim_generation values(1980,1990,'Milenial 1');
insert into dim_generation values(1991,2000,'Milenial 2');
insert into dim_generation values(2001,2040,'Lain-Lain');

===========================================================
Member Mobility
===========================================================

--CREATE TABLE fact_Mbr_Mobility_byMonth_2016 (
--CREATE TABLE fact_Mbr_Mobility_byMonth_2017 (
CREATE TABLE fact_Mbr_Mobility_byMonth_2018 (
Memberid    string,
store_code  string,
generation  string,
month_trn   string,
jml_trans   float,
jml_bs      float
)
PARTITIONED BY (year string, month string)
CLUSTERED BY(Memberid) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

CREATE VIEW fact_Mbr_Mobility_byMonth
PARTITIONED ON (year,month)
AS
SELECT
    Memberid,
    store_code,
    month_trn,
    generation,
    jml_trans,
    jml_bs,
    year,
    month
FROM  fact_Mbr_Mobility_byMonth_2016
UNION ALL
SELECT
    Memberid,
    store_code,
    month_trn,
    generation,
    jml_trans,
    jml_bs,
    year,
    month
FROM  fact_Mbr_Mobility_byMonth_2017
UNION ALL
SELECT
    Memberid,
    store_code,
    month_trn,
    generation,
    jml_trans,
    jml_bs,
    year,
    month
FROM  fact_Mbr_Mobility_byMonth_2018


--CREATE TABLE fact_Mbr_Mobility_byYear_2016 (
--CREATE TABLE fact_Mbr_Mobility_byYear_2017 (
CREATE TABLE fact_Mbr_Mobility_byYear_2018 (
Memberid    string,
store_code  string,
generation  string,
year_trn    string,
jml_trans   float,
jml_bs      float
)
PARTITIONED BY (year string)
CLUSTERED BY(Memberid) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

CREATE VIEW fact_Mbr_Mobility_byYear
PARTITIONED ON (year)
AS
SELECT
    Memberid,
    store_code,
    generation,
    year_trn,
    jml_trans,
    jml_bs,
    year
FROM  fact_Mbr_Mobility_byYear_2016
UNION ALL
SELECT
    Memberid,
    store_code,
    generation,
    year_trn,
    jml_trans,
    jml_bs,
    year
FROM  fact_Mbr_Mobility_byYear_2017
UNION ALL
SELECT
    Memberid,
    store_code,
    generation,
    year_trn,
    jml_trans,
    jml_bs,
    year
FROM  fact_Mbr_Mobility_byYear_2018
