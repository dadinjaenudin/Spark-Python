'''

hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2016
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2016/01
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2016/02
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2016/03
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2016/04
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2016/05
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2016/06
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2016/07
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2016/08
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2016/09
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2016/10
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2016/11
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2016/12

hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2017
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2017/01
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2017/02
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2017/03
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2017/04
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2017/05
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2017/06
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2017/07
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2017/08
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2017/09
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2017/10
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2017/11
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2017/12

hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2018
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2018/01
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2018/02
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2018/03
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2018/04
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2018/05
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2018/06
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2018/07
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2018/08
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2018/09
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2018/10
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2018/11
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmem/2018/12

hdfs dfs -chmod -R 777 /yogya-bigdata-prd/fact_mkgmem

'''

drop table fs_mkgmem;

CREATE EXTERNAL TABLE IF NOT EXISTS fs_mkgmem(
store_code  string,
date_trn    string,
termnmbr    string,
transnmbr   string,
member      string,
cshrnmbr    string,
empname     string
) PARTITIONED BY (year string, month string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/yogya-bigdata-prd/fact_mkgmem'
tblproperties ("skip.header.line.count"="2", "skip.footer.line.count"="1" );

ALTER TABLE fs_mkgmem ADD PARTITION (year='2016',month='01') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2016/01';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2016',month='02') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2016/02';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2016',month='03') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2016/03';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2016',month='04') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2016/04';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2016',month='05') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2016/05';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2016',month='06') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2016/06';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2016',month='07') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2016/07';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2016',month='08') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2016/08';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2016',month='09') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2016/09';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2016',month='10') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2016/10';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2016',month='11') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2016/11';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2016',month='12') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2016/12';

ALTER TABLE fs_mkgmem ADD PARTITION (year='2017',month='01') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2017/01';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2017',month='02') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2017/02';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2017',month='03') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2017/03';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2017',month='04') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2017/04';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2017',month='05') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2017/05';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2017',month='06') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2017/06';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2017',month='07') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2017/07';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2017',month='08') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2017/08';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2017',month='09') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2017/09';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2017',month='10') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2017/10';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2017',month='11') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2017/11';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2017',month='12') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2017/12';

ALTER TABLE fs_mkgmem ADD PARTITION (year='2018',month='01') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2018/01';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2018',month='02') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2018/02';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2018',month='03') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2018/03';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2018',month='04') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2018/04';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2018',month='05') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2018/05';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2018',month='06') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2018/06';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2018',month='07') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2018/07';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2018',month='08') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2018/08';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2018',month='09') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2018/09';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2018',month='10') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2018/10';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2018',month='11') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2018/11';
ALTER TABLE fs_mkgmem ADD PARTITION (year='2018',month='12') LOCATION '/yogya-bigdata-prd/fact_mkgmem/2018/12';

hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201701* /yogya-bigdata-prd/fact_mkgmem/2017/01
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201702* /yogya-bigdata-prd/fact_mkgmem/2017/02
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201703* /yogya-bigdata-prd/fact_mkgmem/2017/03
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201704* /yogya-bigdata-prd/fact_mkgmem/2017/04
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201705* /yogya-bigdata-prd/fact_mkgmem/2017/05
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201706* /yogya-bigdata-prd/fact_mkgmem/2017/06
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201707* /yogya-bigdata-prd/fact_mkgmem/2017/07
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201708* /yogya-bigdata-prd/fact_mkgmem/2017/08
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201709* /yogya-bigdata-prd/fact_mkgmem/2017/09
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201710* /yogya-bigdata-prd/fact_mkgmem/2017/10
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201711* /yogya-bigdata-prd/fact_mkgmem/2017/11
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201712* /yogya-bigdata-prd/fact_mkgmem/2017/12

hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201801* /yogya-bigdata-prd/fact_mkgmem/2018/01
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201802* /yogya-bigdata-prd/fact_mkgmem/2018/02
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201803* /yogya-bigdata-prd/fact_mkgmem/2018/03
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201804* /yogya-bigdata-prd/fact_mkgmem/2018/04
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201805* /yogya-bigdata-prd/fact_mkgmem/2018/05
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201806* /yogya-bigdata-prd/fact_mkgmem/2018/06
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201807* /yogya-bigdata-prd/fact_mkgmem/2018/07
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201808* /yogya-bigdata-prd/fact_mkgmem/2018/08
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201809* /yogya-bigdata-prd/fact_mkgmem/2018/09
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201810* /yogya-bigdata-prd/fact_mkgmem/2018/10
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201811* /yogya-bigdata-prd/fact_mkgmem/2018/11
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmem/MkgMem201812* /yogya-bigdata-prd/fact_mkgmem/2018/12


DROP TABLE fact_mkgmem_2016;

CREATE TABLE fact_mkgmem_2016 (
    postra_id   string,
    store_code  string,
    date_trn    string,
    termnmbr    string,
    transnmbr   string,
    member      string,
    cshrnmbr    string,
    empname     string
)
PARTITIONED BY (year string, month string)
CLUSTERED BY (postra_id) INTO 10 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP TABLE fact_mkgmem_2017;

CREATE TABLE fact_mkgmem_2017 (
    postra_id   string,
    store_code  string,
    date_trn    string,
    termnmbr    string,
    transnmbr   string,
    member      string,
    cshrnmbr    string,
    empname     string
)
PARTITIONED BY (year string, month string)
CLUSTERED BY (postra_id) INTO 10 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP TABLE fact_mkgmem_2018;

CREATE TABLE fact_mkgmem_2018 (
    postra_id   string,
    store_code  string,
    date_trn    string,
    termnmbr    string,
    transnmbr   string,
    member      string,
    cshrnmbr    string,
    empname     string
)
PARTITIONED BY (year string, month string)
CLUSTERED BY (postra_id) INTO 10 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');


DROP VIEW fact_mkgmem;

CREATE VIEW fact_mkgmem PARTITIONED ON (year,month)
AS
    SELECT
        postra_id,
        store_code,
        date_trn,
        termnmbr,
        transnmbr,
        member,
        cshrnmbr,
        empname,
        year,
        month
     FROM  fact_mkgmem_2016
     UNION ALL
    SELECT
        postra_id,
        store_code,
        date_trn,
        termnmbr,
        transnmbr,
        member,
        cshrnmbr,
        empname,
        year,
        month
     FROM  fact_mkgmem_2017
     UNION ALL
    SELECT
        postra_id,
        store_code,
        date_trn,
        termnmbr,
        transnmbr,
        member,
        cshrnmbr,
        empname,
        year,
        month
     FROM  fact_mkgmem_2018;

