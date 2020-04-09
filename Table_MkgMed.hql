'''
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2016
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2016/01
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2016/02
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2016/03
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2016/04
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2016/05
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2016/06
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2016/07
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2016/08
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2016/09
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2016/10
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2016/11
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2016/12

hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2017
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2017/01
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2017/02
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2017/03
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2017/04
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2017/05
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2017/06
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2017/07
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2017/08
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2017/09
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2017/10
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2017/11
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2017/12

hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2018
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2018/01
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2018/02
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2018/03
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2018/04
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2018/05
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2018/06
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2018/07
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2018/08
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2018/09
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2018/10
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2018/11
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgmed/2018/12

hdfs dfs -chmod -R 777 /yogya-bigdata-prd/fact_mkgmed

'''

drop table fs_mkgmed;

CREATE EXTERNAL TABLE IF NOT EXISTS fs_mkgmed(
    store_code  string,
    date_trn    string,
    termnmbr    string,
    transnmbr   string,
    mdesc       string,
    mediaamnt   float,
    accountnmbr string
) PARTITIONED BY (year string, month string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/yogya-bigdata-prd/fact_mkgmed'
tblproperties ("skip.header.line.count"="2", "skip.footer.line.count"="1" );

ALTER TABLE fs_mkgmed ADD PARTITION (year='2016',month='01') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2016/01';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2016',month='02') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2016/02';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2016',month='03') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2016/03';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2016',month='04') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2016/04';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2016',month='05') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2016/05';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2016',month='06') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2016/06';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2016',month='07') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2016/07';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2016',month='08') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2016/08';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2016',month='09') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2016/09';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2016',month='10') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2016/10';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2016',month='11') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2016/11';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2016',month='12') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2016/12';

ALTER TABLE fs_mkgmed ADD PARTITION (year='2017',month='01') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2017/01';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2017',month='02') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2017/02';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2017',month='03') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2017/03';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2017',month='04') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2017/04';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2017',month='05') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2017/05';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2017',month='06') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2017/06';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2017',month='07') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2017/07';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2017',month='08') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2017/08';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2017',month='09') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2017/09';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2017',month='10') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2017/10';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2017',month='11') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2017/11';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2017',month='12') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2017/12';

ALTER TABLE fs_mkgmed ADD PARTITION (year='2018',month='01') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2018/01';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2018',month='02') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2018/02';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2018',month='03') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2018/03';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2018',month='04') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2018/04';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2018',month='05') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2018/05';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2018',month='06') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2018/06';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2018',month='07') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2018/07';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2018',month='08') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2018/08';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2018',month='09') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2018/09';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2018',month='10') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2018/10';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2018',month='11') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2018/11';
ALTER TABLE fs_mkgmed ADD PARTITION (year='2018',month='12') LOCATION '/yogya-bigdata-prd/fact_mkgmed/2018/12';

hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201701* /yogya-bigdata-prd/fact_mkgmed/2017/01
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201702* /yogya-bigdata-prd/fact_mkgmed/2017/02
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201703* /yogya-bigdata-prd/fact_mkgmed/2017/03
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201704* /yogya-bigdata-prd/fact_mkgmed/2017/04
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201705* /yogya-bigdata-prd/fact_mkgmed/2017/05
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201706* /yogya-bigdata-prd/fact_mkgmed/2017/06
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201707* /yogya-bigdata-prd/fact_mkgmed/2017/07
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201708* /yogya-bigdata-prd/fact_mkgmed/2017/08
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201709* /yogya-bigdata-prd/fact_mkgmed/2017/09
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201710* /yogya-bigdata-prd/fact_mkgmed/2017/10
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201711* /yogya-bigdata-prd/fact_mkgmed/2017/11
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201712* /yogya-bigdata-prd/fact_mkgmed/2017/12

hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201801* /yogya-bigdata-prd/fact_mkgmed/2018/01
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201802* /yogya-bigdata-prd/fact_mkgmed/2018/02
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201803* /yogya-bigdata-prd/fact_mkgmed/2018/03
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201804* /yogya-bigdata-prd/fact_mkgmed/2018/04
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201805* /yogya-bigdata-prd/fact_mkgmed/2018/05
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201806* /yogya-bigdata-prd/fact_mkgmed/2018/06
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201807* /yogya-bigdata-prd/fact_mkgmed/2018/07
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201808* /yogya-bigdata-prd/fact_mkgmed/2018/08
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201809* /yogya-bigdata-prd/fact_mkgmed/2018/09
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201810* /yogya-bigdata-prd/fact_mkgmed/2018/10
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201811* /yogya-bigdata-prd/fact_mkgmed/2018/11
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgmed/MkgMed201812* /yogya-bigdata-prd/fact_mkgmed/2018/12

DROP TABLE fact_mkgmed_2016;

CREATE TABLE fact_mkgmed_2016 (
    postra_id   string,
    store_code  string,
    date_trn    string,
    termnmbr    string,
    transnmbr   string,
    mdesc       string,
    mediaamnt   float,
    accountnmbr string
)
PARTITIONED BY (year string, month string)
CLUSTERED BY (postra_id) INTO 10 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');


DROP TABLE fact_mkgmed_2017;

CREATE TABLE fact_mkgmed_2017 (
    postra_id   string,
    store_code  string,
    date_trn    string,
    termnmbr    string,
    transnmbr   string,
    mdesc       string,
    mediaamnt   float,
    accountnmbr string
)
PARTITIONED BY (year string, month string)
CLUSTERED BY (postra_id) INTO 10 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP TABLE fact_mkgmed_2018;

CREATE TABLE fact_mkgmed_2018 (
    postra_id   string,
    store_code  string,
    date_trn    string,
    termnmbr    string,
    transnmbr   string,
    mdesc       string,
    mediaamnt   float,
    accountnmbr string
)
PARTITIONED BY (year string, month string)
CLUSTERED BY (postra_id) INTO 10 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');


DROP VIEW fact_mkgmed;

CREATE VIEW fact_mkgmed PARTITIONED ON (year,month)
AS
    SELECT
        postra_id,
        store_code,
        date_trn,
        termnmbr,
        transnmbr,
        mdesc,
        mediaamnt ,
        accountnmbr,
        year,
        month
     FROM  fact_mkgmed_2016
     UNION ALL
    SELECT
        postra_id,
        store_code,
        date_trn,
        termnmbr,
        transnmbr,
        mdesc,
        mediaamnt ,
        accountnmbr,
        year,
        month
     FROM  fact_mkgmed_2017
     UNION ALL
    SELECT
        postra_id,
        store_code,
        date_trn,
        termnmbr,
        transnmbr,
        mdesc,
        mediaamnt ,
        accountnmbr,
        year,
        month
     FROM  fact_mkgmed_2018;

