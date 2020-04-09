'''

hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2016
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2016/01
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2016/02
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2016/03
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2016/04
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2016/05
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2016/06
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2016/07
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2016/08
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2016/09
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2016/10
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2016/11
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2016/12

hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2017
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2017/01
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2017/02
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2017/03
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2017/04
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2017/05
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2017/06
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2017/07
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2017/08
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2017/09
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2017/10
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2017/11
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2017/12

hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2018
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2018/01
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2018/02
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2018/03
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2018/04
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2018/05
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2018/06
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2018/07
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2018/08
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2018/09
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2018/10
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2018/11
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtrn/2018/12

hdfs dfs -chmod -R 777 /yogya-bigdata-prd/fact_mkgtrn

'''

drop table fs_mkgtrn;

CREATE EXTERNAL TABLE IF NOT EXISTS fs_mkgtrn(
store_code  string,
date_trn    string,
termnmbr    string,
transnmbr   string,
art_code    string,
pludesc     string,
division    string,
gross       float,
qty         float,
disc        float,
sp          float,
skunmbr     string
) PARTITIONED BY (year string, month string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/yogya-bigdata-prd/fact_mkgtrn'
tblproperties ("skip.header.line.count"="1", "skip.footer.line.count"="1" );

ALTER TABLE fs_mkgtrn ADD PARTITION (year='2016',month='01') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2016/01';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2016',month='02') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2016/02';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2016',month='03') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2016/03';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2016',month='04') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2016/04';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2016',month='05') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2016/05';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2016',month='06') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2016/06';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2016',month='07') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2016/07';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2016',month='08') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2016/08';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2016',month='09') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2016/09';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2016',month='10') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2016/10';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2016',month='11') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2016/11';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2016',month='12') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2016/12';

ALTER TABLE fs_mkgtrn ADD PARTITION (year='2017',month='01') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2017/01';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2017',month='02') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2017/02';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2017',month='03') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2017/03';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2017',month='04') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2017/04';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2017',month='05') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2017/05';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2017',month='06') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2017/06';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2017',month='07') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2017/07';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2017',month='08') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2017/08';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2017',month='09') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2017/09';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2017',month='10') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2017/10';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2017',month='11') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2017/11';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2017',month='12') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2017/12';

ALTER TABLE fs_mkgtrn ADD PARTITION (year='2018',month='01') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2018/01';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2018',month='02') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2018/02';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2018',month='03') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2018/03';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2018',month='04') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2018/04';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2018',month='05') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2018/05';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2018',month='06') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2018/06';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2018',month='07') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2018/07';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2018',month='08') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2018/08';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2018',month='09') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2018/09';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2018',month='10') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2018/10';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2018',month='11') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2018/11';
ALTER TABLE fs_mkgtrn ADD PARTITION (year='2018',month='12') LOCATION '/yogya-bigdata-prd/fact_mkgtrn/2018/12';

DROP TABLE fact_mkgtrn_2016;

CREATE TABLE fact_mkgtrn_2016 (
    postra_id   string,
    store_code  string,
    date_trn    date,
    termnmbr    string,
    transnmbr   string,
    art_code    string,
    pludesc     string,
    division    string,
    skunmbr     string,
    gross       float,
    qty         float,
    disc        float,
    sp          float
)
PARTITIONED BY (year string, month string)
CLUSTERED BY (postra_id) INTO 10 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP TABLE fact_mkgtrn_2017;

CREATE TABLE fact_mkgtrn_2017 (
    postra_id   string,
    store_code  string,
    date_trn    date,
    termnmbr    string,
    transnmbr   string,
    art_code    string,
    pludesc     string,
    division    string,
    skunmbr     string,
    gross       float,
    qty         float,
    disc        float,
    sp          float
)
PARTITIONED BY (year string, month string)
CLUSTERED BY (postra_id) INTO 10 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP TABLE fact_mkgtrn_2018;

CREATE TABLE fact_mkgtrn_2018 (
    postra_id   string,
    store_code  string,
    date_trn    date,
    termnmbr    string,
    transnmbr   string,
    art_code    string,
    pludesc     string,
    division    string,
    skunmbr     string,
    gross       float,
    qty         float,
    disc        float,
    sp          float
)
PARTITIONED BY (year string, month string)
CLUSTERED BY (postra_id) INTO 10 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');


DROP VIEW fact_mkgtrn;

CREATE VIEW fact_mkgtrn PARTITIONED ON (year,month)
AS
    SELECT
        postra_id,
        store_code,
        date_trn,
        termnmbr,
        transnmbr,
        art_code,
        pludesc,
        division,
        skunmbr,
        gross,
        qty,
        disc,
        sp,
        year,
        month
     FROM  fact_mkgtrn_2016
     UNION ALL
    SELECT
        postra_id,
        store_code,
        date_trn,
        termnmbr,
        transnmbr,
        art_code,
        pludesc,
        division,
        skunmbr,
        gross,
        qty,
        disc,
        sp,
        year,
        month
     FROM  fact_mkgtrn_2017
     UNION ALL
    SELECT
        postra_id,
        store_code,
        date_trn,
        termnmbr,
        transnmbr,
        art_code,
        pludesc,
        division,
        skunmbr,
        gross,
        qty,
        disc,
        sp,
        year,
        month
     FROM  fact_mkgtrn_2018;

