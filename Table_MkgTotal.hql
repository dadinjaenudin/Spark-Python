'''

hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2016
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2016/01
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2016/02
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2016/03
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2016/04
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2016/05
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2016/06
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2016/07
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2016/08
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2016/09
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2016/10
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2016/11
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2016/12

hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2017
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2017/01
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2017/02
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2017/03
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2017/04
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2017/05
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2017/06
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2017/07
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2017/08
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2017/09
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2017/10
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2017/11
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2017/12

hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2018
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2018/01
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2018/02
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2018/03
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2018/04
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2018/05
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2018/06
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2018/07
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2018/08
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2018/09
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2018/10
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2018/11
hdfs dfs -mkdir /yogya-bigdata-prd/fact_mkgtotal/2018/12

hdfs dfs -chmod -R 777 /yogya-bigdata-prd/fact_mkgtotal

'''

drop table fs_mkgtotal;

CREATE EXTERNAL TABLE IF NOT EXISTS fs_mkgtotal(
store_code  string,
date_trn    string,
termnmbr    string,
transnmbr   string,
amt         float,
posgrpnmbr  string
) PARTITIONED BY (year string, month string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/yogya-bigdata-prd/fact_mkgtotal'
tblproperties ("skip.header.line.count"="2", "skip.footer.line.count"="1" );


ALTER TABLE fs_mkgtotal ADD PARTITION (year='2016',month='01') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2016/01';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2016',month='02') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2016/02';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2016',month='03') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2016/03';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2016',month='04') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2016/04';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2016',month='05') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2016/05';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2016',month='06') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2016/06';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2016',month='07') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2016/07';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2016',month='08') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2016/08';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2016',month='09') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2016/09';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2016',month='10') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2016/10';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2016',month='11') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2016/11';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2016',month='12') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2016/12';


ALTER TABLE fs_mkgtotal ADD PARTITION (year='2017',month='01') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2017/01';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2017',month='02') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2017/02';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2017',month='03') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2017/03';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2017',month='04') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2017/04';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2017',month='05') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2017/05';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2017',month='06') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2017/06';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2017',month='07') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2017/07';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2017',month='08') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2017/08';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2017',month='09') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2017/09';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2017',month='10') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2017/10';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2017',month='11') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2017/11';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2017',month='12') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2017/12';

ALTER TABLE fs_mkgtotal ADD PARTITION (year='2018',month='01') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2018/01';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2018',month='02') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2018/02';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2018',month='03') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2018/03';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2018',month='04') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2018/04';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2018',month='05') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2018/05';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2018',month='06') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2018/06';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2018',month='07') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2018/07';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2018',month='08') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2018/08';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2018',month='09') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2018/09';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2018',month='10') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2018/10';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2018',month='11') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2018/11';
ALTER TABLE fs_mkgtotal ADD PARTITION (year='2018',month='12') LOCATION '/yogya-bigdata-prd/fact_mkgtotal/2018/12';


hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201701* /yogya-bigdata-prd/fact_mkgtotal/2017/01
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201702* /yogya-bigdata-prd/fact_mkgtotal/2017/02
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201703* /yogya-bigdata-prd/fact_mkgtotal/2017/03
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201704* /yogya-bigdata-prd/fact_mkgtotal/2017/04
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201705* /yogya-bigdata-prd/fact_mkgtotal/2017/05
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201706* /yogya-bigdata-prd/fact_mkgtotal/2017/06
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201707* /yogya-bigdata-prd/fact_mkgtotal/2017/07
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201708* /yogya-bigdata-prd/fact_mkgtotal/2017/08
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201709* /yogya-bigdata-prd/fact_mkgtotal/2017/09
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201710* /yogya-bigdata-prd/fact_mkgtotal/2017/10
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201711* /yogya-bigdata-prd/fact_mkgtotal/2017/11
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201712* /yogya-bigdata-prd/fact_mkgtotal/2017/12

hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201801* /yogya-bigdata-prd/fact_mkgtotal/2018/01
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201802* /yogya-bigdata-prd/fact_mkgtotal/2018/02
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201803* /yogya-bigdata-prd/fact_mkgtotal/2018/03
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201804* /yogya-bigdata-prd/fact_mkgtotal/2018/04
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201805* /yogya-bigdata-prd/fact_mkgtotal/2018/05
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201806* /yogya-bigdata-prd/fact_mkgtotal/2018/06
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201807* /yogya-bigdata-prd/fact_mkgtotal/2018/07
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201808* /yogya-bigdata-prd/fact_mkgtotal/2018/08
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201809* /yogya-bigdata-prd/fact_mkgtotal/2018/09
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201810* /yogya-bigdata-prd/fact_mkgtotal/2018/10
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201811* /yogya-bigdata-prd/fact_mkgtotal/2018/11
hdfs dfs -mv /yogya-bigdata-prd/fact_mkgtotal/MkgTotal201812* /yogya-bigdata-prd/fact_mkgtotal/2018/12

DROP TABLE dim_mkggroup;

CREATE TABLE dim_mkggroup (
    grp_kode    int,
    grp_desc    string
)
CLUSTERED BY(grp_kode) INTO 3 BUCKETS
STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table dim_mkggroup values(1,'FASHION');
insert into table dim_mkggroup values(2,'SUPERMARKET');
insert into table dim_mkggroup values(3,'MART');
insert into table dim_mkggroup values(4,'MO');
insert into table dim_mkggroup values(5,'GTMS');
insert into table dim_mkggroup values(6,'Cafe');
insert into table dim_mkggroup values(7,'Exp');
insert into table dim_mkggroup values(8,'');
insert into table dim_mkggroup values(9,'Yomart');


DROP TABLE fact_mkgtotal_2016;

CREATE TABLE fact_mkgtotal_2016 (
    postra_id   string,
    store_code  string,
    date_trn    string,
    termnmbr    string,
    transnmbr   string,
    amt         float,
    posgrpnmbr  int
)
PARTITIONED BY (year string, month string)
CLUSTERED BY (postra_id) INTO 10 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');



DROP TABLE fact_mkgtotal_2017;

CREATE TABLE fact_mkgtotal_2017 (
    postra_id   string,
    store_code  string,
    date_trn    string,
    termnmbr    string,
    transnmbr   string,
    amt         float,
    posgrpnmbr  int
)
PARTITIONED BY (year string, month string)
CLUSTERED BY (postra_id) INTO 10 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');

DROP TABLE fact_mkgtotal_2018;

CREATE TABLE fact_mkgtotal_2018 (
    postra_id   string,
    store_code  string,
    date_trn    string,
    termnmbr    string,
    transnmbr   string,
    amt         float,
    posgrpnmbr  int
)
PARTITIONED BY (year string, month string)
CLUSTERED BY (postra_id) INTO 10 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true');


DROP VIEW fact_mkgtotal;

CREATE VIEW fact_mkgtotal PARTITIONED ON (year,month)
AS
    SELECT
        postra_id,
        store_code,
        date_trn,
        termnmbr,
        transnmbr,
        amt ,
        posgrpnmbr,
        year,
        month
     FROM  fact_mkgtotal_2016
     UNION ALL
    SELECT
        postra_id,
        store_code,
        date_trn,
        termnmbr,
        transnmbr,
        amt ,
        posgrpnmbr,
        year,
        month
     FROM  fact_mkgtotal_2017
     UNION ALL
    SELECT
        postra_id,
        store_code,
        date_trn,
        termnmbr,
        transnmbr,
        amt ,
        posgrpnmbr,
        year,
        month
     FROM  fact_mkgtotal_2018;

