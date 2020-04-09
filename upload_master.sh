#!/usr/bin/env bash

# ###############################################################
# Purpose       : Upload file Master to Bigdata Cloud
# Server        : 172.16.9.130
# Directory     : /arch/batch/BIG_DATA
# Crontab       : 00 01 * * * /arch/batch/BIG_DATA/upload_master.sh
# ###############################################################

#!/bin/sh
. ~/.profile

#generate File Article
if sqlplus -s $USERID @/arch/batch/BIG_DATA/gen_article.sql; then
    echo "OK"
else
    echo "NOK"
fi

LOG_FILE=/home/oracle/log/art_master-`date +"%Y%m%d"`.log;
DIR=/arch/batch/BIG_DATA/ART;
PTH=/data/yogyadata/article;

FILENAME=ART_MASTER.TXT

cd ${DIR}/
gzip -f $FILENAME
echo "Started upload fle ${DIR}/$FILENAME  `date '+%Y-%m-%d %H:%M:%S'`   " >> $LOG_FILE
scp -rp -q ${DIR}/$FILENAME.gz  hdfs@129.154.85.159:${PTH}/

ssh  hdfs@129.154.85.159 << EOF
   gunzip -f ${PTH}/$FILENAME.gz  >> /var/lib/hadoop-hdfs/log/article.log 2>&1
   hdfs dfs -put -f ${PTH}/$FILENAME /yogya-bigdata-prd/dim_article >> /var/lib/hadoop-hdfs/log/master.log 2>&1
   /var/lib/hadoop-hdfs/spark-python/./run_master.sh
EOF

echo "Finish upload fle ${DIR}/$FILENAME  `date '+%Y-%m-%d %H:%M:%S'`   " >> $LOG_FILE

