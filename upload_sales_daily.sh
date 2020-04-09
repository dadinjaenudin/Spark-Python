#!/bin/sh

# ###############################################################
# Purpose       : Upload file Sales Gold to Bigdata Cloud
# Server        : 172.16.9.130
# Directory     : /arch/batch/BIG_DATA
# Crontab       : 00 07 * * * /arch/batch/BIG_DATA/upload_sales_daily.sh
# ###############################################################

. ~/.profile

# In order to "zip" a directory, the correct command would be
# tar -cvf archive.tar directory/
# gzip archive.tar
# To decompress and unpack the archive into the current directory you would use
# tar -zxvf archive.tar.gz

MONTH=08
YEAR=2018
LOG_FILE=/home/oracle/log/log-sales-upload-hdfs-`date +"%Y%m%d"`.log
PTH=/data/yogyadata/SALES;

echo "Started upload to cloud  `date '+%Y-%m-%d %H:%M:%S'`   "
echo "Started upload to cloud  `date '+%Y-%m-%d %H:%M:%S'`   " >> $LOG_FILE

cd /arch/batch/BIG_DATA/SALES/
# YEAR & MONTH needed by partiion
YEAR=`TZ=aaa24 date +%Y`
MONTH=`TZ=aaa24 date +%m`
YESTERDAY=`TZ=aaa24 date +%d%m%Y`
FILENAME=$YESTERDAY.TXT
echo ${FILENAME}

tar -cvf SALESGOLD_${FILENAME}.tar ${FILENAME}
gzip -f SALESGOLD_${FILENAME}.tar
scp -r -q SALESGOLD_${FILENAME}.tar.gz  hdfs@129.154.85.159:${PTH}/

#for d in $(ls ${DIR});
#do
#  echo "Started upload file ${d}  `date '+%Y-%m-%d %H:%M:%S'`   "
#  echo "Started upload file ${d}  `date '+%Y-%m-%d %H:%M:%S'`   " >> $LOG_FILE
#  scp -r -q ${d} hdfs@129.154.85.159:${PTH}/
#  echo "Finish upload fle ${d}  `date '+%Y-%m-%d %H:%M:%S'`   "
#  echo "Finish upload fle ${d}  `date '+%Y-%m-%d %H:%M:%S'`   " >> $LOG_FILE
#done;

ssh  hdfs@129.154.85.159 << EOF
     cd ${PTH}
     tar -zxvf SALESGOLD_${FILENAME}.tar.gz
     rm SALESGOLD_${FILENAME}.tar.gz
     hdfs dfs -put -f ${FILENAME} /yogya-bigdata-prd/fact_sales_gold/${YEAR}/${MONTH}
     /var/lib/hadoop-hdfs/spark-python/./run_sales_gold_daily.sh ${YEAR} ${MONTH}
EOF
cd /arch/batch/BIG_DATA/SALES/
rm SALESGOLD_${FILENAME}.tar.gz
echo "Finish upload to cloud  `date '+%Y-%m-%d %H:%M:%S'`   "
echo "Finish upload to cloud  `date '+%Y-%m-%d %H:%M:%S'`   " >> $LOG_FILE


