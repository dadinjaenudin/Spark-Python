#!/bin/sh

# ###############################################################
# Purpose       : Processing Jml_Transaksi after refresh Mkg
# Server        : 172.16.9.130
# Directory     : /u07/mkg
# Crontab       : # Process Jml_Transaksi after refress mkg
#                 00 03 * * * /u07/mkg/jml_transaksi.sh >>/u07/mkg/jml_transaksi.sh.log
# ###############################################################

. ~/.profile

LOG_FILE=/u07/mkg/log_hdfs/log-jml-transaksi-`date +"%Y%m%d"`.log
PTH=/data/yogyadata/mkg

echo "Started process jml_transaksi  `date '+%Y-%m-%d %H:%M:%S'`   "
echo "Started process jml_transaksi  `date '+%Y-%m-%d %H:%M:%S'`   " >> $LOG_FILE

# YEAR & MONTH needed by partiion
# full year 2018
YEAR_FULL=`TZ=aaa24 date +%Y`
# year=18
YEAR=`TZ=aaa24 date +%y`
MONTH=`TZ=aaa24 date +%m`
YESTERDAY=`TZ=aaa24 date +%d%m%Y`

ssh  hdfs@129.154.85.159 << EOF
     /var/lib/hadoop-hdfs/spark-python/./run_jml_transaksi.sh ${YEAR_FULL} ${MONTH}
EOF
echo "Finish process jml_transaksi `date '+%Y-%m-%d %H:%M:%S'`   "
echo "Finish process jml_transaksi  `date '+%Y-%m-%d %H:%M:%S'`   " >> $LOG_FILE