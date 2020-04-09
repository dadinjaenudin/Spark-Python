#!/bin/sh

. ~/.profile

LOG_FILE=/u07/mkg/log_hdfs/log-upload-manual_hdfs-`date +"%Y%m%d"`.log
PTH=/data/yogyadata/mkg

echo "Startd upload to cloud  `date '+%Y-%m-%d %H:%M:%S'`   "
echo "Started upload to cloud  `date '+%Y-%m-%d %H:%M:%S'`   " >> $LOG_FILE

######################################################
# Please Add Parameter ############################
######################################################
#Short Year
YEAR=18
#Full Year
YEAR_FULL=2018
#Short Month
MONTH=07
######################################################

cd /u07/mkg
tar -cvf mkg_${YEAR}${MONTH}.tar ${YEAR}${MONTH}/
gzip -f mkg_${YEAR}${MONTH}.tar
scp -r -q mkg_${YEAR}${MONTH}.tar.gz  hdfs@129.154.85.159:${PTH}/

cd /u07/mkg
tar -cvf mkg_${YEAR}${MONTH}.tar ${YEAR}${MONTH}/
gzip -f mkg_${YEAR}${MONTH}.tar
scp -r -q mkg_${YEAR}${MONTH}.tar.gz  hdfs@129.154.85.159:${PTH}/

ssh  hdfs@129.154.85.159 << EOF
     cd ${PTH}
     tar -zxvf mkg_${YEAR}${MONTH}.tar.gz
     rm mkg_${YEAR}${MONTH}.tar.gz
     cd ${YEAR}${MONTH}
     hdfs dfs -put -f MkgMed* /yogya-bigdata-prd/fact_mkgmed/${YEAR_FULL}/${MONTH}
     hdfs dfs -put -f MkgMem* /yogya-bigdata-prd/fact_mkgmem/${YEAR_FULL}/${MONTH}
     hdfs dfs -put -f MkgSupInt* /yogya-bigdata-prd/fact_mkgsupint/${YEAR_FULL}/${MONTH}
     hdfs dfs -put -f MkgTot* /yogya-bigdata-prd/fact_mkgtotal/${YEAR_FULL}/${MONTH}
     hdfs dfs -put -f MkgTrn* /yogya-bigdata-prd/fact_mkgtrn/${YEAR_FULL}/${MONTH}
     /var/lib/hadoop-hdfs/spark-python/./run_refresh_mkg.sh ${YEAR_FULL} ${MONTH}
EOF
echo "Finish upload to cloud  `date '+%Y-%m-%d %H:%M:%S'`   "
echo "Finish upload to cloud  `date '+%Y-%m-%d %H:%M:%S'`   " >> $LOG_FILE
