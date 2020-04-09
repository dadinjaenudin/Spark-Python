#!/bin/bash

. ~/.profile

DIR=/arch/batch/BIG_DATA/PO;
PTH=/data/yogyadata/PO;

# text file format
# 270418 27042018 2018-04-27

# date sql where format DDMMYY
DATE_=$1
# date file name
filename=$2.TXT
# date hive = 2018-04-01
DATE_HIVE=$3

#generate File sales
if sqlplus -s $USERID @/arch/batch/BIG_DATA/gen_po.sql $DATE_ $filename; then
    echo "OK"
else
    echo "NOK"
fi

# Substring
YEAR_FULL=${DATE_HIVE:0:4}
MONTH=${DATE_HIVE:5:2}
echo $YEAR_FULL
echo $MONTH

cd ${DIR}/
gzip -f $filename
echo "Started upload fle ${DIR}/$filename  `date '+%Y-%m-%d %H:%M:%S'`   " >> $LOG_FILE
scp -rp -q ${DIR}/$filename.gz  hdfs@129.154.85.159:${PTH}/

# proses data wihtout background job
ssh hdfs@129.154.85.159 << EOF
    gunzip -f ${PTH}/$filename.gz    >> /var/lib/hadoop-hdfs/log/po_refresh_art.log 2>&1
    hdfs dfs -put -f ${PTH}/$filename /yogya-bigdata-prd/fact_po/${YEAR_FULL}/${MONTH}
    /var/lib/hadoop-hdfs/spark-python/./run_service_level.sh ${YEAR_FULL} ${MONTH}
EOF
