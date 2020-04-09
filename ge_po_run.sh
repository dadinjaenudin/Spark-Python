#!/bin/bash
#=================================================
# Proses from fle looping from date
# Format File : 010518 01052018 2018-05-01
#==================================================

#file=date_process.text
#while IFS=' ' read -r f1 f2 f3
#do
#    echo "field # 1 : $f1 ==> field #2 : $f2  ==> field #3 : $f3 "
#    ./gen_po_daily.sh $f1 $f2 $f3
#done < "$file"

#====================================================
# Proses automatic
# get date yesterday
#====================================================
f1=`TZ=aaa24 date +%d%m%y`
f2=`TZ=aaa24 date +%d%m%Y`
f3=`TZ=aaa24 date +%Y-%m-%d`
echo $f1
echo $f2
echo $f3

./gen_po_daily.sh $f1 $f2 $f3
