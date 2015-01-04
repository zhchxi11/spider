#!/bin/sh
source /home/disk5/dm/.bashrc
cur_day=`date +%Y%m%d`
year=`date +%Y`
month=`date +%m`
day=`date +%d`
last_year=`date -d 'last day' +%Y`
last_month=`date -d 'last day' +%m`
last_day=`date -d 'last day' +%d`
output_dir="/user/rd/dm/spider/app_cate/${year}/${month}/${day}"
last_dir="/user/rd/dm/spider/app_cate/${last_year}/${last_month}/${last_day}"

python crawler.py 1> data/bd_assistent_info_${cur_day} 2>run_${cur_day}.log
if [ $? -ne 0 ];then
    echo "crawler failed"
    php error.php "crawler failed"
    exit 1
fi
${HADOOP_HOME}/bin/hadoop fs -rmr ${output_dir}
${HADOOP_HOME}/bin/hadoop fs -mkdir -p ${output_dir}

hive -e "
use dm;
create table if not exists app_cate
(first_level string,
sec_level string,
third_level string,
app_name string,
down_num string,
size string) 
PARTITIONED BY ( 
  year string, 
  month string, 
  day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
LOCATION \"/user/rd/dm/spider/app_cate/\";

alter table app_cate  ADD IF NOT EXISTS PARTITION (year = '$year', month = '$month', day = '$day') LOCATION \"${output_dir}\";
"
${HADOOP_HOME}/bin/hadoop fs -test -e ${last_dir}
if [ $? -eq 0 ];then
    rm -fr data/last_info
    ${HADOOP_HOME}/bin/hadoop fs -get ${last_dir}/part-00000 data/last_info
    awk -F'\t' 'BEGIN{OFS="\t"}ARGIND==1{info[$4]=$0;print $0;}ARGIND==2{if(info[$4]) next; print $0}' data/last_info data/bd_assistent_info_${cur_day} > data/bd_assistent_info
else
    mv data/bd_assistent_info_${cur_day}  data/bd_assistent_info
fi
${HADOOP_HOME}/bin/hadoop fs -put data/bd_assistent_info ${output_dir}/part-00000
