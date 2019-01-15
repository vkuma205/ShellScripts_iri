#!/bin/bash

Usage(){
#clear
echo "LoopHiveSQL.sh [StartTimeDimKey] [EndTimeDimKey] [HQL_PATH]"
exit 9
}

OPTIND=$#
echo $#

if [[ $# -ne 3  ]] ;  then 
	echo "Missing Arguments!!!"
	Usage;
fi

StartTDK=$1
EndTDK=$2
HQL_PATH=$3

for (( c=$StartTDK; c<=$EndTDK; c++ ))
do  
   echo "["`date +%Y-%m-%d:%H:%M:%S`"] : Starting Load for Time Dim Key : "$c
   
   TIME_KEY_DAY=`hive -e "use wh_postx_saveon_p1; select FROM_UNIXTIME(UNIX_TIMESTAMP(day_dsc ,'MM-dd-yyyy'), 'yyyyMMdd') from tm_dim_day where tm_dim_key=${c};"`

   WEEK_START=`hive -e "use wh_postx_saveon_p1; select from_unixtime(UNIX_TIMESTAMP(flyer_end_date ,'yyyy.MM.dd') -6*60*60*24, 'yyyyMMdd') from time_stg where FROM_UNIXTIME(UNIX_TIMESTAMP(tdate ,'yyyy.MM.dd'), 'yyyyMMdd')=${TIME_KEY_DAY};"`

   WEEK_END=`hive -e "use wh_postx_saveon_p1; select FROM_UNIXTIME(UNIX_TIMESTAMP(flyer_end_date ,'yyyy.MM.dd'), 'yyyyMMdd') from time_stg where FROM_UNIXTIME(UNIX_TIMESTAMP(tdate ,'yyyy.MM.dd'), 'yyyyMMdd')=${TIME_KEY_DAY};"` 
   
   echo "end date key is $TIME_KEY_DAY";

   hive -hiveconf time_key=$c -hiveconf enddatekey=$TIME_KEY_DAY -hiveconf weekendkey=$WEEK_END -hiveconf weekstartkey=$WEEK_START -f $HQL_PATH
   RC=$?
   if [[ $RC -ne 0 ]]; then
	echo "["`date +%Y-%m-%d:%H:%M:%S`"] : Job Failed"
	exit 2;
   else 
	echo "["`date +%Y-%m-%d:%H:%M:%S`"] : Job Finished"
   fi
done
