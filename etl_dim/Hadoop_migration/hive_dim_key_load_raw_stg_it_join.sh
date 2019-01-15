#!/bin/bash
########################################################################################################################
#
# Purpose: Join Item Table with other datasets for SAVEON
#
# Change History Record: 
#
# ======================================================================================================================
# DATE        |   AUTHOR NAME        |    CHANGE DESCRIPTION | JIRA_ID 
# ======================================================================================================================
# 06 April 2018 |   Vinod Gardas       |    New script created |i
# 10 May 2018	|   Inno	       |    Changed the excutable sql code to be dynamic based on dimension
# ----------------------------------------------------------------------------------------------------------------------
########################################################################################################################


echo
echo "**********************************************************************************"
echo
echo "Job name - $0"
echo
echo "Number of Parameters - $#"
echo

#Checks the number of Parameters passed 
if [ $# -ne 2 ]
then
        echo
        echo "***************************************************************************"
        echo
        echo "ERROR MESSAGE:"
        echo "--------------"
        echo
        echo "ERR001 -- SYNTAX ERROR. CHECK THE PARAMETERS GIVEN TO SHELL."
        echo ""
        echo "Usage: sh $0 <CONFIG_FILE>"
        echo "***************************************************************************"
        echo ""
        exit 1
fi

echo
echo "Script $0 execution started @ `date`"
echo

CONFIG_FILE=${1}
CONTAINER_ID=${2}

. ${CONFIG_FILE}

#initialize error trap functions to check the availability of files and directory
. $ERRORTRAPS


if [ ! -d ${LOGDIR} ]
then
        echo
        echo "The Log Directory does not exist"
        exit -1
fi

if [ ! -d ${SRCDIR} ]
then
        echo
        echo "The Source Directory does not exist"
        exit -1
fi

if [ ! -d ${GENDIR} ]
then
        echo
        echo "The Generated Scripts Directory does not exist"
        exit -1
fi

echo
echo "**********************************************************************************"
echo

JOB_STRT_TS=`date '+%Y-%m-%d_%H%M%S'`
export JOB_STRT_TS=${JOB_STRT_TS}_$$

META_INFO=`hive -e "SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;select client,'|',dim_name,'|',dim_abbr,'|',file_name_prefix FROM ${HIVE_DB}.${KEY_DEFN_TAB} WHERE CONTAINER_ID=${CONTAINER_ID};"`

if [ -z "${META_INFO}" ]
then
        echo
        echo " Error in fetching META_INFO from table ${HIVE_DB}.${KEY_DEFN_TAB} for container id ${CONTAINER_ID}"
        exit -1;
fi

echo
echo "Meta table info:  $META_INFO"
echo

CLIENT_LWR=`echo ${CLIENT} | awk '{print tolower($0)}'`
CLIENT_UPR=`echo ${CLIENT} | awk '{print toupper($0)}'`
IT_TABLE=`echo ${IT_TABLE} | awk '{print toupper($0)}'`
LV_TABLE=`echo ${LV_TABLE} | awk '{print toupper($0)}'`
STG_JOIN_TAB=`echo ${STG_JOIN_TAB} | awk '{print toupper($0)}'`


FILE_NAME_PREFIX=`echo ${META_INFO} | cut -d'|' -f4`

DIM_NAME=`echo ${META_INFO} | cut -d'|' -f2`
DIM_ABBR=`echo ${META_INFO} | cut -d'|' -f3`

DIM_ABBR_UPR=`echo ${DIM_ABBR} | awk '{print toupper($0)}'`
DIM_SQL_PATH=${DIM_ABBR_UPR}.hql

echo "${DIM_ABBR_UPR} SQL PATH IS "  ${DIM_SQL_PATH}

DIM_NAME_LWR=`echo ${DIM_NAME} | awk '{print tolower($0)}'`
#DIM_NAME_UPR=`echo ${DIM_NAME} | awk '{print toupper($0)}'`
DIM_ABBR_LWR=`echo ${DIM_ABBR} | awk '{print tolower($0)}'`

echo
echo "Executing ${DIM_SQL_PATH}"
echo

hive -hiveconf db=${HIVE_DB} -f ${DIM_SQL_PATH}


echo
echo "Script execution completed @ `date`"

