#!/bin/bash
########################################################################################################################
#
# Purpose: Load raw data to External table and create Staging Table for SAVEON
#
# Change History Record: 
#
# ======================================================================================================================
# DATE        |   AUTHOR NAME        |    CHANGE DESCRIPTION | JIRA_ID 
# ======================================================================================================================
# 06 April 2018 |   Vinod Gardas     |    New script created |
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
        echo "Usage: sh $0 <CONFIG_FILE> <CONTAINER_ID>"
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

CLIENT_LWR=`echo ${CLIENT} | awk '{print tolower($0)}'`
#CLIENT_UPR=`echo ${CLIENT} | awk '{print toupper($0)}'`

echo
echo "Start Date : $JOB_STRT_TS"
echo "Container id : $CONTAINER_ID"
echo "Client Name: $CLIENT_LWR"
echo
echo


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

FILE_NAME_PREFIX_TAB=`echo ${META_INFO} | cut -d'|' -f4`
FILE_NAME_PREFIX=`echo ${FILE_NAME_PREFIX_TAB} | sed 's/ //g'`
#FILE_PREFIX=${FILE_NAME_PREFIX}


DIM_NAME=`echo ${META_INFO} | cut -d'|' -f2`
DIM_ABBR=`echo ${META_INFO} | cut -d'|' -f3`

DIM_NAME_LWR=`echo ${DIM_NAME} | awk '{print tolower($0)}'`
DIM_NAME_UPR=`echo ${DIM_NAME} | awk '{print toupper($0)}'`
DIM_ABBR_LWR=`echo ${DIM_ABBR} | awk '{print tolower($0)}'`
#DIM_ABBR_UPR=`echo ${DIM_ABBR} | awk '{print toupper($0)}'`

#FILE_SRC_DIR=${SRC_DIR}
#FILE_LOG_DIR=${FILE_LOG_DIR}
#FILEEXTENSION='txt'
#FILE_NAME=${SRC_DIR}/${FILE_NAME_PREFIX}*.txt
#FILE_NAME=${SRC_DIR}/${FILE_NAME_PREFIX}
#BASE_FILE_NAME=`basename ${FILE_NAME}`

#FILE_NAME_WITHOUT_PATH=${FILE_NAME_PREFIX}_${FILE_DATE}.t
#echo "SRC_DIR: ${SRC_DIR}"
#echo "FILE_NAME_PREFIX: ${FILE_NAME_PREFIX}"
#echo "FILE_NAME_WITHOUT_PATH: ${FILE_NAME_PREFIX}"

#echo "FILE_NAME_PREFIX1: $FILE_NAME_PREFIX1"
#echo "FILE_NAME :$FILE_NAME"

FILE_NAME=`find ${SRC_DIR}/${FILE_NAME_PREFIX}* -print | sort -r | head -n1`

echo "FILE_NAME: $FILE_NAME"
echo "SRC_DIR: ${SRC_DIR}"
echo "FILE_NAME_PREFIX: ${FILE_NAME_PREFIX}"


echo
echo "FILE_NAME: $FILE_NAME"
#echo "BASE_FILE_NAME: $BASE_FILE_NAME" 
#echo "FILE_NAME_WITHOUT_PATH: $FILE_NAME_WITHOUT_PATH"
#echo "FILE_SRC_DIR: $FILE_SRC_DIR"
echo "FILE_LOG_DIR: $FILE_LOG_DIR"
echo "FILE_DATE:" $FILE_DATE
echo


FILE_DATE_WH=${FILE_DATE}
echo
echo "FILE_DATE_WH : ${FILE_DATE_WH}"
echo

RAW_TAB=${CLIENT_LWR}_${DIM_ABBR_LWR}_ext
STG_TAB=${CLIENT_LWR}_${DIM_ABBR_LWR}_stg
echo
echo "RAW_TAB: $RAW_TAB"
echo "STG_TAB: $STG_TAB"
echo

echo
echo "Container id : $CONTAINER_ID"
echo "FILE_NAME: $FILE_NAME"
echo

TAB_DEFN=`hive -e "SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;SELECT COLUMN_META FROM ${HIVE_DB}.${KEY_DEFN_TAB} WHERE CONTAINER_ID=${CONTAINER_ID};"`

echo
echo "TAB_DEFN : ${TAB_DEFN}"
echo "*********************************************************************************"
echo

echo "RAW_TAB: $RAW_TAB"
echo "TAB_DEFN: $TAB_DEFN"

echo
echo "Dropping ${HIVE_DB}.${RAW_TAB}"
echo

hive -e "DROP TABLE IF EXISTS ${HIVE_DB}.${RAW_TAB}"

echo
echo "Creating ${HIVE_DB}.${RAW_TAB}"
echo

hive -e "CREATE TABLE IF NOT EXISTS ${HIVE_DB}.${RAW_TAB}
(
         ${TAB_DEFN}
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;
"

#hive -e "LOAD DATA LOCAL INPATH '/externaldata01/dev/saveon/rtlr_data/raw/OWFG_Item.20180904' INTO TABLE ${HIVE_DB}.${RAW_TAB};"
hive -e "LOAD DATA LOCAL INPATH '${FILE_NAME}' INTO TABLE ${HIVE_DB}.${RAW_TAB};"

echo
echo "CREATED HIVE_DB.RAW_TAB: ${HIVE_DB}.${RAW_TAB}"
echo

echo
echo "DROP TABLE IF EXISTS: ${HIVE_DB}.${STG_TAB}"
echo

hive -e "DROP TABLE IF EXISTS ${HIVE_DB}.${STG_TAB}"

echo
echo "Creating stage table: ${HIVE_DB}.${STG_TAB}"
echo

hive -e "CREATE TABLE IF NOT EXISTS ${HIVE_DB}.${STG_TAB} AS SELECT *, '${FILE_DATE}' AS file_date from ${HIVE_DB}.${RAW_TAB}"

echo
echo "Created stage table: ${HIVE_DB}.${STG_TAB}"
echo

echo "Script execution completed @ `date`"

