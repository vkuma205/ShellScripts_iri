#!/bin/bash

########################################################################################################################
#
# Purpose:Merge keyeded data into Delta Master
#
# Change History Record:
#
# ======================================================================================================================
# DATE        |   AUTHOR NAME        |    CHANGE DESCRIPTION | JIRA ID
# ======================================================================================================================
# 06 April 2018 |   Vinod Gardas     | New script created    |   
# ----------------------------------------------------------------------------------------------------------------------
########################################################################################################################


echo
echo "######################################################################################"
echo
echo "Job name - $0"
echo
echo "Number of Parameters - $#"
echo

#syntax check
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

#initialize error trap functions
. $ERRORTRAPS

echo
echo "**********************************************************************************"
echo

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

#META_INFO=`sqlplus -s ${OPSBLD_USER}/${OPSBLD_PASSWD}@${OPSBLD_DB}<<EOF
#SET HEADING OFF ECHO OFF FEEDBACK OFF PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 500;
#WHENEVER SQLERROR EXIT 1
#SELECT CLIENT || '|' || DIM_NAME || '|' || DIM_ABBR || '|' || FILE_NAME_PREFIX || '|' || SURROGATE_KEY FROM ${KEY_DEFN_TAB} WHERE CONTAINER_ID=${CONTAINER_ID};
#EOF`

META_INFO=`hive -e "SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;select client,'|',dim_name,'|',dim_abbr,'|',file_name_prefix,'|',surrogate_key FROM ${HIVE_DB}.${KEY_DEFN_TAB} WHERE CONTAINER_ID=${CONTAINER_ID};"`
#testdberror $?

#if [ -z "${META_INFO}" ]
#then
       # echo
       # echo " Error in fetching META_INFO from table ${KEY_DEFN_TAB} on ${OPSBLD_USER}@${OPSBLD_DB} for container id ${CONTAINER_ID}"
       # exit -1;
#fi

#FILE_NAME_PREFIX=`echo ${META_INFO} | cut -d'|' -f4`

FILE_NAME_PREFIX_TAB=`echo ${META_INFO} | cut -d'|' -f4`
FILE_NAME_PREFIX=`echo ${FILE_NAME_PREFIX_TAB} | sed 's/ //g'`

DIM_NAME=`echo ${META_INFO} | cut -d'|' -f2`
DIM_ABBR=`echo ${META_INFO} | cut -d'|' -f3`

SURROGATE_KEY_TAB=`echo ${META_INFO} | cut -d'|' -f5`
SURROGATE_KEY=`echo ${SURROGATE_KEY_TAB} | sed 's/ //g'`

CLIENT_LWR=`echo ${CLIENT} | awk '{print tolower($0)}'`
CLIENT_UPR=`echo ${CLIENT} | awk '{print toupper($0)}'`

DIM_NAME_LWR=`echo ${DIM_NAME} | awk '{print tolower($0)}'`
DIM_NAME_UPR=`echo ${DIM_NAME} | awk '{print toupper($0)}'`
DIM_ABBR_LWR=`echo ${DIM_ABBR} | awk '{print tolower($0)}'`
DIM_ABBR_UPR=`echo ${DIM_ABBR} | awk '{print toupper($0)}'`

RAW_TAB=${CLIENT_UPR}_${DIM_ABBR_UPR}_EXT_${FILE_DATE}
#RAW_TAB=${CLIENT_UPR}_${DIM_ABBR_UPR}_EXT
STG_TAB=${CLIENT_UPR}_${DIM_ABBR_UPR}_STG
#STG_TAB=${CLIENT_UPR}_${DIM_ABBR_UPR}_STG_${FILE_DATE}

EAL_STG_TAB=eal_dk_${CLIENT_LWR}_${DIM_ABBR_LWR}_stg
EAL_MSTR_TAB=eal_dk_${CLIENT_LWR}_${DIM_ABBR_LWR}_mstr
EAL_DELTA_TAB=eal_dk_${CLIENT_LWR}_${DIM_ABBR_LWR}_delta
EAL_DELTA_TAB_TMP=eal_dk_${CLIENT_LWR}_${DIM_ABBR_LWR}_delta_tmp
#EAL_MSTR_TAB_EXT=EAL_DK_${CLIENT_UPR}_${DIM_ABBR_UPR}_MSTR_MDM
#EAL_DELTA_TAB_EXT=EAL_DK_${CLIENT_UPR}_${DIM_ABBR_UPR}_DELTA_MDM

EAL_DELTA_TAB_KEYED=eal_${CLIENT_LWR}_${DIM_ABBR_LWR}_dlt_kyd

echo
echo "**********************************************************************************"
echo


#FACT_SELECT=`sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<EOF
#set echo off verify off feed off termout off pages 0 lines 10000
#SELECT LISTAGG(COLUMN_NAME,',') WITHIN GROUP(ORDER BY COLUMN_ID) FROM USER_TAB_COLS WHERE UPPER(TABLE_NAME)=UPPEiR('${EAL_MSTR_TAB}');
#EOF`

FACT_SELECT_TMP=`hive -e "SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;SHOW COLUMNS IN ${HIVE_DB}.${EAL_DELTA_TAB};"`
FACT_SELECT=`echo ${FACT_SELECT_TMP} | sed '$!s/$/,/'  | sed 's/ /,/g'`

FACT_EX_KEY_TMP=`hive -e "SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;SHOW COLUMNS IN ${HIVE_DB}.${EAL_DELTA_TAB_TMP};"`
FACT_EX_KEY=`echo ${FACT_EX_KEY_TMP} | sed '$!s/$/,/' | sed 's/ /,/g'`


echo $FACT_EX_KEY

# | sed '$!s/$/,/'

if [ -z "${FACT_SELECT}" ]
then
        echo
        echo "FACT_SELECT not fetched properly"
        exit -1;
fi


echo
echo "FACT_SELECT_TAB_PREFIX : ${FACT_SELECT_TAB_PREFIX}"
echo

echo
echo "FACT_SELECT : ${FACT_SELECT}"
echo

echo
echo "**********************************************************************************"
echo

#NATURAL_KEY_LIST=`sqlplus -s ${OPSBLD_USER}/${OPSBLD_PASSWD}@${OPSBLD_DB}<<EOF
#set echo off verify off feed off termout off pages 0 lines 500
#SELECT REPLACE(NATURAL_KEY, '|',' || ''~'' || ') FROM ${KEY_DEFN_TAB} WHERE CONTAINER_ID=${CONTAINER_ID};
#EOF`

NATURAL_KEY_LIST=`hive -e "SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;SELECT NATURAL_KEY FROM ${HIVE_DB}.${KEY_DEFN_TAB} WHERE CONTAINER_ID=${CONTAINER_ID};"`

if [ -z "${NATURAL_KEY_LIST}" ]
then
        echo
        echo " Error in fetching NATURAL_KEY list "
        exit -1;
fi

echo
echo "NATURAL_KEY_LIST : ${NATURAL_KEY_LIST}"
echo

echo
echo "**********************************************************************************"
echo

echo
echo "use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; drop table if exists ${EAL_DELTA_TAB}_bak;create table  ${EAL_DELTA_TAB}_bak as select distinct k.${SURROGATE_KEY},${FACT_EX_KEY} FROM ${EAL_DELTA_TAB} d join ${EAL_DELTA_TAB_KEYED} k  ON concat('SAVEON~', d.${NATURAL_KEY_LIST})  = k.ALT_KEY_TXT; truncate table ${EAL_DELTA_TAB};insert into ${EAL_DELTA_TAB} select * from ${EAL_DELTA_TAB}_bak;"
#hive -e "SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;use ${HIVE_DB};MERGE INTO ${EAL_DELTA_TAB} USING ( SELECT DISTINCT * FROM ${EAL_DELTA_TAB_KEYED} WHERE ${SURROGATE_KEY} > 0 ) B ON (('${CLIENT_UPR}~'|| UPPER(TRIM(${NATURAL_KEY_LIST}))) = UPPER(TRIM(B.ALT_KEY_TXT)) ) WHEN MATCHED THEN UPDATE SET ${SURROGATE_KEY} = B.${SURROGATE_KEY};" 

hive -e "use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; drop table if exists ${EAL_DELTA_TAB}_bak;create table  ${EAL_DELTA_TAB}_bak as select distinct k.${SURROGATE_KEY},${FACT_EX_KEY} FROM ${EAL_DELTA_TAB} d join ${EAL_DELTA_TAB_KEYED} k  ON concat('SAVEON~', d.${NATURAL_KEY_LIST})  = k.ALT_KEY_TXT; truncate table ${EAL_DELTA_TAB};insert into ${EAL_DELTA_TAB} select * from ${EAL_DELTA_TAB}_bak;"

echo
echo "**********************************************************************************"
echo
echo "CREATE TABLE ${EAL_MSTR_TAB}_BKP if not present in our schema;"

echo "SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;use ${HIVE_DB};CREATE TABLE IF NOT EXISTS ${HIVE_DB}.${EAL_MSTR_TAB}_BKP AS SELECT * FROM ${HIVE_DB}.${EAL_MSTR_TAB} WHERE 1=2;INSERT INTO ${HIVE_DB}.${EAL_MSTR_TAB}_BKP SELECT * FROM ${HIVE_DB}.${EAL_MSTR_TAB} WHERE  ${SURROGATE_KEY} NOT IN ( SELECT distinct ${SURROGATE_KEY} FROM ${EAL_MSTR_TAB}_BKP )"

hive -e "SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;use ${HIVE_DB};CREATE TABLE IF NOT EXISTS ${HIVE_DB}.${EAL_MSTR_TAB}_BKP AS SELECT * FROM ${HIVE_DB}.${EAL_MSTR_TAB} WHERE 1=2;INSERT INTO ${HIVE_DB}.${EAL_MSTR_TAB}_BKP SELECT * FROM ${HIVE_DB}.${EAL_MSTR_TAB} WHERE  ${SURROGATE_KEY} NOT IN ( SELECT distinct ${SURROGATE_KEY} FROM ${EAL_MSTR_TAB}_BKP );                     "


echo 
echo "Inserting data to EAL Master table"
echo

echo "use ${HIVE_DB}; SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;DELETE FROM ${HIVE_DB}.${EAL_MSTR_TAB} WHERE ${SURROGATE_KEY} IN ( SELECT distinct ${SURROGATE_KEY} FROM ${EAL_DELTA_TAB} );INSERT INTO ${EAL_MSTR_TAB}  SELECT ${SURROGATE_KEY},${FACT_EX_KEY} FROM ${EAL_DELTA_TAB} WHERE ${SURROGATE_KEY} > 0 and ${SURROGATE_KEY} NOT IN ( SELECT distinct ${SURROGATE_KEY} FROM ${EAL_MSTR_TAB});"

hive -e "use ${HIVE_DB}; SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;DELETE FROM ${HIVE_DB}.${EAL_MSTR_TAB} WHERE ${SURROGATE_KEY} IN ( SELECT distinct ${SURROGATE_KEY} FROM ${EAL_DELTA_TAB} );INSERT INTO ${EAL_MSTR_TAB}  SELECT ${SURROGATE_KEY},${FACT_EX_KEY} FROM ${EAL_DELTA_TAB} WHERE ${SURROGATE_KEY} > 0 and ${SURROGATE_KEY} NOT IN ( SELECT distinct ${SURROGATE_KEY} vn FROM ${EAL_MSTR_TAB});"

echo "Post Delta to Master processing begun"
echo "**********************************************************************************"
echo 


echo "Executing  ${SCRIPTDIR}/SAVEON_${DIM_NAME_UPR}.hql "
hive -f ${SCRIPTDIR}/SAVEON_${DIM_NAME_UPR}.hql


echo "Post Delta to Master processing completed"
echo "**********************************************************************************"
echo

#######################################################################

echo
echo "Script execution completed @ `date`"



