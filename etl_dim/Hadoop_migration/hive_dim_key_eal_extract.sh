#!/bin/bash

########################################################################################################################
#
# Purpose: Extract EAL MSTR to MDM
#
# Change History Record:
#
# ======================================================================================================================
# DATE      |   AUTHOR NAME        |    CHANGE DESCRIPTION | JIRA ID
# ======================================================================================================================
# April 6 2018 |  Vinod Gardas           | New script created |
# ----------------------------------------------------------------------------------------------------------------------
########################################################################################################################

###RAW_TAB
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
EMAIL=${EMAIL}

#META_INFO=`sqlplus -s ${OPSBLD_USER}/${OPSBLD_PASSWD}@${OPSBLD_DB}<<EOF
#SET HEADING OFF ECHO OFF FEEDBACK OFF PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 500;
#WHENEVER SQLERROR EXIT 1
#SELECT CLIENT || '|' || DIM_NAME || '|' || DIM_ABBR || '|' || FILE_NAME_PREFIX || '|' || SURROGATE_KEY || '|' || EAL_ATTR_LOAD_TYPE || '|' || EAL_ATTR_SPEC_NAME FROM ${KEY_DEFN_TAB} WHERE CONTAINER_ID=${CONTAINER_ID};
#EOF`

#testdberror $?

META_INFO=`hive -e "use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;select concat(client,'|',dim_name,'|',dim_abbr,'|',file_name_prefix,'|',surrogate_key,'|',eal_attr_load_type,'|',eal_attr_spec_name) FROM ${HIVE_DB}.${KEY_DEFN_TAB} WHERE CONTAINER_ID=${CONTAINER_ID};"`




if [ -z "${META_INFO}" ]
then
        echo
        echo " Error in fetching META_INFO from table ${KEY_DEFN_TAB} on ${HIVE_DB} for container id ${CONTAINER_ID}"
        exit -1;
fi

FILE_NAME_PREFIX=`echo ${META_INFO} | cut -d'|' -f4`
DIM_NAME=`echo ${META_INFO} | cut -d'|' -f2`
DIM_ABBR=`echo ${META_INFO} | cut -d'|' -f3`
SURROGATE_KEY=`echo ${META_INFO} | cut -d'|' -f5`
EAL_ATTR_LOAD_TYPE=`echo ${META_INFO} | cut -d'|' -f6`
EAL_ATTR_SPEC_NAME=`echo ${META_INFO} | cut -d'|' -f7`

CLIENT_LWR=`echo ${CLIENT} | awk '{print tolower($0)}'`
CLIENT_UPR=`echo ${CLIENT} | awk '{print toupper($0)}'`

DIM_NAME_LWR=`echo ${DIM_NAME} | awk '{print tolower($0)}'`
DIM_NAME_UPR=`echo ${DIM_NAME} | awk '{print toupper($0)}'`
DIM_ABBR_LWR=`echo ${DIM_ABBR} | awk '{print tolower($0)}'`
DIM_ABBR_UPR=`echo ${DIM_ABBR} | awk '{print toupper($0)}'`


EAL_ATTR_LOAD_TYPE_UPR=`echo ${EAL_ATTR_LOAD_TYPE} | awk '{print toupper($0)}'`
EAL_ATTR_SPEC_NAME_UPR=`echo ${EAL_ATTR_SPEC_NAME} | awk '{print toupper($0)}'`

#STG_TAB=${CLIENT_UPR}_${DIM_ABBR_UPR}_STG_${FILE_DATE}
STG_TAB=${CLIENT_UPR}_${DIM_ABBR_UPR}_STG
RAW_TAB=${CLIENT_UPR}_${DIM_ABBR_UPR}_EXT
#RAW_TAB=${CLIENT_UPR}_${DIM_ABBR_UPR}_EXT_${FILE_DATE}
FORMAT_FILE_DATE=${FILE_DATE}

EAL_STG_TAB=EAL_DK_${CLIENT_UPR}_${DIM_ABBR}_STG
#EAL_MSTR_TAB=EAL_DK_${CLIENT_UPR}_${DIM_ABBR}_MSTR_MDM
EAL_MSTR_TAB=EAL_DK_${CLIENT_UPR}_${DIM_ABBR}_MSTR
EAL_DELTA_TAB=EAL_DK_${CLIENT_UPR}_${DIM_ABBR}_DELTA

EAL_DELTA_TAB_KEYED=EAL_${CLIENT_UPR}_${DIM_ABBR}_DLT_KYD

echo
echo "**********************************************************************************"
echo 


echo "FILE_NAME_PREFIX:  ${FILE_NAME_PREFIX}"
echo "DIM_NAME:  ${DIM_NAME}"
echo "DIM_ABBR:  ${DIM_ABBR}"
echo "SURROGATE_KEY:  ${SURROGATE_KEY}"
echo "EAL_ATTR_LOAD_TYPE:  ${EAL_ATTR_LOAD_TYPE}"
echo "EAL_ATTR_SPEC_NAME:  ${EAL_ATTR_SPEC_NAME}"
echo "CLIENT_LWR:  ${CLIENT_LWR}"
echo "CLIENT_UPR:  ${CLIENT_UPR}"
echo "DIM_NAME_LWR:  ${DIM_NAME_LWR}"
echo "DIM_NAME_UPR:  ${DIM_NAME_UPR}"
echo "DIM_ABBR_LWR:  ${DIM_ABBR_LWR}"
echo "DIM_ABBR_UPR:  ${DIM_ABBR_UPR}"
echo "EAL_ATTR_LOAD_TYPE_UPR:  ${EAL_ATTR_LOAD_TYPE_UPR}"
echo "EAL_ATTR_SPEC_NAME_UPR:  ${EAL_ATTR_SPEC_NAME_UPR}"
echo "STG_TAB:  ${STG_TAB}"
echo "RAW_TAB:  ${RAW_TAB}"
echo "EAL_STG_TAB:  ${EAL_STG_TAB}"
echo "EAL_MSTR_TAB:  ${EAL_MSTR_TAB}"
echo "EAL_DELTA_TAB:  ${EAL_DELTA_TAB}"
echo "EAL_DELTA_TAB_KEYED :  ${EAL_DELTA_TAB_KEYED}"





echo
echo "**********************************************************************************"
echo

#echo "SELECT LISTAGG(DECODE(COLUMN_NAME,'','','A.'||COLUMN_NAME),'||''|''||') WITHIN GROUP(ORDER BY COLUMN_ID) FROM USER_TAB_COLS WHERE UPPER(TABLE_NAME)=UPPER('${EAL_MSTR_TAB}');"

#FACT_EXTRACT_TAB_PREFIX=`sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<EOF
#set echo off verify off feed off termout off pages 0 lines 10000
#SELECT LISTAGG(DECODE(COLUMN_NAME,'','','A.'||COLUMN_NAME),'||''|''||') WITHIN GROUP(ORDER BY COLUMN_ID) FROM USER_TAB_COLS WHERE UPPER(TABLE_NAME)=UPPER('${EAL_MSTR_TAB}') and COLUMN_NAME not in ('FILE_DATE');
#EOF`

FACT_EXTRACT_TAB_PREFIX=`hive -e "use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;SHOW COLUMNS IN ${EAL_MSTR_TAB};" |sed -e 's/^/NVL(A./g' -e ':a' -e "s/$/,'')/g" -e 'N' -e '$!ba' -e "s/\n/,'|',NVL(A./g" -e "s/$/,'')/g" -e 's/ //g'`

echo "failed"

if [ -z "${FACT_EXTRACT_TAB_PREFIX}" ]
then
        echo
        echo "FACT_SELECT_TAB_PREFIX not fetched properly"
        exit -1;
fi


echo
echo "FACT_EXTRACT_TAB_PREFIX : ${FACT_EXTRACT_TAB_PREFIX}"
echo

echo
echo "**********************************************************************************"
echo

echo "Export the ${CLIENT_UPR} - ${DIM_ABBR} - ${EAL_ATTR_LOAD_TYPE} using EAL SPEC - ${EAL_ATTR_SPEC_NAME}"
echo

echo
echo "**********************************************************************************"
echo

echo
if [ "${EAL_ATTR_LOAD_TYPE_UPR}" == 'FULL' ]
then

#echo "SELECT distinct ${FACT_EXTRACT_TAB_PREFIX} FROM ${EAL_MSTR_TAB} A;" > ${GENDIR}/${EAL_ATTR_SPEC_NAME_UPR}_extract_${FORMAT_FILE_DATE}.sql


echo "use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;SELECT distinct ${FACT_EXTRACT_TAB_PREFIX} FROM ${EAL_MSTR_TAB} A ;" > ${GENDIR}/${EAL_ATTR_SPEC_NAME_UPR}_extract_${FORMAT_FILE_DATE}.hql

elif [ "${EAL_ATTR_LOAD_TYPE_UPR}" == 'DELTA' ]
then
#echo "SELECT distinct ${FACT_EXTRACT_TAB_PREFIX} FROM ${EAL_DELTA_TAB} A;" > ${GENDIR}/${EAL_ATTR_SPEC_NAME_UPR}_extract_${FORMAT_FILE_DATE}.sql

echo "use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;SELECT distinct ${FACT_EXTRACT_TAB_PREFIX} FROM ${EAL_MSTR_TAB} A ;" > ${GENDIR}/${EAL_ATTR_SPEC_NAME_UPR}_extract_${FORMAT_FILE_DATE}.hql

else
echo " EAL_ATTR_LOAD_TYPE is incorrect. Valid values are FULL and DELTA".
echo
exit 1

fi


echo "Starting EAL extract"
echo

#rm -f "/lddata/dim/${DATAENV}/eal/${EAL_ATTR_SPEC_NAME_UPR}_${FORMAT_FILE_DATE}.dat"

#echo "${ETL_UTIL_PATH}/ora_util/ora_spool_export.sh ${WH_POSTX_DATABASE} ${WH_POSTX_SCHEMA} ${WH_POSTX_PASSWORD} ${SCRIPTENV} ${GENDIR}${EAL_ATTR_SPEC_NAME_UPR}_extract_${FORMAT_FILE_DATE}.sql /lddata/dim/${DATAENV}/eal/ ${EAL_ATTR_SPEC_NAME_UPR}_${FORMAT_FILE_DATE}.dat"


#sh ${ETL_UTIL_PATH}/ora_util/ora_spool_export.sh ${WH_POSTX_DATABASE} ${WH_POSTX_USER} ${WH_POSTX_PASSWORD} ${SCRIPTENV} ${GENDIR}/${EAL_ATTR_SPEC_NAME_UPR}_extract_${FORMAT_FILE_DATE}.sql /lddata/dim/${DATAENV}/eal/ ${EAL_ATTR_SPEC_NAME_UPR}_${FORMAT_FILE_DATE}.dat


hive -f ${GENDIR}/${EAL_ATTR_SPEC_NAME_UPR}_extract_${FILE_DATE}.hql > /lddata/dim/${DATAENV}/eal/${EAL_ATTR_SPEC_NAME_UPR}_${FILE_DATE}.dat;

testdberror $?

sed -i 's/\t//g' /lddata/dim/${DATAENV}/eal/${EAL_ATTR_SPEC_NAME_UPR}_${FILE_DATE}.dat

testdberror $?

#echo "${ETL_UTIL_PATH}/ora_util/ora_spool_export.sh ${WH_POSTX_DATABASE} ${WH_POSTX_SCHEMA} ${WH_POSTX_PASSWORD} ${SCRIPTENV} ${GENDIR}/${EAL_ATTR_SPEC_NAME_UPR}_extract_${FORMAT_FILE_DATE}.sql /lddata/dim/${DATAENV}/eal/ ${EAL_ATTR_SPEC_NAME_UPR}_${FORMAT_FILE_DATE}.dat"


rm -f "/lddata/dim/${DATAENV}/eal/${EAL_ATTR_SPEC_NAME_UPR}.ctrl"

echo "${EAL_ATTR_SPEC_NAME_UPR}
${EAL_ATTR_SPEC_NAME_UPR}_${FORMAT_FILE_DATE}.dat
${EMAIL}" > /lddata/dim/${DATAENV}/eal/${EAL_ATTR_SPEC_NAME_UPR}.ctrl


if [ ! -s /lddata/dim/${DATAENV}/eal/${EAL_ATTR_SPEC_NAME_UPR}.ctrl ]
then
echo "File size is zero - /lddata/dim/${DATAENV}/eal/${EAL_ATTR_SPEC_NAME_UPR}.ctrl"
exit 99
fi

#touch /lddata/dim/${DATAENV}/eal/${EAL_ATTR_SPEC_NAME_UPR}.start

echo
echo "EAL File extract completed"
#### "*************Remove Datewise Staging and Raw tables*********************************************************************"
echo
echo "Drop Raw and Stage Tables"


#STG_TAB_PRESENT=`sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
#set echo off verify off feed off termout off pages 0
#SELECT COUNT(*) AS tablepresent FROM user_tables WHERE UPPER(table_name)=UPPER('${STG_TAB}');
#ENDSQL`

#STG_TAB_PRESENT= hive -S -e "USE ${HIVE_DB}; show tables like '${STG_TAB}'"| wc -l;

#if [ ${STG_TAB_PRESENT} -eq 1 ]
#then
#echo "drop TABLE ${STG_TAB}"> ${GENDIR}/eal_stage_${JOB_STRT_TS}.sql

#sqlplus $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
#WHENEVER SQLERROR EXIT 1
#SET ECHO ON
#@${GENDIR}/eal_stage_${JOB_STRT_TS}.sql
#ENDSQL
#else
#rm -f ${GENDIR}/eal_stage_${JOB_STRT_TS}.sql

#echo
#echo "${STG_TAB} is not present"
#echo

#fi

#EXT_TAB_PRESENT=`sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
#set echo off verify off feed off termout off pages 0
#SELECT COUNT(*) AS tablepresent FROM user_tables WHERE UPPER(table_name)=UPPER('${RAW_TAB}');
#ENDSQL`

#EXT_TAB_PRESENT= hive -S -e "USE ${HIVE_DB}; show tables like '${RAW_TAB}'" | wc -l;

#if [ ${EXT_TAB_PRESENT} -eq 1 ]
#then
#echo "drop TABLE ${RAW_TAB};
#" > ${GENDIR}/ext_tab_${JOB_STRT_TS}.sql

#sqlplus $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
#WHENEVER SQLERROR EXIT 1
#SET ECHO ON
#@${GENDIR}/ext_tab_${JOB_STRT_TS}.sql
#ENDSQL

#rm -f ${GENDIR}/ext_tab_${JOB_STRT_TS}.sql
#else
#echo
#echo "${RAW_TAB} is not present"
#echo

#fi


echo
echo "Script execution completed @ `date`"

