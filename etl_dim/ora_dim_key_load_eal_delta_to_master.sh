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

META_INFO=`sqlplus -s ${OPSBLD_USER}/${OPSBLD_PASSWD}@${OPSBLD_DB}<<EOF
SET HEADING OFF ECHO OFF FEEDBACK OFF PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 500;
WHENEVER SQLERROR EXIT 1
SELECT CLIENT || '|' || DIM_NAME || '|' || DIM_ABBR || '|' || FILE_NAME_PREFIX || '|' || SURROGATE_KEY FROM ${KEY_DEFN_TAB} WHERE CONTAINER_ID=${CONTAINER_ID};
EOF`

testdberror $?

if [ -z "${META_INFO}" ]
then
        echo
        echo " Error in fetching META_INFO from table ${KEY_DEFN_TAB} on ${OPSBLD_USER}@${OPSBLD_DB} for container id ${CONTAINER_ID}"
        exit -1;
fi

FILE_NAME_PREFIX=`echo ${META_INFO} | cut -d'|' -f4`
DIM_NAME=`echo ${META_INFO} | cut -d'|' -f2`
DIM_ABBR=`echo ${META_INFO} | cut -d'|' -f3`
SURROGATE_KEY=`echo ${META_INFO} | cut -d'|' -f5`

CLIENT_LWR=`echo ${CLIENT} | awk '{print tolower($0)}'`
CLIENT_UPR=`echo ${CLIENT} | awk '{print toupper($0)}'`

DIM_NAME_LWR=`echo ${DIM_NAME} | awk '{print tolower($0)}'`
DIM_NAME_UPR=`echo ${DIM_NAME} | awk '{print toupper($0)}'`
DIM_ABBR_LWR=`echo ${DIM_ABBR} | awk '{print tolower($0)}'`
DIM_ABBR_UPR=`echo ${DIM_ABBR} | awk '{print toupper($0)}'`

RAW_TAB=${CLIENT_UPR}_${DIM_ABBR_UPR}_EXT_${FILE_DATE}
#RAW_TAB=${CLIENT_UPR}_${DIM_ABBR_UPR}_EXT
#STG_TAB=${CLIENT_UPR}_${DIM_ABBR_UPR}_STG
STG_TAB=${CLIENT_UPR}_${DIM_ABBR_UPR}_STG_${FILE_DATE}

EAL_STG_TAB=EAL_DK_${CLIENT_UPR}_${DIM_ABBR_UPR}_STG
EAL_MSTR_TAB=EAL_DK_${CLIENT_UPR}_${DIM_ABBR_UPR}_MSTR
EAL_DELTA_TAB=EAL_DK_${CLIENT_UPR}_${DIM_ABBR_UPR}_DELTA
#EAL_MSTR_TAB_EXT=EAL_DK_${CLIENT_UPR}_${DIM_ABBR_UPR}_MSTR_MDM
#EAL_DELTA_TAB_EXT=EAL_DK_${CLIENT_UPR}_${DIM_ABBR_UPR}_DELTA_MDM

EAL_DELTA_TAB_KEYED=EAL_${CLIENT_UPR}_${DIM_ABBR_UPR}_DLT_KYD

echo
echo "**********************************************************************************"
echo


FACT_SELECT=`sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<EOF
set echo off verify off feed off termout off pages 0 lines 10000
SELECT LISTAGG(COLUMN_NAME,',') WITHIN GROUP(ORDER BY COLUMN_ID) FROM USER_TAB_COLS WHERE UPPER(TABLE_NAME)=UPPER('${EAL_MSTR_TAB}');
EOF`

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

NATURAL_KEY_LIST=`sqlplus -s ${OPSBLD_USER}/${OPSBLD_PASSWD}@${OPSBLD_DB}<<EOF
set echo off verify off feed off termout off pages 0 lines 500
SELECT REPLACE(NATURAL_KEY, '|',' || ''~'' || ') FROM ${KEY_DEFN_TAB} WHERE CONTAINER_ID=${CONTAINER_ID};
EOF`

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
echo "MERGE INTO ${EAL_DELTA_TAB} USING ( SELECT DISTINCT * FROM ${EAL_DELTA_TAB_KEYED} WHERE ${SURROGATE_KEY} > 0 ) B ON (('${CLIENT_UPR}~'|| UPPER(TRIM(${NATURAL_KEY_LIST}))) = UPPER(TRIM(B.ALT_KEY_TXT)) ) WHEN MATCHED THEN UPDATE SET ${SURROGATE_KEY} = B.${SURROGATE_KEY};"

sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<EOF
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET SERVEROUTPUT ON;
SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0  VERIFY OFF LINESIZE 110

MERGE INTO ${EAL_DELTA_TAB} USING ( SELECT distinct * FROM ${EAL_DELTA_TAB_KEYED} WHERE ${SURROGATE_KEY} > 0 ) B ON (( '${CLIENT_UPR}~'||UPPER(TRIM(${NATURAL_KEY_LIST}))) = UPPER(TRIM(B.ALT_KEY_TXT)) ) WHEN MATCHED THEN UPDATE SET ${SURROGATE_KEY} = B.${SURROGATE_KEY};

COMMIT;

EOF

testdberror $?

echo
echo "**********************************************************************************"
echo

echo "==============================================================================================================="

# create a backuptable if it's not exist

echo
echo "**********************************************************************************"
echo
echo "CREATE TABLE ${EAL_MSTR_TAB}_BKP if not present in our schema;"


EAL_MSTR_TAB_BKP_Present=`sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
set echo off verify off feed off termout off pages 0
SELECT COUNT(*) AS tablepresent FROM user_tables WHERE UPPER(table_name)=UPPER('${EAL_MSTR_TAB}_BKP');
ENDSQL`
testdberror $?
if [ ${EAL_MSTR_TAB_BKP_Present} -eq 0 ]
then
echo " "
                echo "${EAL_MSTR_TAB}_BKP table is not present in schema $WH_POSTX_USER@$WH_POSTX_DATABASE"
                echo 
                echo "Creating table ${EAL_MSTR_TAB}_BKP"
                echo 
echo "***********************************${SURROGATE_KEY}***********************************************" 
sqlplus $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
                
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET SERVEROUTPUT ON;
SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0  VERIFY OFF LINESIZE 110
CREATE TABLE ${EAL_MSTR_TAB}_BKP AS SELECT * FROM ${EAL_MSTR_TAB}  WHERE 1=2;
INSERT INTO ${EAL_MSTR_TAB}_BKP  SELECT ${FACT_SELECT} FROM ${EAL_MSTR_TAB} WHERE TO_NUMBER(${SURROGATE_KEY}) > 0 and ${SURROGATE_KEY} NOT IN ( SELECT distinct ${SURROGATE_KEY} FROM ${EAL_MSTR_TAB}_BKP );
ENDSQL

        testdberror $?

####        rm -f  ${GENDIR}/ora_dim_key_load_eal_stage_${JOB_STRT_TS}.sql

else

echo
echo "INSERT INTO ${EAL_MSTR_TAB}_BKP ( ${FACT_SELECT} ) SELECT ${FACT_SELECT} FROM ${EAL_DELTA_TAB} WHERE TO_NUMBER(${SURROGATE_KEY}) > 0 and ${SURROGATE_KEY} NOT IN ( SELECT distinct ${SURROGATE_KEY} FROM ${EAL_MSTR_TAB}_BKP );"
echo

sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<EOF
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET SERVEROUTPUT ON;
SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0  VERIFY OFF LINESIZE 110

INSERT INTO ${EAL_MSTR_TAB}_BKP ( ${FACT_SELECT} ) SELECT ${FACT_SELECT} FROM ${EAL_DELTA_TAB} WHERE TO_NUMBER(${SURROGATE_KEY}) > 0 and ${SURROGATE_KEY} NOT IN ( SELECT distinct ${SURROGATE_KEY} FROM ${EAL_MSTR_TAB}_BKP );
COMMIT;

EOF

testdberror $?

fi


echo 
echo "Inserting data to EAL Master table"
echo

sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<EOF
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET SERVEROUTPUT ON;
SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0  VERIFY OFF LINESIZE 110

DELETE FROM ${EAL_MSTR_TAB} WHERE ${SURROGATE_KEY} IN ( SELECT distinct ${SURROGATE_KEY} FROM ${EAL_DELTA_TAB} );
COMMIT;

INSERT INTO ${EAL_MSTR_TAB}  SELECT ${FACT_SELECT} FROM ${EAL_DELTA_TAB} WHERE TO_NUMBER(${SURROGATE_KEY}) > 0
and ${SURROGATE_KEY} NOT IN ( SELECT distinct ${SURROGATE_KEY} FROM ${EAL_MSTR_TAB} );


COMMIT;

EOF

testdberror $?

echo "Post Delta to Master processing begun"
echo "**********************************************************************************"
echo 


echo "sqlplus $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
                WHENEVER SQLERROR EXIT 1
                SET ECHO ON
                @ ${SCRIPTDIR}/${DIM_NAME_UPR}.sql
ENDSQL

"

sqlplus $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
                WHENEVER SQLERROR EXIT 1
                SET ECHO ON

                ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
                ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
                ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

                @ ${SCRIPTDIR}/${DIM_NAME_UPR}.sql


ENDSQL

testdberror $?


echo "Post Delta to Master processing completed"
echo "**********************************************************************************"
echo

#######################################################################

echo
echo "Script execution completed @ `date`"



