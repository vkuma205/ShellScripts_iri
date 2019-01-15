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
CLIENT_UPR=`echo ${CLIENT} | awk '{print toupper($0)}'`

echo
echo "Start Date : $JOB_STRT_TS"
echo "Container id : $CONTAINER_ID"
echo "Client Name: $CLIENT_UPR"
echo
echo

#echo "Moving all .log files from $SRC_DIR to $SRC_DIR/logs"
#echo
#`mv -f $SRC_DIR/*.log $SRC_DIR/logs`
#echo "moving the logs is completed"
#echo

META_INFO=`sqlplus -s ${OPSBLD_USER}/${OPSBLD_PASSWD}@${OPSBLD_DB}<<ENDSQL
SET HEADING OFF ECHO OFF FEEDBACK OFF PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 500;
WHENEVER SQLERROR EXIT 1
SELECT CLIENT || '|' || DIM_NAME || '|' || DIM_ABBR || '|' || FILE_NAME_PREFIX FROM ${KEY_DEFN_TAB} WHERE CONTAINER_ID=${CONTAINER_ID};
ENDSQL`

testdberror $?

FILE_PREFIX=${FILE_NAME_PREFIX}
if [ -z "${META_INFO}" ]
then
	echo
	echo " Error in fetching META_INFO from table ${KEY_DEFN_TAB} on ${OPSBLD_USER}@${OPSBLD_DB} for container id ${CONTAINER_ID}"
	exit -1;
fi

echo
echo "Meta table info:  $META_INFO"
echo

FILE_NAME_PREFIX=`echo ${META_INFO} | cut -d'|' -f4`

DIM_NAME=`echo ${META_INFO} | cut -d'|' -f2`
DIM_ABBR=`echo ${META_INFO} | cut -d'|' -f3`

DIM_NAME_LWR=`echo ${DIM_NAME} | awk '{print tolower($0)}'`
DIM_NAME_UPR=`echo ${DIM_NAME} | awk '{print toupper($0)}'`
DIM_ABBR_LWR=`echo ${DIM_ABBR} | awk '{print tolower($0)}'`
DIM_ABBR_UPR=`echo ${DIM_ABBR} | awk '{print toupper($0)}'`

FILE_SRC_DIR=${SRC_DIR}
FILE_LOG_DIR=${FILE_LOG_DIR}
FILEEXTENSION='txt'
#FILE_NAME=${SRC_DIR}/${FILE_NAME_PREFIX}*.txt
FILE_NAME=${SRC_DIR}/${FILE_NAME_PREFIX}.txt
BASE_FILE_NAME=`basename ${FILE_NAME}`

#FILE_NAME_WITHOUT_PATH=${FILE_NAME_PREFIX}_${FILE_DATE}.t

echo "FILE_NAME_WITHOUT_PATH: ${FILE_NAME_PREFIX}"

FILE_NAME=`find ${SRC_DIR}/${FILE_NAME_PREFIX}* -print | sort -r | head -n1`
FILE_NAME_WITHOUT_PATH=`echo ${FILE_NAME} | awk -F "/" '{print $NF}'`
FILE_DATE=`echo ${FILE_NAME_WITHOUT_PATH} | awk -F "." '{print $2}'`
BASE_FILE_NAME=`basename ${FILE_NAME}`
FILE_NAME_WITHOUT_PATH=$BASE_FILE_NAME
echo
echo "FILE_NAME: $FILE_NAME"
echo "BASE_FILE_NAME: $BASE_FILE_NAME" 
echo "FILE_NAME_WITHOUT_PATH: $FILE_NAME_WITHOUT_PATH"
echo "FILE_SRC_DIR: $FILE_SRC_DIR"
echo "FILE_LOG_DIR: $FILE_LOG_DIR"
echo "FILE_DATE:" $FILE_DATE
echo


FILE_DATE_WH=${FILE_DATE}
echo
echo "FILE_DATE_WH : ${FILE_DATE_WH}"
echo

RAW_TAB=${CLIENT_UPR}_${DIM_ABBR_UPR}_EXT
STG_TAB=${CLIENT_UPR}_${DIM_ABBR_UPR}_STG
echo
echo "RAW_TAB: $RAW_TAB"
echo "STG_TAB: $STG_TAB"
echo

echo
echo "Container id : $CONTAINER_ID"
echo "FILE_NAME: $FILE_NAME"
echo "BASE_FILE_NAME: $BASE_FILE_NAME"
echo "FILE_NAME_WITHOUT_PATH: $FILE_NAME_WITHOUT_PATH"
echo

TAB_DEFN_TEMP=`sqlplus -s ${OPSBLD_USER}/${OPSBLD_PASSWD}@${OPSBLD_DB}<<ENDSQL
set heading off echo off verify off feed off termout off pages 0 linesize 10000  long 3000 trims on trim on
SELECT REPLACE(COLUMN_META,' ','@') FROM ${KEY_DEFN_TAB} WHERE CONTAINER_ID=${CONTAINER_ID};
ENDSQL`

testdberror $?

TAB_DEFN_TEMP=`echo ${TAB_DEFN_TEMP} | sed 's/ //g'`
TAB_DEFN=`echo ${TAB_DEFN_TEMP} | sed 's/@/ /g'`

echo
echo "TAB_DEFN : ${TAB_DEFN}"
echo "*********************************************************************************"
echo "TAB_DEFN_TEMP : ${TAB_DEFN_TEMP}"
echo "**********************************************************************************"
echo

EXT_TAB_EXISTS=`sqlplus -s ${WH_POSTX_USER}/${WH_POSTX_PASSWORD}@${WH_POSTX_DATABASE} <<ENDSQL
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;
SET SERVEROUTPUT OFF HEADING OFF ECHO OFF FEEDBACK OFF PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 500;

SELECT COUNT(1)
FROM USER_TABLES
WHERE TABLE_NAME='${RAW_TAB}';
ENDSQL`

testdberror $?

if [ ${EXT_TAB_EXISTS} -eq 1 ]
then

echo
echo " ${RAW_TAB} Table  exists ..!! So dropping the table and creating"
echo
echo "DROP and CREATE TABLE ${RAW_TAB};"
echo
echo "
CREATE OR REPLACE DIRECTORY ${CLIENT_LWR}_SRC as '${FILE_SRC_DIR}';
"
sqlplus -s ${WH_POSTX_USER}/${WH_POSTX_PASSWORD}@${WH_POSTX_DATABASE} <<ENDSQL
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET SERVEROUTPUT ON;
SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 110;

DROP TABLE ${RAW_TAB};

CREATE OR REPLACE DIRECTORY ${CLIENT_LWR}_SRC as '${FILE_SRC_DIR}';
CREATE OR REPLACE DIRECTORY ${CLIENT_LWR}_LOG as '${FILE_LOG_DIR}';

CREATE TABLE ${RAW_TAB}
(
	${TAB_DEFN}
)
	ORGANIZATION EXTERNAL
	(
		TYPE ORACLE_LOADER
		DEFAULT DIRECTORY ${CLIENT_LWR}_SRC
		ACCESS PARAMETERS
		(
			RECORDS DELIMITED BY NEWLINE
     			FIELDS TERMINATED BY '|'
		)
		LOCATION
		(
			'${FILE_NAME_WITHOUT_PATH}'
		)
	)
REJECT LIMIT 1;
ENDSQL
testdberror $?
else
# Creating external table if it doesn't exist
echo
echo "Creating External Table"
echo
sqlplus -s ${WH_POSTX_USER}/${WH_POSTX_PASSWORD}@${WH_POSTX_DATABASE}<<ENDSQL
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET SERVEROUTPUT ON;
SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 110;

CREATE OR REPLACE DIRECTORY ${CLIENT_LWR}_SRC as '${FILE_SRC_DIR}';
CREATE OR REPLACE DIRECTORY ${CLIENT_LWR}_LOG as '${FILE_LOG_DIR}';

CREATE TABLE ${RAW_TAB}
(
	${TAB_DEFN}
)
	ORGANIZATION EXTERNAL
	(
		TYPE ORACLE_LOADER
		DEFAULT DIRECTORY ${CLIENT_LWR}_LOG
		ACCESS PARAMETERS
		(
			RECORDS DELIMITED BY NEWLINE
                        BADFILE '$FILE_LOG_DIR/${BASE_FILE_NAME}.bad'
                        LOGFILE '$FILE_LOG_DIR/${BASE_FILE_NAME}.log'
			FIELDS TERMINATED BY '|'
		)
		LOCATION
		(
			'${FILE_NAME_WITHOUT_PATH}'
		)
	)
REJECT LIMIT 1;
ENDSQL

testdberror $?

fi

echo "CREATE TABLE ${RAW_TAB}
(
         ${TAB_DEFN}
)
         ORGANIZATION EXTERNAL
         (
                TYPE ORACLE_LOADER
                DEFAULT DIRECTORY ${CLIENT_LWR}_LOG
                ACCESS PARAMETERS
                (
                        RECORDS DELIMITED BY NEWLINE
                        BADFILE ${CLIENT_LWR}_LOG :'${BASE_FILE_NAME}.bad'
                        LOGFILE ${CLIENT_LWR}_LOG :'${BASE_FILE_NAME}.log'
                        FIELDS TERMINATED BY '|'
                )
                LOCATION
                (
                        '${FILE_NAME_WITHOUT_PATH}'
                )
           )
REJECT LIMIT 1;
"

echo
echo "External Table has created"
echo

echo "**********************************************************************************"
echo

echo
echo "DROP and CREATE TABLE ${STG_TAB} if exists  else creating ${STG_TAB} table;"
echo

# Checks for STG tablel exists or not if it's not exists then  creates STG table

STG_TAB_EXISTS=`sqlplus -s ${WH_POSTX_USER}/${WH_POSTX_PASSWORD}@${WH_POSTX_DATABASE} <<ENDSQL
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;
SET SERVEROUTPUT OFF HEADING OFF ECHO OFF FEEDBACK OFF PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 500;
SELECT COUNT(1)
FROM USER_TABLES
WHERE TABLE_NAME='${STG_TAB}';
ENDSQL`

testdberror $?



if [ ${STG_TAB_EXISTS} -eq 1 ]
then

echo
echo " ${STG_TAB} Table  exists ..!! So dropping the table and creating" 
echo "DROP and CREATE TABLE ${STG_TAB};"
echo

sqlplus -s ${WH_POSTX_USER}/${WH_POSTX_PASSWORD}@${WH_POSTX_DATABASE} <<ENDSQL
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET SERVEROUTPUT OFF;
SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 110;

DROP TABLE ${STG_TAB};

CREATE TABLE ${STG_TAB} AS
SELECT A.*,TO_CHAR(TO_DATE('${FILE_DATE}','YYYYMMDD'),'YYYYMMDD') FILE_DATE
FROM ${RAW_TAB} A;
ENDSQL

testdberror $?

else

echo
echo "Table $STG_TAB does not exit and Creating it now"
echo

echo "CREATE TABLE ${STG_TAB} AS
SELECT A.*,TO_CHAR(TO_DATE('${FILE_DATE}','YYYYMMDD'),'YYYYMMDD') FILE_DATE
FROM ${RAW_TAB} A;
"

sqlplus -s ${WH_POSTX_USER}/${WH_POSTX_PASSWORD}@${WH_POSTX_DATABASE} <<ENDSQL
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET SERVEROUTPUT ON;
SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 110;

CREATE TABLE ${STG_TAB} AS
SELECT A.*,TO_CHAR(TO_DATE('${FILE_DATE}','YYYYMMDD'),'YYYYMMDD') FILE_DATE
FROM ${RAW_TAB} A;

ENDSQL
testdberror $?

fi


echo "Staging table has created"



echo "backing up ${FILE_NAME_PREFIX} files to ${BACKUP_DIR} folder"

mv ${SRC_DIR}/${FILE_NAME_PREFIX}* ${BACKUP_DIR}
echo

echo
echo "****************Moving all .log files from $SRC_DIR to $SRC_DIR/logs*************************"
echo
`mv -f $SRC_DIR/*.log $SRC_DIR/logs`
echo
echo "***************Logs have been moved to: $SRC_DIR/logs************************************"
echo


echo
echo "**********************************************************************************"
echo

echo
echo "Script execution completed @ `date`"

