#!/bin/bash

########################################################################################################################
#
# Purpose: To initialize all schema tables used in EAL fro Sobeys
#
# Change History Record:
#
# ======================================================================================================================
# DATE      |   AUTHOR NAME        |    CHANGE DESCRIPTION |JIRA_ID
# ======================================================================================================================
# 16 May  2017 |   Innocent Musanzikwa  |    New script created | ETLR-35
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
if [ $# -ne 1 ]
then
        echo
        echo "***************************************************************************"
        echo
        echo "ERROR MESSAGE:"
        echo "--------------"
        echo
        echo "ERR001 -- SYNTAX ERROR. CHECK THE PARAMETERS GIVEN TO SHELL."
        echo ""
        echo "Usage: sh $0 <config_file>"
        echo "***************************************************************************"
        echo ""
        exit 1
fi

echo
echo "Script $0 execution started @ `date`"
echo


CONFIG_FILE=${1}

. ${CONFIG_FILE}

#initialize error trap functions to check the availability of files and directory
. $ERRORTRAPS


echo "CREATE TABLE ${SAVEON_EAL_TABLE}_it_all_keys if not present in our schema;"


#SAVEON_IT_TABLE_ALL_KEYS_Present=`sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
#set echo off verify off feed off termout off pages 0
#SELECT COUNT(*) AS tablepresent FROM user_tables WHERE UPPER(table_name)=UPPER('${SAVEON_EAL_TABLE}_IT_ALL_KEYS');
#ENDSQL`
#testdberror $?

#SAVEON_IT_TABLE_ALL_KEYS_Present=`hive -e "SELECT COUNT(*) AS tablepresent FROM "`

#DROP TABLE ${HIVE_DB}.${SAVEON_EAL_TABLE}_it_all_keys;

hive -e "CREATE TABLE IF NOT EXISTS wh_postx_saveon_p1_backup.${SAVEON_EAL_TABLE}_it_all_keys AS SELECT UPC FROM ${HIVE_DB}.${SAVEON_EAL_TABLE}_it_stg;"

if [ $? -ne 0 ]
   then
      echo "Error while creating the table: ${SAVEON_EAL_TABLE} in Hive DB: ${HIVE_DB}. Aborting the program."
      echo
      exit 255
fi


#if [ ${SAVEON_IT_TABLE_ALL_KEYS_Present} -eq 0 ]
#then
#echo " "
#                echo "${SAVEON_EAL_TABLE}_IT_ALL_KEYS table is not present in schema $WH_POSTX_USER@$WH_POSTX_DATABASE"
#                echo
#                echo "Creating table ${SAVEON_EAL_TABLE}_IT_ALL_KEYS"
#                echo

#sqlplus $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL

#WHENEVER SQLERROR EXIT FAILURE;
#WHENEVER OSERROR EXIT FAILURE;

#ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
#ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
#ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

#SET SERVEROUTPUT ON;
#SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0  VERIFY OFF LINESIZE 110
#CREATE TABLE ${SAVEON_EAL_TABLE}_IT_ALL_KEYS AS SELECT UPC FROM ${SAVEON_EAL_TABLE}_IT_STG  WHERE 1=2;
#ENDSQL

#testdberror $?

#else

#echo
#echo "TABLE ${SAVEON_EAL_TABLE}_IT_ALL_KEYS ALREADY EXISTS IN SCHEMA $WH_POSTX_USER@$WH_POSTX_DATABASE"
#echo

#fi

SAVEON_VN_TABLE_ALL_KEYS_Present=`sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
set echo off verify off feed off termout off pages 0
SELECT COUNT(*) AS tablepresent FROM user_tables WHERE UPPER(table_name)=UPPER('${SAVEON_EAL_TABLE}_VN_ALL_KEYS');
ENDSQL`
testdberror $?
if [ ${SAVEON_VN_TABLE_ALL_KEYS_Present} -eq 0 ]
then
echo " "
                echo "${SAVEON_EAL_TABLE}_VN_ALL_KEYS table is not present in schema $WH_POSTX_USER@$WH_POSTX_DATABASE"
                echo
                echo "Creating table ${SAVEON_EAL_TABLE}_VN_ALL_KEYS"
                echo

sqlplus $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL

WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET SERVEROUTPUT ON;
SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0  VERIFY OFF LINESIZE 110
CREATE TABLE ${SAVEON_EAL_TABLE}_VN_ALL_KEYS AS SELECT LOCATION_ID FROM ${SAVEON_EAL_TABLE}_VN_STG  WHERE 1=2;
ENDSQL

testdberror $?

else

echo
echo "TABLE ${SAVEON_EAL_TABLE}_VN_ALL_KEYS ALREADY EXISTS IN SCHEMA $WH_POSTX_USER@$WH_POSTX_DATABASE"
echo

fi

SAVEON_CN_TABLE_ALL_KEYS_Present=`sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
set echo off verify off feed off termout off pages 0
SELECT COUNT(*) AS tablepresent FROM user_tables WHERE UPPER(table_name)=UPPER('${SAVEON_EAL_TABLE}_CN_ALL_KEYS');
ENDSQL`
testdberror $?
if [ ${SAVEON_CN_TABLE_ALL_KEYS_Present} -eq 0 ]
then
echo " "
                echo "${SAVEON_EAL_TABLE}_CN_ALL_KEYS table is not present in schema $WH_POSTX_USER@$WH_POSTX_DATABASE"
                echo
                echo "Creating table ${SAVEON_EAL_TABLE}_CN_ALL_KEYS"
                echo

sqlplus $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL

WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET SERVEROUTPUT ON;
SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0  VERIFY OFF LINESIZE 110
CREATE TABLE ${SAVEON_EAL_TABLE}_CN_ALL_KEYS AS SELECT CX FROM ${SAVEON_EAL_TABLE}_CN_STG  WHERE 1=2;
ENDSQL

testdberror $?

else

echo
echo "TABLE ${SAVEON_EAL_TABLE}_CN_ALL_KEYS ALREADY EXISTS IN SCHEMA $WH_POSTX_USER@$WH_POSTX_DATABASE"
echo

fi

echo "Script ${0} Execution Completed @ : `date`"
