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


echo "CREATE TABLE ${SAVEON_EAL_TABLE}_CN_ALL_KEYS if not present in our schema;"


SAVEON_EAL_TABLE_CN_ALL_KEYS_Present=`sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
set echo off verify off feed off termout off pages 0
SELECT COUNT(*) AS tablepresent FROM user_tables WHERE UPPER(table_name)=UPPER('${SAVEON_EAL_TABLE}_CN_ALL_KEYS');
ENDSQL`
testdberror $?
if [ ${SAVEON_EAL_TABLE_CN_ALL_KEYS_Present} -eq 0 ]
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
CREATE TABLE ${SAVEON_EAL_TABLE}_CN_ALL_KEYS (CUSTOMER_SK VARCHAR(50));
ENDSQL

testdberror $?

else

echo
echo "TABLE ${SAVEON_EAL_TABLE}_CN_ALL_KEYS ALREADY EXISTS IN SCHEMA $WH_POSTX_USER@$WH_POSTX_DATABASE"
echo

fi

echo "CREATE TABLE ${SAVEON_EAL_TABLE}_VN_all_keys if not present in our schema;"


SAVEON_EAL_TABLE_VN_all_keys_Present=`sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
set echo off verify off feed off termout off pages 0
SELECT COUNT(*) AS tablepresent FROM user_tables WHERE UPPER(table_name)=UPPER('${SAVEON_EAL_TABLE}_VN_all_keys');
ENDSQL`
testdberror $?
if [ ${SAVEON_EAL_TABLE_VN_all_keys_Present} -eq 0 ]
then
echo " "
                echo "${SAVEON_EAL_TABLE}_VN_all_keys table is not present in schema $WH_POSTX_USER@$WH_POSTX_DATABASE"
                echo
                echo "Creating table ${SAVEON_EAL_TABLE}_VN_all_keys"
                echo

sqlplus $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL

WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET SERVEROUTPUT ON;
SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0  VERIFY OFF LINESIZE 110
CREATE TABLE ${SAVEON_EAL_TABLE}_VN_all_keys (RETAIL_OUTLET_LOCATION_SK VARCHAR(50));
ENDSQL

testdberror $?

else

echo
echo "TABLE ${SAVEON_EAL_TABLE}_VN_all_keys ALREADY EXISTS IN SCHEMA $WH_POSTX_USER@$WH_POSTX_DATABASE"
echo

fi



echo "CREATE TABLE ${SAVEON_EAL_TABLE}_it_all_keys if not present in our schema;"


SAVEON_EAL_TABLE_it_all_keys_Present=`sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
set echo off verify off feed off termout off pages 0
SELECT COUNT(*) AS tablepresent FROM user_tables WHERE UPPER(table_name)=UPPER('${SAVEON_EAL_TABLE}_it_all_keys');
ENDSQL`
testdberror $?
if [ ${SAVEON_EAL_TABLE_it_all_keys_Present} -eq 0 ]
then
echo " "
                echo "${SAVEON_EAL_TABLE}_it_all_keys table is not present in schema $WH_POSTX_USER@$WH_POSTX_DATABASE"
                echo
                echo "Creating table ${SAVEON_EAL_TABLE}_it_all_keys"
                echo

sqlplus $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL

WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET SERVEROUTPUT ON;
SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0  VERIFY OFF LINESIZE 110
CREATE TABLE ${SAVEON_EAL_TABLE}_it_all_keys (ITEM_SK VARCHAR(50));
ENDSQL

testdberror $?

else

echo
echo "TABLE ${SAVEON_EAL_TABLE}_it_all_keys ALREADY EXISTS IN SCHEMA $WH_POSTX_USER@$WH_POSTX_DATABASE"
echo

fi


echo "CREATE TABLE ${SAVEON_EAL_TABLE}_POS_DEPT if not present in our schema;"


SAVEON_EAL_TABLE_POS_DEPT_Present=`sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
set echo off verify off feed off termout off pages 0
SELECT COUNT(*) AS tablepresent FROM user_tables WHERE UPPER(table_name)=UPPER('${SAVEON_EAL_TABLE}_POS_DEPT');
ENDSQL`
testdberror $?
if [ ${SAVEON_EAL_TABLE_POS_DEPT_Present} -eq 0 ]
then
echo " "
                echo "${SAVEON_EAL_TABLE}_POS_DEPT table is not present in schema $WH_POSTX_USER@$WH_POSTX_DATABASE"
                echo
                echo "Creating table ${SAVEON_EAL_TABLE}_POS_DEPT"
                echo

sqlplus $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL

WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET SERVEROUTPUT ON;
SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0  VERIFY OFF LINESIZE 110
CREATE TABLE ${SAVEON_EAL_TABLE}_POS_DEPT (ITEM_SK VARCHAR(50),POS_DEPT_SK VARCHAR(50));
ENDSQL

testdberror $?

else

echo
echo "TABLE ${SAVEON_EAL_TABLE}_POS_DEPT ALREADY EXISTS IN SCHEMA $WH_POSTX_USER@$WH_POSTX_DATABASE"
echo

fi



echo
echo "TRUNCATING EAL TABES IN SCHEMA $WH_POSTX_USER@$WH_POSTX_DATABASE"
echo

sqlplus $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL

WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET SERVEROUTPUT ON;
SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0  VERIFY OFF LINESIZE 110
TRUNCATE TABLE ${SAVEON_EAL_TABLE}_VN_all_keys;
TRUNCATE TABLE ${SAVEON_EAL_TABLE}_IT_all_keys;
TRUNCATE TABLE ${SAVEON_EAL_TABLE}_CN_all_keys;
TRUNCATE TABLE ${SAVEON_EAL_TABLE}_POS_DEPT;
COMMIT;
ENDSQL

testdberror $?



echo
echo "Script ${0} Execution Completed @ : `date`"

