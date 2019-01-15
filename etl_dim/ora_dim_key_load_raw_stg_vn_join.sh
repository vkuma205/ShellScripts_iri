#!/bin/bash
########################################################################################################################
#
# Purpose: Join venue table with StoresIn for SAVEON
#
# Change History Record: 
#
# ======================================================================================================================
# DATE        |   AUTHOR NAME        |    CHANGE DESCRIPTION | JIRA_ID 
# ======================================================================================================================
# 06 April 2018 |   Vinod Gardas       |    New script created |
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
SI_TABLE=`echo ${SI_TABLE} | awk '{print toupper($0)}'`
VNS_TABLE=`echo ${VNS_TABLE} | awk '{print toupper($0)}'`
STG_VN_JOIN_TAB=`echo ${STG_VN_JOIN_TAB} | awk '{print toupper($0)}'`

# Reading Meta info
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

STG_TAB=${CLIENT_UPR}_${DIM_ABBR_UPR}_STG
echo "Stage Table Name : "$STG_TAB

EXT_TAB_EXISTS=`sqlplus -s ${WH_POSTX_USER}/${WH_POSTX_PASSWORD}@${WH_POSTX_DATABASE} <<ENDSQL
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;
SET SERVEROUTPUT OFF HEADING OFF ECHO OFF FEEDBACK OFF PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 500;

SELECT COUNT(1)
FROM USER_TABLES
WHERE TABLE_NAME='${STG_TAB}';
ENDSQL`

testdberror $?

if [ ${EXT_TAB_EXISTS} -eq 1 ]
then

echo
echo " ${STG_TAB} Table  exists ..!! So dropping the table and creating"
echo
echo "DROP AND CREATE TABLE ${STG_TAB} "
echo
echo "Joining SAVEON_VN_STG with SAVEON_SI_SRC_STG have started..."

sqlplus -s ${WH_POSTX_USER}/${WH_POSTX_PASSWORD}@${WH_POSTX_DATABASE} <<ENDSQL
               WHENEVER SQLERROR EXIT 1
               SET ECHO ON
               @ ${SCRIPTDIR}/${CLIENT_UPR}_${DIM_ABBR_UPR}.sql
ENDSQL

#WHENEVER SQLERROR EXIT FAILURE;
#WHENEVER OSERROR EXIT FAILURE;

#ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
#ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
#ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

#SET SERVEROUTPUT ON;
#SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 110;
#DROP TABLE ${STG_TAB};
#CREATE TABLE ${STG_TAB} AS
#SELECT SI.SX,SI.STORENAME,SI.STORENUMBER,S.OPENING_DATE,S.CLOSING_DATE,S.LOCATION_DESC,S.ADDRESS_LINE_1,S.ADDRESS_LINE_2,S.TOWN_CODE,S.PROVINCE_CODE,S.POSTAL_CODE,S.DIVISION_DESC,S.RM_REGION_ID,S.PRICING_ZONE_ID,S.LOCATION_ID FROM ${SI_TABLE} SI join ${VNS_TABLE} S on SI.STORENUMBER=S.LOCATION_ID; 
#ENDSQL

#else

#echo "CREATE TABLE ${STG_TAB}."
#sqlplus -s ${WH_POSTX_USER}/${WH_POSTX_PASSWORD}@${WH_POSTX_DATABASE} <<ENDSQL
#                 WHENEVER SQLERROR EXIT 1
#                    SET ECHO ON
#               @ ${SCRIPTDIR}/${VenueJoin}
#ENDSQL
#WHENEVER SQLERROR EXIT FAILURE;
#WHENEVER OSERROR EXIT FAILURE;

#ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
#ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
#ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

#SET SERVEROUTPUT ON;
#SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 110;

#CREATE TABLE ${STG_TAB} AS
#SELECT SI.SX,SI.STORENAME,SI.STORENUMBER,S.OPENING_DATE,S.CLOSING_DATE,S.LOCATION_DESC,S.ADDRESS_LINE_1,S.ADDRESS_LINE_2,S.TOWN_CODE,S.PROVINCE_CODE,S.POSTAL_CODE,S.DIVISION_DESC,S.RM_REGION_I
#D,S.PRICING_ZONE_ID,S.LOCATION_ID FROM ${SI_TABLE} SI join ${VNS_TABLE} S on SI.STORENUMBER=S.LOCATION_ID;

#ENDSQL

fi

testdberror $?

echo
echo "Joining SAVEON_VN_STG with SAVEON_SI_SRC_STG have completed succesfully"
echo
echo
echo "**********************************************************************************"
echo

echo
echo "Script execution completed @ `date`"

