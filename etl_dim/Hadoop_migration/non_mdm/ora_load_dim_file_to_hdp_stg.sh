#!/bin/bash
##########################################################################################################################################################
#
# Purpose: Script to load source file to stage table
# Change History Record:
#
# =======================================================================================================================================================
# Version | DATE       |   Modifier         |   Change made                                                                           | JIRA-ID
# ======================================================================================================================================================
# v1.0    | 10/10/2018 | VinodGardas        | 1. Hadoop Migration
# ======================================================================================================================================================
##########################################################################################################################################################

echo "######################################################################################";
echo
echo "Job name  - $0";
echo
echo "Number of Parameters - $#";
echo

#Checks the number of Parameters passed
if [ $# -ne 4 ]
then
         echo
         echo "***************************************************************************"
         echo
         echo "ERROR MESSAGE:"
         echo "--------------"
         echo
         echo "ERR001 -- SYNTAX ERROR. CHECK THE PARAMETERS GIVEN TO SHELL."
         echo ""
         echo "Usage: sh $0 <CONFIG_FILE> <FILENAME> <STG_TABLE> <META_TABLE>"
         echo "***************************************************************************"
         echo ""
         exit 1
fi

echo
echo "Script $0 execution started @ `date`"
echo

CONFIG_FILE=${1}

. ${CONFIG_FILE}

JOB_STRT_TS=`date '+%Y-%m-%d_%H%M%S'`
export JOB_STRT_TS=${JOB_STRT_TS}_$$

echo
echo "Start Date : $JOB_STRT_TS"
echo "Client Name: $CLIENT_UPR"
echo
echo

#set variables
#export OPSBLD_DATABASE=$OPSBLD_DB;
#export OPSBLD_USER=$OPSBLD_USER;
#export OPSBLD_PASSWORD=$OPSBLD_PASSWD;
#export DIM_DATABASE=$WH_POSTX_DATABASE;
#export DIM_USER=$WH_POSTX_USER;
#export DIM_PASSWORD=$WH_POSTX_PASSWORD;
export SCRIPTENV=`echo $DATAENV | tr "A-Z" "a-z"`;
export SRC_DIR=$SRC_DIR;
export SRC_FILE=$2;
export STG_TABLE=`echo ${3} | tr "a-z" "A-Z"`;
export META_TABLE=`echo ${4} | tr "a-z" "A-Z"`;

STARTDATE=`date`
JOBTYPE="RAW File Load to Stage Table "
CURR_TSP=`date +"%Y%m%d_%H%M%S"`

##############################################################

echo
echo "Started ${JOBTYPE} : ${STARTDATE}"
echo
echo
echo "Parameters passed to script"
echo "****************************************************************************"
#echo "OPSBLD_DATABASE  : ${OPSBLD_DATABASE}"
#echo "OPSBLD_USER      : ${OPSBLD_USER}"
#echo "OPSBLD_PASSWORD  : ${OPSBLD_PASSWORD}"
#echo "DIM_DATABASE     : ${DIM_DATABASE}"
#echo "DIM_USER         : ${DIM_USER}"
#echo "DIM_PASSWORD     : ${DIM_PASSWORD}"
echo "SCRIPT_ENV       : ${SCRIPTENV}"
echo "SRC_DIR          : ${SRC_DIR}"
echo "SRC_FILE         : ${SRC_FILE}"
echo "STG_TABLE        : ${STG_TABLE}"
echo "META_TABLE       : ${META_TABLE}"
#echo "FILEEXTENSION    : ${FILEEXTENSION}"
echo "******************************************************************************"

export PROGRAM_DIR=/opt/ildbld/${SCRIPTENV}
export GEN_DIR=${PROGRAM_DIR}/gen_scrpt/etl_rtlr_frmwrk/non_mdm/
export SCRIPT_UTIL_DIR=${PROGRAM_DIR}/etl_util/scrpt_util


#Create generated direcory if required
if [ ! -d "$GEN_DIR" ]
then
 mkdir -p $GEN_DIR
 chmod 775 $GEN_DIR
fi


echo
echo "Following directories are used in the process"
echo "**********************************************************************"
echo "SCRIPT UTIL DIR     : ${SCRIPT_UTIL_DIR}"
echo "GENERATED DIRECTORY : ${GEN_DIR}"
echo "**********************************************************************"

TABLENAME=${STG_TABLE}

#initialize error traps
. $SCRIPT_UTIL_DIR/ora_etl_error_traps.sh

#Fetching absolute file path

FILE_PATH=`find ${SRC_DIR}/${SRC_FILE}* -print | sort -r | head -n1`
FILE_NAME=`echo ${FILE_PATH} | awk -F "/" '{print $NF}'`

#Check for file availability an dexit if Not present

echo
echo "Latest file available is ${FILE_PATH}"
echo

testfilenotfound $FILE_PATH


echo "Converting file $FILE_NAME from dos to unix format"
dos2unix $FILE_PATH

#SCHEMA_SELECTION=`sqlplus -s ${OPSBLD_USER}/${OPSBLD_PASSWD}@${OPSBLD_DB}<<ENDSQL
#set heading off;
#SELECT * FROM (SELECT  COLUMN_NAME  || ' ' || COLUMN_TYPE  ||  DECODE( COLUMN_ORDER, (SELECT MAX(COLUMN_ORDER) FROM ${META_TABLE} ),'  ', ',' )
#FROM ${META_TABLE} ORDER BY COLUMN_ORDER);`

SCHEMA_SELECTION_TMP=`hive -e "select a.COLUMN_NAME, a.COLUMN_TYPE, ',' from (select COLUMN_NAME, COLUMN_TYPE, column_order from ${HIVE_DB}.${META_TABLE} ORDER BY column_order asc) a;"`
SCHEMA_SELECTION=`echo ${SCHEMA_SELECTION_TMP} | sed 's/.$//'`

echo
echo "Here is the Schema_selection: ${SCHEMA_SELECTION}"
#echo "SCHEMA_SELECTION_TMP: ${SCHEMA_SELECTION_TMP}"
echo

echo
echo "Creating Hive Table..."
echo "Loading FILE_PATH: ${FILE_PATH}"
echo

hive -e "DROP TABLE IF EXISTS ${HIVE_DB}.${STG_TABLE};

         CREATE TABLE ${HIVE_DB}.${STG_TABLE}(
               ${SCHEMA_SELECTION}
         )
         ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
         STORED AS TEXTFILE;
         LOAD DATA LOCAL INPATH '${FILE_PATH}' INTO TABLE ${HIVE_DB}.${STG_TABLE};"

#hive -e "DROP TABLE IF EXISTS ${HIVE_DB}.${STG_TABLE};

#         CREATE TABLE ${HIVE_DB}.${STG_TABLE}(
#               ${SCHEMA_SELECTION});"

echo
echo "Created the Stage Table..."
echo

echo
echo "***********************************************************************************"
echo
echo "Script execution completed @ `date`"

