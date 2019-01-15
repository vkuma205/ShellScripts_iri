#!/bin/bash
##########################################################################################################################################################
#
# Purpose: Script to load source file to stage table
# Change History Record:
#
# =======================================================================================================================================================
# Version | DATE       |   Modifier         |   Change made                                                                           | JIRA-ID
# ======================================================================================================================================================
# v1.0    | 05/27/2016 | RAGHAVENDRA B      | 1. New Script created                                                                   | ETLR-84

# v2.0    | 08/26/2016 | MUSANZIKWA I       | 1. Added file extension parameter for non dat files                                     |


# v2.0    | 04/11/2017 | MUSANZIKWA I       | 1. Added fetching parameters from file

# v3.0    | 03/24/2018 | VinodGardas        | 1. Added logs
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
echo "Container id : $CONTAINER_ID"
echo "Client Name: $CLIENT_UPR"
echo
echo
#set variables
export OPSBLD_DATABASE=$OPSBLD_DB;
export OPSBLD_USER=$OPSBLD_USER;
export OPSBLD_PASSWORD=$OPSBLD_PASSWD;
export DIM_DATABASE=$WH_POSTX_DATABASE;
export DIM_USER=$WH_POSTX_USER;
export DIM_PASSWORD=$WH_POSTX_PASSWORD;
export SCRIPTENV=`echo $DATAENV | tr "A-Z" "a-z"`;
export SRC_DIR=$SRC_DIR;
export SRC_FILE=$2;
export STG_TABLE=`echo ${3} | tr "a-z" "A-Z"`;
export META_TABLE=`echo ${4} | tr "a-z" "A-Z"`;
#export FILEEXTENSION=${FILE_EXTENSION};

STARTDATE=`date`
JOBTYPE="RAW File Load to Stage Table "
CURR_TSP=`date +"%Y%m%d_%H%M%S"`

#########################################################

echo
echo "Started ${JOBTYPE} : ${STARTDATE}"
echo
echo
echo "Parameters passed to script"
echo "****************************************************************************"
echo "OPSBLD_DATABASE  : ${OPSBLD_DATABASE}"
echo "OPSBLD_USER      : ${OPSBLD_USER}"
echo "OPSBLD_PASSWORD  : ${OPSBLD_PASSWORD}"
echo "DIM_DATABASE     : ${DIM_DATABASE}"
echo "DIM_USER         : ${DIM_USER}"
echo "DIM_PASSWORD     : ${DIM_PASSWORD}"
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

#Create STG Table DDL for new run

export DDL_SQL_FILE_TEMP=${GEN_DIR}/ORA_CREATE_TEMP_${STG_TABLE}_${CURR_TSP}.sql
touch $DDL_SQL_FILE_TEMP
chmod 777 $DDL_SQL_FILE_TEMP

export DDL_SQL_FILE=${GEN_DIR}/ORA_CREATE_${STG_TABLE}_${CURR_TSP}.sql
touch $DDL_SQL_FILE
chmod 777 $DDL_SQL_FILE

echo "SELECT 'CREATE TABLE ${STG_TABLE} ('  AS COL FROM DUAL
UNION ALL
SELECT * FROM (SELECT  COLUMN_NAME  || ' ' || COLUMN_TYPE  ||  DECODE( COLUMN_ORDER, (SELECT MAX(COLUMN_ORDER) FROM ${META_TABLE} ),'  ', ',' ) 
FROM ${META_TABLE} ORDER BY COLUMN_ORDER)
UNION ALL
SELECT ') COMPRESS FOR QUERY HIGH NOLOGGING ;' FROM DUAL;" > $DDL_SQL_FILE_TEMP

echo
cat $DDL_SQL_FILE_TEMP
echo


sqlplus -s ${OPSBLD_USER}/${OPSBLD_PASSWORD}@${OPSBLD_DATABASE} <<EOF
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;
ALTER SESSION SET PARALLEL_DEGREE_POLICY=MANUAL;
ALTER SESSION FORCE PARALLEL DDL PARALLEL $DOP;
ALTER SESSION FORCE PARALLEL DML PARALLEL $DOP;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL $DOP;
SET SERVEROUTPUT ON HEADING OFF ECHO OFF FEEDBACK ON PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 110;
SPOOL $DDL_SQL_FILE
@${DDL_SQL_FILE_TEMP}
SPOOL OFF
COMMIT;
EXIT;
EOF


if [ $? -ne 0 ]
then
   echo "Oracle error occured. Couldnt perform table create operation."
   echo "*****************************************************************************"
   exit -1;
fi

echo
cat $DDL_SQL_FILE
echo

sqlplus -s ${DIM_USER}/${DIM_PASSWORD}@${DIM_DATABASE} <<EOF
SET SERVEROUTPUT ON HEADING OFF ECHO OFF FEEDBACK ON PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 110;
DROP TABLE  ${STG_TABLE} PURGE;
EXIT;
EOF



sqlplus -s ${DIM_USER}/${DIM_PASSWORD}@${DIM_DATABASE} <<EOF
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;
ALTER SESSION SET PARALLEL_DEGREE_POLICY=MANUAL;
ALTER SESSION FORCE PARALLEL DDL PARALLEL $DOP;
ALTER SESSION FORCE PARALLEL DML PARALLEL $DOP;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL $DOP;
SET SERVEROUTPUT ON HEADING OFF ECHO OFF FEEDBACK ON PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 110;
@${DDL_SQL_FILE}
COMMIT;
EXIT;
EOF

if [ $? -ne 0 ]
then
   echo "Oracle error occured. Couldnt perform table create operation."
   echo "*****************************************************************************"
   exit -1;
fi

echo "Loading file ${FILE_PATH} to ${STG_TABLE}"
echo "sh /opt/ildbld/${SCRIPTENV}/etl_util/ora_util/ora_sqlldr.sh ${DIM_DATABASE} ${DIM_USER} ${DIM_PASSWORD} ${STG_TABLE} ${FILE_PATH} true ${SCRIPTENV}"


sh /opt/ildbld/${SCRIPTENV}/etl_util/ora_util/ora_sqlldr.sh ${DIM_DATABASE} ${DIM_USER} ${DIM_PASSWORD} ${STG_TABLE} ${FILE_PATH} true ${SCRIPTENV}

if [ $? -ne 0 ]
then
   echo "Oracle error occurred. Couldn't load data to ${STG_TABLE}."
   exit -1;
else
   echo "${STG_TABLE} Loaded"
fi


#if [ ${STG_TABLE} = "SOBEYS_CN_SRC_STG" ]
#then
#   echo "Altering ${STG_TABLE} to include additional shopper attributes from another file."

echo "***********************************************************************************************"
echo "---Initializing consumer tables used in EAL process-----"
echo "***********************************************************************************************"

sh ${SCRIPTDIR}/initialize_non_mdm_schema_tables.sh ${CONFIG_FILE}


echo "sqlplus ${DIM_USER}/${DIM_PASSWORD}@${DIM_DATABASE}<<ENDSQL
                WHENEVER SQLERROR EXIT 1
                SET ECHO ON
                @ ${SCRIPTDIR}/${CONSUMER_SQL}
ENDSQL

"

sqlplus ${DIM_USER}/${DIM_PASSWORD}@${DIM_DATABASE}<<ENDSQL
                WHENEVER SQLERROR EXIT 1
                SET ECHO ON
                @ ${SCRIPTDIR}/${CONSUMER_SQL}
ENDSQL

testdberror $?
#fi

echo "backing up ${FILE_NAME_PREFIX} files to ${BACKUP_DIR} folder"

mv ${SRC_DIR}/*${SRC_FILE} ${BACKUP_DIR}
 
echo
echo "Script $0 execution completed @  `date`"
