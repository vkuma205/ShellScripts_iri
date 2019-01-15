#!/bin/bash

########################################################################################################################
#
# Purpose: create eal stage, master and delta tables and publish MDM keying
#
# Change History Record:
#
# ======================================================================================================================
# DATE         |   AUTHOR NAME        |    CHANGE DESCRIPTION | JIRA_ID
# ======================================================================================================================
# 06 April  2018 |   Vinod Gardas   |    New script created | 
# ----------------------------------------------------------------------------------------------------------------------
########################################################################################################################


echo
echo "######################################################################################"
echo
echo "Job name - $0"
echo
echo "Number of Parameters - $#"
echo

#Parameters validation
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
        echo "Usage: sh $0 CONFIG_FILE CONTAINER_ID"
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

echo "sqlplus -s ${OPSBLD_USER}/${OPSBLD_PASSWD}@${OPSBLD_DB}<<ENDSQL
SET HEADING OFF ECHO OFF FEEDBACK OFF PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 500;
WHENEVER SQLERROR EXIT 1
SELECT CLIENT || '|' || DIM_NAME || '|' || DIM_ABBR || '|' || FILE_NAME_PREFIX || '|' || UPPER(CNTRY_CD) || '|' || SURROGATE_KEY FROM ${KEY_DEFN_TAB} WHERE
CONTAINER_ID=${CONTAINER_ID};
ENDSQL
"

###extracting the required metadata attributes for a specific dimension

META_INFO=`sqlplus -s ${OPSBLD_USER}/${OPSBLD_PASSWD}@${OPSBLD_DB}<<ENDSQL
SET HEADING OFF ECHO OFF FEEDBACK OFF PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 500;
WHENEVER SQLERROR EXIT 1
SELECT CLIENT || '|' || DIM_NAME || '|' || DIM_ABBR || '|' || FILE_NAME_PREFIX || '|' || UPPER(CNTRY_CD) || '|' || SURROGATE_KEY FROM ${KEY_DEFN_TAB} WHERE 
CONTAINER_ID=${CONTAINER_ID};
ENDSQL`

testdberror $?

echo
echo "META_INFO:$META_INFO"
echo


if [ -z "${META_INFO}" ]
then
        echo
        echo " Error in fetching META_INFO from table ${KEY_DEFN_TAB} on ${OPSBLD_USER}@${OPSBLD_DB} for container id ${CONTAINER_ID}"
        exit -1;
fi

echo
echo "META_INFO:$META_INFO"
echo


CLIENT_LWR=`echo ${CLIENT} | awk '{print tolower($0)}'`
CLIENT_UPR=`echo ${CLIENT} | awk '{print toupper($0)}'`

FILE_NAME_PREFIX=`echo ${META_INFO} | cut -d'|' -f4`
DIM_NAME=`echo ${META_INFO} | cut -d'|' -f2`
DIM_ABBR=`echo ${META_INFO} | cut -d'|' -f3`
CNTRY_CD=`echo ${META_INFO} | cut -d'|' -f5`
SURROGATE_KEY=`echo ${META_INFO} | cut -d'|' -f6`


DIM_NAME_LWR=`echo ${DIM_NAME} | awk '{print tolower($0)}'`
DIM_NAME_UPR=`echo ${DIM_NAME} | awk '{print toupper($0)}'`
DIM_ABBR_LWR=`echo ${DIM_ABBR} | awk '{print tolower($0)}'`
DIM_ABBR_UPR=`echo ${DIM_ABBR} | awk '{print toupper($0)}'`

STG_TAB=${CLIENT_UPR}_${DIM_ABBR_UPR}_STG
RAW_TAB=${CLIENT_UPR}_${DIM_ABBR_UPR}_EXT

EAL_STG_TAB=EAL_DK_${CLIENT_UPR}_${DIM_ABBR_UPR}_STG
EAL_MSTR_TAB=EAL_DK_${CLIENT_UPR}_${DIM_ABBR_UPR}_MSTR
EAL_DELTA_TAB=EAL_DK_${CLIENT_UPR}_${DIM_ABBR_UPR}_DELTA


DELTA_FILE_PATH=${LOGDIR}
DELTA_FILE=${CLIENT_LWR}_${DIM_NAME_LWR}_${FILE_DATE}_${JOB_STRT_TS}
DELTA_FILE_LWR=`echo ${DELTA_FILE} | awk '{print tolower($0)}'`
EAL_DELTA_TAB_KEYED=EAL_${CLIENT_UPR}_${DIM_ABBR}_DLT_KYD

echo
echo "CLIENT_UPR :$CLIENT_UPR"
echo "FILE_NAME_PREFIX :$FILE_NAME_PREFIX"
echo "DIM_NAME :$DIM_NAME_UPR"
echo "DIM_ABBR :$DIM_ABBR_UPR"
echo "CNTRY_CD :$CNTRY_CD"
echo "SURROGATE_KEY :$SURROGATE_KEY"
echo "STG_TAB :$STG_TAB"
echo "RAW_TAB :$RAW_TAB"
echo "EAL_STG_TAB :$EAL_STG_TAB"
echo "EAL_MSTR_TAB :$EAL_MSTR_TAB"
echo "EAL_DELTA_TAB :$EAL_DELTA_TAB"
echo "DELTA_FILE_PATH :$DELTA_FILE_PATH"
echo "DELTA_FILE :$DELTA_FILE_LWR"
echo "EAL_DELTA_TAB_KEYED :$EAL_DELTA_TAB_KEYED"
echo 


CLIENT_ID=`sqlplus -s ${OPSBLD_USER}/${OPSBLD_PASSWD}@${OPSBLD_DB}<<ENDSQL
set echo off verify off feed off termout off lines 1000 pages 0
SELECT CLIENT_ID FROM MDM_CLIENT_MAP WHERE UPPER(CLIENT_SHORT_NAME)=UPPER('${CLIENT_UPR}');
ENDSQL`

if [ -z "${CLIENT_ID}" ]
then
        echo
        echo "Error in fetching client id "
        exit -1;
fi

echo
echo "CLIENT_ID: $CLIENT_ID"
echo

#Table Definition from Key Definition Table for creating EAL DELTA and MSTR TABLE. 
TAB_DEFN_TEMP=`sqlplus -s ${OPSBLD_USER}/${OPSBLD_PASSWD}@${OPSBLD_DB}<<ENDSQL
set heading off echo off verify off feed off termout off pages 0 linesize 10000  long 3000 trims on trim on
SELECT REPLACE(STG_COLUMN_META,' ','@') FROM ${KEY_DEFN_TAB} WHERE CONTAINER_ID=${CONTAINER_ID};
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

EAL_STG_TAB_PRESENT=`sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
set echo off verify off feed off termout off pages 0
SELECT COUNT(*) AS tablepresent FROM user_tables WHERE UPPER(table_name)=UPPER('${EAL_STG_TAB}');
ENDSQL`

testdberror $?

if [ ${EAL_STG_TAB_PRESENT} -eq 0 ]
then
                echo " "
                echo "${EAL_STG_TAB} table is not present in schema $WH_POSTX_USER@$WH_POSTX_DATABASE"
                echo " "
                echo "Creating table ${EAL_STG_TAB}"
                echo " "

                echo " CREATE TABLE ${EAL_STG_TAB} AS SELECT TO_NUMBER('123456789','999999999') ${DIM_ABBR_UPR}_DIM_KEY, A.* FROM ${STG_TAB} A WHERE 1=2; " > ${GENDIR}/ora_dim_key_load_eal_stage_${JOB_STRT_TS}.sql

                sqlplus $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
                WHENEVER SQLERROR EXIT 1
                SET ECHO ON
#                @ ${GENDIR}/ora_dim_key_load_eal_stage_${JOB_STRT_TS}.sql
                CREATE TABLE ${EAL_STG_TAB} AS SELECT TO_NUMBER('123456789','999999999') ${DIM_ABBR_UPR}_DIM_KEY, A.* FROM ${STG_TAB} A WHERE 1=2;          

ENDSQL

        testdberror $?

        rm -f  ${GENDIR}/ora_dim_key_load_eal_stage_${JOB_STRT_TS}.sql
fi


echo "**********************************************************************************"


EAL_MSTR_TAB_PRESENT=`sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
set echo off verify off feed off termout off pages 0
SELECT COUNT(*) AS tablepresent FROM user_tables WHERE UPPER(table_name)=UPPER('${EAL_MSTR_TAB}');
ENDSQL`

testdberror $?

if [ ${EAL_MSTR_TAB_PRESENT} -eq 0 ]
then
                echo " "
                echo "${EAL_MSTR_TAB} table is not present in schema $WH_POSTX_USER@$WH_POSTX_DATABASE"
                echo " "
                echo "Creating table ${EAL_MSTR_TAB}"
                echo " "

                echo " CREATE TABLE ${EAL_MSTR_TAB} (${SURROGATE_KEY} NUMBER, ${TAB_DEFN} ); " > ${GENDIR}/ora_dim_key_load_eal_mstr_${JOB_STRT_TS}.sql

                sqlplus $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
                WHENEVER SQLERROR EXIT 1
                SET ECHO ON
                CREATE TABLE ${EAL_MSTR_TAB} (${SURROGATE_KEY} NUMBER, ${TAB_DEFN} );
               
ENDSQL

        testdberror $?

        rm -f  ${GENDIR}/ora_dim_key_load_eal_mstr_${JOB_STRT_TS}.sql
fi

echo "**********************************************************************************"

EAL_DELTA_TAB_PRESENT=`sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
set echo off verify off feed off termout off pages 0
SELECT COUNT(*) AS tablepresent FROM user_tables WHERE UPPER(table_name)=UPPER('${EAL_DELTA_TAB}');
ENDSQL`

 testdberror $?

if [ ${EAL_DELTA_TAB_PRESENT} -eq 0 ]
then
                echo " "
                echo "${EAL_DELTA_TAB} table is not present in schema $WH_POSTX_USER@$WH_POSTX_DATABASE"
                echo " "
                echo "Creating table ${EAL_DELTA_TAB}"
                echo " "

                echo " CREATE TABLE ${EAL_DELTA_TAB} (${SURROGATE_KEY} NUMBER, ${TAB_DEFN} ); " > ${GENDIR}/ora_dim_key_load_eal_delta_${JOB_STRT_TS}.sql

                sqlplus $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
                WHENEVER SQLERROR EXIT 1
                SET ECHO ON
                CREATE TABLE ${EAL_DELTA_TAB} (${SURROGATE_KEY} NUMBER, ${TAB_DEFN} );
                
ENDSQL

        testdberror $?

        rm -f  ${GENDIR}/ora_dim_key_load_eal_delta_${JOB_STRT_TS}.sql
fi


echo "**********************************************************************************"
### Checks for the Natural Key Existance in the metadata table
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


echo "**********************************************************************************"

### Extracting the list of Attributes with filedate 

echo "sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
set echo off verify off feed off termout off pages 0 lines 10000
SELECT LISTAGG(COLUMN_NAME,',') WITHIN GROUP(ORDER BY COLUMN_ID) FROM USER_TAB_COLS WHERE UPPER(TABLE_NAME)=UPPER('${STG_TAB}');
ENDSQL"


FACT_SELECT=`sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
set echo off verify off feed off termout off pages 0 lines 10000
SELECT LISTAGG(COLUMN_NAME,',') WITHIN GROUP(ORDER BY COLUMN_ID) FROM USER_TAB_COLS WHERE UPPER(TABLE_NAME)=UPPER('${STG_TAB}');
ENDSQL`

if [ -z "${FACT_SELECT}" ]
then
        echo
        echo "FACT_SELECT not fetched properly"
        exit -1;
fi

### Extracting the list of Attributes without file date

FACT_SELECT_TAB_PREFIX=`sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
set echo off verify off feed off termout off pages 0 lines 10000
SELECT LISTAGG(COLUMN_NAME,',') WITHIN GROUP(ORDER BY COLUMN_ID) FROM USER_TAB_COLS WHERE UPPER(TABLE_NAME)=UPPER('${EAL_DELTA_TAB}') AND COLUMN_ID > 1;
ENDSQL`

if [ -z "${FACT_SELECT_TAB_PREFIX}" ]
then
        echo
        echo "FACT_SELECT_TAB_PREFIX not fetched properly"
        exit -1;
fi

FACT_SELECT_CONCAT=`echo ${FACT_SELECT} | sed 's/,/ || '\''|'\'' ||/g'`

echo 
echo "FACT_SELECT_TAB_PREFIX: $FACT_SELECT_TAB_PREFIX"
echo "FACT_SELECT_CONCAT: $FACT_SELECT_CONCAT"
echo 

### Inserting data to EAL Staging table
echo "................Adding missing dimensions from fact file...................."

if [ ${DIM_ABBR_UPR} = "IT" ]
then

echo
echo " Adding missing records in ITEM Dimension from fact file"
echo

echo "
truncate table SAVEON_IT_ALL_KEYS_MSTR;
insert into SAVEON_IT_ALL_KEYS_MSTR select * from SAVEON_IT_ALL_KEYS;

DELETE FROM SAVEON_IT_ALL_KEYS WHERE UPC IN (select UPC  from ${EAL_MSTR_TAB});
DELETE FROM SAVEON_IT_ALL_KEYS WHERE UPC IN (select UPC  from $STG_TAB);
INSERT INTO $STG_TAB (UPC,ITEM_ID,ITEM_DESC,LEAD_ITEM_RETAIL_CD,PADDED_UPC,PADDED_ITEM_ID) SELECT UPC,0 , 'UNKNOWN PRODUCT FROM FACT',0,LPAD( CAST(UPC AS VARCHAR2(100 BYTE)), 14,'0') PADDED_UPC,LPAD( CAST(0 AS VARCHAR2(100 BYTE)), 7,'0') PADDED_RETAIL_ITEM_CD FROM SAVEON_IT_ALL_KEYS;
COMMIT;
#COMMIT;
"

echo
sqlplus -s ${WH_POSTX_USER}/${WH_POSTX_PASSWORD}@${WH_POSTX_DATABASE} <<ENDSQL
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET SERVEROUTPUT ON;
SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 110;



DELETE FROM ${SAVEON_IT_ALL_KEYS} WHERE UPC IN (select UPC  from ${EAL_MSTR_TAB});
COMMIT;

DELETE FROM ${SAVEON_IT_ALL_KEYS} WHERE UPC IN (select UPC   from $STG_TAB);
COMMIT;

INSERT INTO $STG_TAB (UPC,ITEM_ID,RETAIL_ITEM_CD,ITEM_DESC,LEAD_ITEM_RETAIL_CD,PADDED_UPC,PADDED_RETAIL_ITEM_CD) SELECT UPC,0,0,'UNKNOWN PRODUCT FROM FACT',0,LPAD( CAST(UPC AS VARCHAR2(100 BYTE)), 14,'0') PADDED_UPC,LPAD( CAST(0 AS VARCHAR2(100 BYTE)), 7,'0') PADDED_RETAIL_ITEM_CD FROM ${SAVEON_IT_ALL_KEYS};
COMMIT;



ENDSQL
testdberror $?
else

echo
echo " Adding missing records in VENUE Dimension from fact file"
echo
echo "
truncate table SAVEON_VN_ALL_KEYS_MSTR;
insert into SAVEON_VN_ALL_KEYS_MSTR select * from SAVEON_VN_ALL_KEYS;

DELETE FROM SAVEON_VN_ALL_KEYS WHERE LOCATION_ID IN (select cast(LOCATION_ID as varchar(10))  from ${EAL_MSTR_TAB});
DELETE FROM SAVEON_VN_ALL_KEYS WHERE LOCATION_ID IN (select cast(LOCATION_ID as varchar(10)) from $STG_TAB);

INSERT INTO $STG_TAB (SX,STORENAME,STORENUMBER,OPENING_DATE,CLOSING_DATE,LOCATION_DESC,ADDRESS_LINE_1,ADDRESS_LINE_2,TOWN_CODE,PROVINCE_CODE,POSTAL_CODE,DIVISION_DESC,RM_REGION_ID,PRICING_ZONE_ID,LOCATION_ID) SELECT '-1','UNKNOWN',LOCATION_ID,'UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','-1','-1',LOCATION_ID FROM SAVEON_VN_ALL_KEYS;
COMMIT;

"

sqlplus -s ${WH_POSTX_USER}/${WH_POSTX_PASSWORD}@${WH_POSTX_DATABASE} <<ENDSQL
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET SERVEROUTPUT ON;
SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 110;


DELETE FROM ${SAVEON_VN_ALL_KEYS} WHERE LOCATION_ID IN (select cast(LOCATION_ID as varchar(10))  from ${EAL_MSTR_TAB});
COMMIT;

DELETE FROM ${SAVEON_VN_ALL_KEYS} WHERE LOCATION_ID IN (select cast(LOCATION_ID as varchar(10))  from $STG_TAB);
COMMIT;

INSERT INTO $STG_TAB (OPENING_DATE,CLOSING_DATE,LOCATION_DESC,ADDRESS_LINE_1,ADDRESS_LINE_2,TOWN_CODE,PROVINCE_CODE,POSTAL_CODE,DIVISION_DESC,RM_REGION_ID,PRICING_ZONE_ID,LOCATION_ID) SELECT 'UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','-1','-1',LOCATION_ID FROM ${SAVEON_VN_ALL_KEYS};
COMMIT;


ENDSQL

testdberror $?

fi


echo "******************************** EAL STG TBL inserting QRY ************************************"
echo
echo "TRUNCATE TABLE ${EAL_STG_TAB}; INSERT INTO ${EAL_STG_TAB} ( ${FACT_SELECT} ) SELECT ${FACT_SELECT} FROM ${STG_TAB}; COMMIT; > ${GENDIR}/ora_dim_key_load_eal_stage_${JOB_STRT_TS}.sql"
echo

echo "TRUNCATE TABLE ${EAL_STG_TAB}; 
INSERT INTO ${EAL_STG_TAB} ( ${FACT_SELECT} ) SELECT DISTINCT ${FACT_SELECT} FROM ${STG_TAB}; 
COMMIT;" > ${GENDIR}/ora_dim_key_load_eal_stage_${JOB_STRT_TS}.sql

sqlplus $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
WHENEVER SQLERROR EXIT 1 
SET HEADING OFF ECHO ON;
TRUNCATE TABLE ${EAL_STG_TAB}; 
INSERT INTO ${EAL_STG_TAB}  SELECT DISTINCT NULL,A.* FROM ${STG_TAB} A;
COMMIT;

ENDSQL

testdberror $?

echo "${GENDIR}/ora_dim_key_load_eal_stage_${JOB_STRT_TS}.sql"

 rm -f  ${GENDIR}/ora_dim_key_load_eal_stage_${JOB_STRT_TS}.sql

echo
echo "**********************************************************************************"
echo

#here considered  FACT_SELECT_TAB_PREFIX's attribute list to ignore filedate and  pulled out the delta records

echo " INSERT INTO ${EAL_DELTA_TAB} ( ${FACT_SELECT_TAB_PREFIX} ) SELECT DISTINCT ${FACT_SELECT_TAB_PREFIX} FROM ${STG_TAB};"

sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET HEADING OFF ECHO ON;

TRUNCATE TABLE ${EAL_DELTA_TAB};


INSERT INTO ${EAL_DELTA_TAB}  SELECT DISTINCT NULL ,A.* FROM ( SELECT ${FACT_SELECT_TAB_PREFIX} FROM ${STG_TAB}) A ;

COMMIT;
ENDSQL

testdberror $?


EAL_DELTA_TAB_KEYED_PRESENT=`sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
set echo off verify off feed off termout off pages 0
SELECT COUNT(*) AS tablepresent FROM user_tables WHERE UPPER(table_name)=UPPER('${EAL_DELTA_TAB_KEYED}');
ENDSQL`

testdberror $?


#####case condition to determine the dimension type and to pick the respective tables for the respective dimensions


case $DIM_ABBR_UPR in 

###ITEM
"IT")

if [ ${EAL_DELTA_TAB_KEYED_PRESENT} -eq 0 ]
then
        echo
        echo "The table ${EAL_DELTA_TAB_KEYED} does not exist in $WH_POSTX_USER@$WH_POSTX_PASSWORD"
        echo "Creating Table ${EAL_DELTA_TAB_KEYED}"

        echo " CREATE TABLE ${EAL_DELTA_TAB_KEYED} (
                ${SURROGATE_KEY} VARCHAR2(50),
                SYSTEM_CODE_NUM VARCHAR2(50),
                VENDOR_CODE_NUM VARCHAR2(50),
                ITEM_CODE_NUM VARCHAR2(50),
                GENERATION_CODE_NUM VARCHAR2(50),      
                COUNTRY_CODE VARCHAR2(50),
                SOURCE_ID VARCHAR2(50),
                CLIENT_ID VARCHAR2(50),
                ALT_KEY_TXT VARCHAR2(2000)
                );"
				
				
sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET HEADING OFF ECHO OFF FEEDBACK OFF PAGES 0  VERIFY OFF LINESIZE 2000;

                CREATE TABLE ${EAL_DELTA_TAB_KEYED} (
                ${SURROGATE_KEY} VARCHAR2(50),
                SYSTEM_CODE_NUM VARCHAR2(50),
                VENDOR_CODE_NUM VARCHAR2(50),
                ITEM_CODE_NUM VARCHAR2(50),
                GENERATION_CODE_NUM VARCHAR2(50),       
                COUNTRY_CODE VARCHAR2(50),
                SOURCE_ID VARCHAR2(50),
                CLIENT_ID VARCHAR2(50),
                ALT_KEY_TXT VARCHAR2(2000)
                );
ENDSQL

testdberror $?

else

echo "${EAL_DELTA_TAB_KEYED} present"
echo "TRUNCATE TABLE ${EAL_DELTA_TAB_KEYED};"

sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
TRUNCATE TABLE ${EAL_DELTA_TAB_KEYED};
ENDSQL

testdberror $?

fi

;;

####Venue
"VN")


if [ ${EAL_DELTA_TAB_KEYED_PRESENT} -eq 0 ]
then
        echo
        echo "The table ${EAL_DELTA_TAB_KEYED} does not exist in $WH_POSTX_USER@$WH_POSTX_PASSWORD"
        echo "Creating Table ${EAL_DELTA_TAB_KEYED}"

         echo
        echo "The table ${EAL_DELTA_TAB_KEYED} does not exist in $WH_POSTX_USER@$WH_POSTX_PASSWORD"
        echo "Creating Table ${EAL_DELTA_TAB_KEYED}"

        echo " CREATE TABLE ${EAL_DELTA_TAB_KEYED} (
                ${SURROGATE_KEY} VARCHAR2(50),
                IRI_STORE_NUMBER VARCHAR2(50),
                LEGACY_SEQ_KEY VARCHAR2(50),
                ADDRESS_ID VARCHAR2(50),
                COUNTRY_CODE VARCHAR2(50),
                SOURCE_ID VARCHAR2(50),
                CLIENT_ID VARCHAR2(50),
                ALT_KEY_TXT VARCHAR2(2000)
                );"
	
				
sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET HEADING OFF ECHO OFF FEEDBACK OFF PAGES 0  VERIFY OFF LINESIZE 2000;

              CREATE TABLE ${EAL_DELTA_TAB_KEYED} (
                ${SURROGATE_KEY} VARCHAR2(50),
                IRI_STORE_NUMBER VARCHAR2(50),
                LEGACY_SEQ_KEY VARCHAR2(50),
                ADDRESS_ID VARCHAR2(50),
                COUNTRY_CODE VARCHAR2(50),
                SOURCE_ID VARCHAR2(50),
                CLIENT_ID VARCHAR2(50),
                ALT_KEY_TXT VARCHAR2(2000)
                );
ENDSQL

testdberror $?

else

echo "${EAL_DELTA_TAB_KEYED} present"
echo "TRUNCATE TABLE ${EAL_DELTA_TAB_KEYED};"

sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL
TRUNCATE TABLE ${EAL_DELTA_TAB_KEYED};
ENDSQL

testdberror $?

fi

;;
esac

echo "************************************************************************************************************"
echo "Initializing Tables used in EAL Process"
echo "************************************************************************************************************"

#sh ${SCRIPTDIR}/initialize_mdm_schema_tables.sh ${CONFIG_FILE}
sh ${SCRIPTDIR}/initialize_non_mdm_schema_tables.sh ${CONFIG_FILE}

echo "**********************************************************************************"
echo "creating text table"


case $DIM_ABBR_UPR in 
"IT") 

echo "SELECT DISTINCT 'CA|SAVEON|00|00|00000|00000|'||'SAVEON'||'~' ||NVL(UPC,0) FROM ${EAL_DELTA_TAB};"

sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<ENDSQL  > /dev/null
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET SERVEROUTPUT ON;
SET HEADING OFF ECHO OFF FEEDBACK OFF PAGES 0  VERIFY OFF LINESIZE 110;
SPOOL ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_pretrim.dat
SELECT DISTINCT 'CA|SAVEON|00|00|00000|00000|'||'SAVEON'||'~' ||NVL(PADDED_UPC,0) FROM ${EAL_DELTA_TAB};
SPOOL OFF;
ENDSQL

testdberror $?

echo
echo "SPOOL : ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_pretrim.dat"
echo

sed '/^$/d' ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_pretrim.dat > ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}.dat
FILE_COUNT=`wc -l ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}.dat | cut -d " " -f1`

echo
echo "Record count in file ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}.dat : ${FILE_COUNT}"
echo

echo "Moving dat file to bulk key directory for bulk keying"
mv ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}.dat ${BULK_PATH}/SAVEON_ADHOC_ITEM_KEY_${FILE_DATE}.dat
echo " ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}.dat moved to ${BULK_PATH}/SAVEON_ADHOC_ITEM_KEY_${FILE_DATE}.dat"

echo "Creating ctrl file for bulk keying of ITEM DIM"
echo "ITEM_BULK_KEY
${EMAIL}
SAVEON_ADHOC_ITEM_KEY_${FILE_DATE}.dat
" > ${BULK_PATH}/SAVEON_ADHOC_ITEM_KEY_${FILE_DATE}.ctrl

if [ ${FILE_COUNT} -eq 0 ]
then
        echo
        echo "The File ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}.dat is empty. There are no records to be keyed."
        exit 0;
fi


echo "Invoke Item Bulk Keying process
Creating start file"

echo "touch ${BULK_PATH}/SAVEON_ADHOC_ITEM_KEY_${FILE_DATE}.start"

touch ${BULK_PATH}/SAVEON_ADHOC_ITEM_KEY_${FILE_DATE}.start

while [ -f ${BULK_PATH}/SAVEON_ADHOC_ITEM_KEY_${FILE_DATE}.dat ] ;
do
      sleep 60
done
#testfilenotfound /lddata/dim/prd/bulkkey/SOBEYS_ADHOC_ITEM_KEY_${FILE_DATE}.dat
####calling Item Keying script

echo
echo "The Process of Generating Keys from MDM is started @ `date`"

echo "sh /opt/ildbld/${ENV}/etl_util/ora_util/ora_spool_export.sh sv05mdmp bldload bdload4 ${ENV} ${SCRIPTDIR}/extract_item_keyed.sql ${DELTA_FILE_PATH} ${DELTA_FILE_LWR}_keyed.dat"

echo "sh /opt/ildbld/${ENV}/etl_util/ora_util/ora_spool_export.sh ${MDM_DB} ${MDM_USER} ${MDM_PASSWORD} ${ENV} ${SCRIPTDIR}/extract_item_keyed.sql ${DELTA_FILE_PATH} ${DELTA_FILE_LWR}_keyed.dat"

sh /opt/ildbld/${ENV}/etl_util/ora_util/ora_spool_export.sh sv05mdmp bldload bdload4 ${ENV} ${SCRIPTDIR}/extract_item_keyed.sql ${DELTA_FILE_PATH} ${DELTA_FILE_LWR}_keyed.dat

sleep 60

testfilenotfound ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_keyed.dat

if [ $? -ne 0 ]
then
   echo
 echo "Error in extracting item Keys"
exit -1;
fi


;;
"VN")

echo "SELECT DISTINCT UPPER(TRIM('CA')) || '|' || UPPER('SAVEON')  || '|' || UPPER('SAVEON') || '~' ||NVL(LOCATION_ID,0) FROM ${EAL_DELTA_TAB};
"


sqlplus -s $WH_POSTX_USER/$WH_POSTX_PASSWORD@$WH_POSTX_DATABASE<<EOF  > /dev/null
WHENEVER SQLERROR EXIT FAILURE;
WHENEVER OSERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET SERVEROUTPUT ON;
SET HEADING OFF ECHO OFF FEEDBACK OFF PAGES 0  VERIFY OFF LINESIZE 110;
SPOOL ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_pretrim.dat
SELECT DISTINCT UPPER(TRIM('CA')) || '|' || UPPER('SAVEON')  || '|' || UPPER('SAVEON') || '~' ||NVL(LOCATION_ID,0) FROM ${EAL_DELTA_TAB};
SPOOL OFF;
EOF

testdberror $?

sed '/^$/d' ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_pretrim.dat > ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}.dat

FILE_COUNT=`wc -l ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}.dat | cut -d " " -f1`

echo
echo "Record count in file ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}.dat : ${FILE_COUNT}"
echo

if [ ${FILE_COUNT} -eq 0 ]
then
        echo
        echo "The File ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}.dat is empty. There are no records to be keyed."
        exit 0;
fi

echo "Moving venue dat file to bulk key directory for bulk keying"
mv ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}.dat ${BULK_PATH}/SAVEON_ADHOC_VENUE_KEY_${FILE_DATE}.dat
echo " ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}.dat moved to ${BULK_PATH}/SAVEON_ADHOC_VENUE_KEY_${FILE_DATE}.dat"

echo "Creating ctrl file for bulk keying of VN DIM"

echo "VENUE_BULK_KEY
${EMAIL}
SAVEON_ADHOC_VENUE_KEY_${FILE_DATE}.dat
"
echo "VENUE_BULK_KEY
${EMAIL}
SAVEON_ADHOC_VENUE_KEY_${FILE_DATE}.dat
" > ${BULK_PATH}/SAVEON_ADHOC_VENUE_KEY_${FILE_DATE}.ctrl

echo "Invoke Venue Bulk Keying process
Creating start file"

echo "touch ${BULK_PATH}/SAVEON_ADHOC_VENUE_KEY_${FILE_DATE}.start"

touch ${BULK_PATH}/SAVEON_ADHOC_VENUE_KEY_${FILE_DATE}.start

echo  " Bulk keying process in progress please wait ......."
while [ -f ${BULK_PATH}/SAVEON_ADHOC_VENUE_KEY_${FILE_DATE}.dat ] ;
do
      sleep 60
done

echo " Bulk keying process completed"

#testfilenotfound  /lddata/dim/prd/bulkkey/SOBEYS_ADHOC_VENUE_KEY_${FILE_DATE}.dat

echo
echo "**********************************************************************************"
echo

### Calling Venue MDM Keying scripts

echo
echo "The Process of Generating Keys from MDM is started @ `date`"
echo "sh /opt/ildbld/${ENV}/etl_util/ora_util/ora_spool_export.sh sv05mdmp bldload bdload4 ${ENV} ${SCRIPTDIR}/extract_venue_keyed.sql ${DELTA_FILE_PATH} ${DELTA_FILE_LWR}_keyed.dat"

sh /opt/ildbld/${ENV}/etl_util/ora_util/ora_spool_export.sh ${MDM_DB} ${MDM_USER} ${MDM_PASSWORD} ${ENV} ${SCRIPTDIR}/extract_venue_keyed.sql ${DELTA_FILE_PATH} ${DELTA_FILE_LWR}_keyed.dat

testfilenotfound ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_keyed.dat

if [ $? -ne 0 ]
then
        echo
        echo "Error in extracting venue Keys"
        exit -1;
fi


;;
esac




echo
echo "The process of Generating Keys from MDM is completed @ `date`"
echo

echo
echo "**********************************************************************************"
echo

echo
echo "Loading the keyed file to table ${EAL_DELTA_TAB_KEYED} started @ `date`"
echo

echo
echo "${ETL_UTIL_PATH}/ora_util/ora_sqlldr.sh $WH_POSTX_DATABASE $WH_POSTX_USER $WH_POSTX_PASSWORD ${EAL_DELTA_TAB_KEYED} ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_keyed.dat true ${SCRIPTENV}"
echo


sh ${ETL_UTIL_PATH}/ora_util/ora_sqlldr.sh $WH_POSTX_DATABASE $WH_POSTX_USER $WH_POSTX_PASSWORD ${EAL_DELTA_TAB_KEYED} ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_keyed.dat true ${SCRIPTENV}

echo
echo "The keyed file has been loaded to table ${EAL_DELTA_TAB_KEYED} completed @ `date`"



echo
echo "***********************************************************************************"
echo
echo "Script execution completed @ `date`"

