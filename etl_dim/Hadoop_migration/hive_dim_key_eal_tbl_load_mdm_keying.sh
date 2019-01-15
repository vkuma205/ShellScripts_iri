et!/bin/bash

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

echo
echo "${JOB_STRT_TS}"
echo

###extracting the required metadata attributes for a specific dimension

#META_INFO=`sqlplus -s ${OPSBLD_USER}/${OPSBLD_PASSWD}@${OPSBLD_DB}<<ENDSQL
#SET HEADING OFF ECHO OFF FEEDBACK OFF PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 500;
#WHENEVER SQLERROR EXIT 1
#SELECT CLIENT || '|' || DIM_NAME || '|' || DIM_ABBR || '|' || FILE_NAME_PREFIX || '|' || UPPER(CNTRY_CD) || '|' || SURROGATE_KEY FROM ${KEY_DEFN_TAB} WHERE 
#CONTAINER_ID=${CONTAINER_ID};
#ENDSQL`

#testdberror $?

META_INFO=`hive -e " use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; select client,'|',dim_name,'|',dim_abbr,'|',file_name_prefix,'|',cntry_cd,'|',surrogate_key FROM ${HIVE_DB}.${KEY_DEFN_TAB} WHERE CONTAINER_ID=${CONTAINER_ID};"`

echo
echo "META_INFO:$META_INFO"
echo


if [ -z "${META_INFO}" ]
then
        echo
        echo " Error in fetching META_INFO from table ${HIVE_DB}.${KEY_DEFN_TAB} for container id ${CONTAINER_ID}"
        exit -1;
fi


CLIENT_LWR=`echo ${CLIENT} | awk '{print tolower($0)}'`
CLIENT_UPR=`echo ${CLIENT} | awk '{print toupper($0)}'`

FILE_NAME_PREFIX_TAB=`echo ${META_INFO} | cut -d'|' -f4`
FILE_NAME_PREFIX=`echo ${FILE_NAME_PREFIX_TAB} | sed 's/ //g'`

#FILE_NAME_PREFIX=`echo ${META_INFO} | cut -d'|' -f4`
DIM_NAME=`echo ${META_INFO} | cut -d'|' -f2`
DIM_ABBR=`echo ${META_INFO} | cut -d'|' -f3`
CNTRY_CD=`echo ${META_INFO} | cut -d'|' -f5`

#SURROGATE_KEY=`echo ${META_INFO} | cut -d'|' -f6`
SURROGATE_KEY_TAB=`echo ${META_INFO} | cut -d'|' -f6`
SURROGATE_KEY=`echo ${SURROGATE_KEY_TAB} | sed 's/ //g'`

DIM_NAME_LWR=`echo ${DIM_NAME} | awk '{print tolower($0)}'`
DIM_NAME_UPR=`echo ${DIM_NAME} | awk '{print toupper($0)}'`
DIM_ABBR_LWR=`echo ${DIM_ABBR} | awk '{print tolower($0)}'`
DIM_ABBR_UPR=`echo ${DIM_ABBR} | awk '{print toupper($0)}'`

STG_TAB=${CLIENT_LWR}_${DIM_ABBR_LWR}_stg
RAW_TAB=${CLIENT_LWR}_${DIM_ABBR_LWR}_ext

EAL_STG_TAB=eal_dk_${CLIENT_LWR}_${DIM_ABBR_LWR}_stg
EAL_MSTR_TAB=eal_dk_${CLIENT_LWR}_${DIM_ABBR_LWR}_mstr
EAL_DELTA_TAB=eal_dk_${CLIENT_LWR}_${DIM_ABBR_LWR}_delta


DELTA_FILE_PATH=${LOGDIR}
DELTA_FILE=${CLIENT_LWR}_${DIM_NAME_LWR}_${FILE_DATE}_${JOB_STRT_TS}
DELTA_FILE_LWR=`echo ${DELTA_FILE} | awk '{print tolower($0)}'`
EAL_DELTA_TAB_KEYED=eal_${CLIENT_LWR}_${DIM_ABBR_LWR}_dlt_kyd

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
#TAB_DEFN_TEMP=`sqlplus -s ${OPSBLD_USER}/${OPSBLD_PASSWD}@${OPSBLD_DB}<<ENDSQL
#set heading off echo off verify off feed off termout off pages 0 linesize 10000  long 3000 trims on trim on
#SELECT REPLACE(STG_COLUMN_META,' ','@') FROM ${KEY_DEFN_TAB} WHERE CONTAINER_ID=${CONTAINER_ID};
#ENDSQL`

#testdberror $?

#TAB_DEFN_TEMP=`echo ${TAB_DEFN_TEMP} | sed 's/ //g'`
#TAB_DEFN=`echo ${TAB_DEFN_TEMP} | sed 's/@/ /g'`

TAB_DEFN=`hive -e " use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;SELECT STG_COLUMN_META FROM ${HIVE_DB}.${KEY_DEFN_TAB} WHERE CONTAINER_ID=${CONTAINER_ID};"`


echo
echo "TAB_DEFN : ${TAB_DEFN}"
echo "*********************************************************************************"
#echo "TAB_DEFN_TEMP : ${TAB_DEFN_TEMP}"
echo "**********************************************************************************"
echo


echo
echo "DROP TABLE: ${HIVE_DB}.${EAL_STG_TAB}"
echo

hive -e " use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;DROP TABLE IF EXISTS ${HIVE_DB}.${EAL_STG_TAB};"

echo
echo "CREATING TABLE: ${HIVE_DB}.${EAL_STG_TAB}"
echo

hive -e " use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; CREATE TABLE IF NOT EXISTS ${HIVE_DB}.${EAL_STG_TAB} AS SELECT CAST('123456789' AS INT) ${DIM_ABBR_LWR}_dim_key, A.* FROM ${HIVE_DB}.${STG_TAB} A WHERE 1=2;"
#"SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; INSERT OVERWRITE TABLE ${HIVE_DB}.${EAL_STG_TAB} SELECT CAST('123456789' AS INT) ${DIM_ABBR_LWR}_dim_key, A.* FROM ${HIVE_DB}.${STG_TAB} A;"

if [ $? -ne 0 ]
then
   echo "Error while creating the table: ${EAL_STG_TAB} in Hive DB: ${HIVE_DB}. Aborting the program."
   echo
   exit 255
fi


echo "**********************************************************************************"


echo
#echo "DROPPING TABLE: ${HIVE_DB}.${EAL_MSTR_TAB}"
echo

#hive -e " use ${HIVE_DB};DROP TABLE IF EXISTS ${HIVE_DB}.${EAL_MSTR_TAB}"

echo
echo "CREATING TABLE: ${HIVE_DB}.${EAL_MSTR_TAB}"
echo


hive -e " use ${HIVE_DB};CREATE TABLE IF NOT EXISTS ${HIVE_DB}.${EAL_MSTR_TAB} (${SURROGATE_KEY} int, ${TAB_DEFN} ) CLUSTERED BY (${SURROGATE_KEY}) into 1 buckets stored as orc tblproperties('transactional'='true','orc.compress'='ZLIB');"

if [ $? -ne 0 ]
then
   echo "Error while creating the table: ${EAL_MSTR_TAB} in Hive DB: ${HIVE_DB}. Aborting the program."
   echo
   exit 255
fi


echo "**********************************************************************************"

echo
echo "DROP TABLE: ${HIVE_DB}.${EAL_DELTA_TAB}"
echo

hive -e " use ${HIVE_DB};DROP TABLE IF EXISTS ${HIVE_DB}.${EAL_DELTA_TAB};"

echo
echo "CREATING THE DELTA TABLE: ${EAL_DELTA_TAB}"
echo

hive -e " use ${HIVE_DB};CREATE TABLE IF NOT EXISTS ${HIVE_DB}.${EAL_DELTA_TAB} (${SURROGATE_KEY} int, ${TAB_DEFN} ) CLUSTERED BY (${SURROGATE_KEY}) into 1 buckets stored as orc tblproperties('transactional'='true','orc.compress'='ZLIB');"

if [ $? -ne 0 ]
then
   echo "Error while creating the table: ${EAL_DELTA_TAB} in Hive DB: ${HIVE_DB}. Aborting the program."
   echo
   exit 255
fi

echo
echo "*********************************************************************************"
echo

### Checks for the Natural Key Existance in the metadata table
#NATURAL_KEY_LIST=`sqlplus -s ${OPSBLD_USER}/${OPSBLD_PASSWD}@${OPSBLD_DB}<<EOF
#set echo off verify off feed off termout off pages 0 lines 500
#SELECT REPLACE(NATURAL_KEY, '|',' || ''~'' || ') FROM ${KEY_DEFN_TAB} WHERE CONTAINER_ID=${CONTAINER_ID};
#EOF`

NATURAL_KEY_LIST=`hive -e " use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;SELECT NATURAL_KEY FROM ${HIVE_DB}.${KEY_DEFN_TAB} WHERE CONTAINER_ID=${CONTAINER_ID};"`

#SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; select NATURAL_KEY from meta_key_definition where CONTAINER_ID=4145;
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

FACT_SELECT_COLUMNS=`hive -S -e "SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;SHOW COLUMNS IN  ${HIVE_DB}.${STG_TAB};" > ${SCRIPTDIR}/headers10.txt`
FACT_SELECT=`sed '$!s/$/,/' ${SCRIPTDIR}/headers10.txt`

if [ -z "${FACT_SELECT}" ]
then
        echo
        echo "FACT_SELECT not fetched properly"
        exit -1;
fi

### Extracting the list of Attributes without file date

FACT_SELECT_TAB_PREFIX_COLUMNS=`hive -e " use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;SHOW COLUMNS IN ${HIVE_DB}.${EAL_DELTA_TAB};" | sed s/it_dim_key//g > ${SCRIPTDIR}/headers11.txt`
FACT_SELECT_TAB_PREFIX=`sed '$!s/$/,/' ${SCRIPTDIR}/headers11.txt`



if [ -z "${FACT_SELECT_TAB_PREFIX}" ]
then
        echo
        echo "FACT_SELECT_TAB_PREFIX not fetched properly"
        exit -1;
fi

FACT_SELECT_CONCAT=`echo ${FACT_SELECT} | sed 's/,/ || '\''|'\'' ||/g'`

echo 
echo "FACT_SELECT RESULTS: $FACT_SELECT"
echo "FACT_SELECT_TAB_PREFIX RESULTS: $FACT_SELECT_TAB_PREFIX"
echo "FACT_SELECT_CONCAT RESULTS: $FACT_SELECT_CONCAT"
echo 


### Inserting data to EAL Staging table
echo "................Adding missing dimensions from fact file...................."

if [ ${DIM_ABBR_LWR} = "it" ]
then

echo
echo " Adding missing records in ITEM Dimension from fact file"
echo


echo "SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; DELETE FROM ${HIVE_DB}.${SAVEON_IT_ALL_KEYS} WHERE UPC IN ( select UPC  from ${HIVE_DB}.${EAL_MSTR_TAB});
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; DELETE FROM ${HIVE_DB}.${SAVEON_IT_ALL_KEYS} WHERE UPC IN ( select UPC from ${HIVE_DB}.${STG_TAB});

SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; INSERT INTO ${HIVE_DB}.${STG_TAB} (UPC,ITEM_ID,RETAIL_ITEM_CD,ITEM_DESC,LEAD_ITEM_RETAIL_CD,PADDED_UPC,PADDED_RETAIL_ITEM_CD) SELECT UPC,0,0,'UNKNOWN PRODUCT FROM FACT',0,LPAD( CAST(UPC AS string), 14,'0') PADDED_UPC,LPAD( CAST(0 AS string), 7,'0') PADDED_RETAIL_ITEM_CD from ${HIVE_DB}.{SAVEON_IT_ALL_KEYS};"


hive -e " use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; DELETE FROM ${HIVE_DB}.${SAVEON_IT_ALL_KEYS} WHERE UPC IN ( select UPC  from ${HIVE_DB}.${EAL_MSTR_TAB});

SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; DELETE FROM ${HIVE_DB}.${SAVEON_IT_ALL_KEYS} WHERE UPC IN ( select UPC from ${HIVE_DB}.${STG_TAB});

SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; INSERT INTO ${HIVE_DB}.${STG_TAB} (UPC,ITEM_ID,RETAIL_ITEM_CD,ITEM_DESC,LEAD_ITEM_RETAIL_CD,PADDED_UPC,PADDED_RETAIL_ITEM_CD) SELECT UPC,0,0,'UNKNOWN PRODUCT FROM FACT',0,LPAD( CAST(UPC AS string), 14,'0') PADDED_UPC,LPAD( CAST(0 AS string), 7,'0') PADDED_RETAIL_ITEM_CD from ${HIVE_DB}.${SAVEON_IT_ALL_KEYS};"

echo "******************************************************"

VALIDATE_TABLE=`hive -e " use ${HIVE_DB};USE $HIVE_DB; SHOW TABLES LIKE '$SAVEON_IT_ALL_KEYS'"`

if [ -z "${VALIDATE_TABLE}" ]
then
     echo
     echo "$SAVEON_IT_ALL_KEYS does not exist"
     exit -1;
fi

else

echo
echo " Adding missing records in VENUE Dimension from fact file"
echo
echo "

"



echo " use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; DELETE FROM ${SAVEON_VN_ALL_KEYS} WHERE LOCATION_ID IN ( select cast(LOCATION_ID as varchar(10))  from ${EAL_MSTR_TAB});
COMMIT;

SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; DELETE FROM ${SAVEON_VN_ALL_KEYS} WHERE LOCATION_ID IN ( select cast(LOCATION_ID as varchar(10))  from $STG_TAB);
COMMIT;

SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; INSERT INTO $STG_TAB (OPENING_DATE,CLOSING_DATE,LOCATION_DESC,ADDRESS_LINE_1,ADDRESS_LINE_2,TOWN_CODE,PROVINCE_CODE,POSTAL_CODE,DIVISION_DESC,RM_REGION_ID,PRICING_ZONE_ID,LOCATION_ID) SELECT 'UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','-1','-1',LOCATION_ID FROM ${SAVEON_VN_ALL_KEYS};
"

hive -e " use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; DELETE FROM ${HIVE_DB}.${SAVEON_VN_ALL_KEYS} WHERE LOCATION_ID IN ( select LOCATION_ID from ${HIVE_DB}.${EAL_MSTR_TAB});
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; DELETE FROM ${HIVE_DB}.${SAVEON_VN_ALL_KEYS} WHERE LOCATION_ID IN ( select LOCATION_ID from ${HIVE_DB}.$STG_TAB);

SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; INSERT INTO ${HIVE_DB}.${STG_TAB} (OPENING_DATE,CLOSING_DATE,LOCATION_DESC,ADDRESS_LINE_1,ADDRESS_LINE_2,TOWN_CODE,PROVINCE_CODE,POSTAL_CODE,DIVISION_DESC,RM_REGION_ID,PRICING_ZONE_ID,LOCATION_ID) SELECT 'UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','-1','-1',LOCATION_ID FROM ${HIVE_DB}.${SAVEON_VN_ALL_KEYS};"

fi

echo
echo "******************************** EAL STG TBL inserting QRY ************************************"
echo

echo "TRUNCATE TABLE ${EAL_STG_TAB}; 
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; INSERT INTO ${EAL_STG_TAB} ${FACT_SELECT} SELECT DISTINCT $FACT_SELECT FROM $STG_TAB; 
" > ${GENDIR}/ora_dim_key_load_eal_stage_${JOB_STRT_TS}.sql

echo
echo "*********************************TRUNCATATE TABLE: ${EAL_STG_TAB}****************************"
echo

hive -e " use ${HIVE_DB}; SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;TRUNCATE TABLE ${HIVE_DB}.${EAL_STG_TAB};"

echo
echo "SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; INSERT INTO TABLE: ${EAL_STG_TAB}*********************************************"
echo

if [ ${DIM_ABBR_LWR} = "it" ]

then

echo "Item Processing"

#hive -e " use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; INSERT INTO ${HIVE_DB}.${EAL_STG_TAB}  SELECT DISTINCT NULL,A.* FROM ${HIVE_DB}.${STG_TAB} A;"

hive -e " use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; INSERT INTO ${HIVE_DB}.${EAL_STG_TAB} SELECT DISTINCT NULL,ITEM_ID,RETAIL_ITEM_CD,ITEM_STATUS_CD,AP_VENDOR_ID,ITEM_DESC,DEPT_ID,DEPT_DESC,CLASS_ID,CLASS_DESC,CAT_ID,CAT_DESC,MERCHANDISER_NUMBER,MERCHANDISER_NM,RETAIL_METRIC,RETAIL_PACK,RETAIL_SIZE,MERCH_GROUP,FAMILY_CD,LEAD_ITEM_RETAIL_CD,BRAND_NM, PADDED_UPC,PADDED_RETAIL_ITEM_CD,UPC,AP_VENDOR_DESCRIPTION,HIGH_LEVEL_DEPARTMENTS,FAMILY_CD_DESC,IRI_BRAND_DESC,IRI_VENDOR_DESC,FIRST_SCAN_DATE,LAST_SCAN_DATE,FILE_DATE FROM ${HIVE_DB}.${STG_TAB};"

else

echo " Venue Processing"

hive -e " use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; INSERT INTO ${HIVE_DB}.${EAL_STG_TAB} SELECT DISTINCT NULL,sx,storename,location_id,opening_date,closing_date,location_desc,address_line_1,address_line_2,town_code,province_code,postal_code,division_desc,rm_region_id,pricing_zone_id,location_name,file_date from ${HIVE_DB}.${STG_TAB};"

fi

echo
echo "**********************************************************************************"
echo

#here considered  FACT_SELECT_TAB_PREFIX's attribute list to ignore filedate and  pulled out the delta records

echo " SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; INSERT INTO EAL_DELTA_TAB ( FACT_SELECT_TAB_PREFIX ) SELECT DISTINCT FACT_SELECT_TAB_PREFIX FROM STG_TAB;"


echo
echo "****************TRUNCATE TABLE: ${EAL_DELTA_TAB}*****************"
echo

hive -e " use ${HIVE_DB}; TRUNCATE TABLE ${HIVE_DB}.${EAL_DELTA_TAB};"

echo
echo "use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; INSERT INTO ${EAL_DELTA_TAB} SELECT DISTINCT NULL, item_id,retail_item_cd,item_status_cd,ap_vendor_id,item_desc,dept_id,dept_desc,class_id,class_desc,cat_id,cat_desc,merchandiser_number,merchandiser_nm,retail_metric,retail_pack,retail_size,merch_group,family_cd,lead_item_retail_cd,brand_nm,padded_upc,padded_retail_item_cd,upc,ap_vendor_description,high_level_departments,family_cd_desc,iri_brand_desc,iri_vendor_desc,first_scan_date,last_scan_date FROM ${HIVE_DB}.${STG_TAB};"
echo

#hive -e " use ${HIVE_DB}; SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; INSERT INTO ${HIVE_DB}.${EAL_DELTA_TAB}  SELECT DISTINCT NULL ,A.* FROM ( SELECT ${FACT_SELECT_TAB_PREFIX} FROM ${HIVE_DB}.${STG_TAB}) A ;"

if [ ${DIM_ABBR_LWR} = "it" ]

then 

hive -e " use ${HIVE_DB}; SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; INSERT INTO ${HIVE_DB}.${EAL_DELTA_TAB} SELECT DISTINCT NULL, item_id,retail_item_cd,item_status_cd,ap_vendor_id,item_desc,dept_id,dept_desc,class_id,class_desc,cat_id,cat_desc,merchandiser_number,merchandiser_nm,retail_metric,retail_pack,retail_size,merch_group,family_cd,lead_item_retail_cd,brand_nm,padded_upc,padded_retail_item_cd,upc,ap_vendor_description,high_level_departments,family_cd_desc,iri_brand_desc,iri_vendor_desc,first_scan_date,last_scan_date FROM ${HIVE_DB}.${STG_TAB};"

else

hive -e "use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; INSERT INTO ${HIVE_DB}.${EAL_DELTA_TAB} SELECT DISTINCT NULL, sx,storename,location_id,opening_date,closing_date,location_desc,address_line_1,address_line_2,town_code,province_code,postal_code,division_desc,rm_region_id,pricing_zone_id,location_name from ${HIVE_DB}.${STG_TAB};"

fi


case $DIM_ABBR_LWR in 

###ITEM
"it")

        echo " CREATE TABLE IF NOT EXISTS ${HIVE_DB}.${EAL_DELTA_TAB_KEYED} (
                ${SURROGATE_KEY} STRING,
                SYSTEM_CODE_NUM STRING,
                VENDOR_CODE_NUM STRING,
                ITEM_CODE_NUM STRING,
                GENERATION_CODE_NUM STRING,      
                COUNTRY_CODE STRING,
                SOURCE_ID STRING,
                CLIENT_ID STRING,
                ALT_KEY_TXT STRING
                )
                ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
                STORED AS TEXTFILE;"
				

echo
echo "Drop Table: ${HIVE_DB}.${EAL_DELTA_TAB_KEYED}"				
echo

hive -e "use ${HIVE_DB};DROP TABLE IF EXISTS ${HIVE_DB}.${EAL_DELTA_TAB_KEYED};"

echo
echo "Creating Table: ${EAL_DELTA_TAB_KEYED}"
echo

hive -e "use ${HIVE_DB};CREATE TABLE IF NOT EXISTS ${HIVE_DB}.${EAL_DELTA_TAB_KEYED} (
                ${SURROGATE_KEY} string,
                SYSTEM_CODE_NUM string,
                VENDOR_CODE_NUM string,
                ITEM_CODE_NUM string,
                GENERATION_CODE_NUM string,
                COUNTRY_CODE string,
                SOURCE_ID string,
                CLIENT_ID string,
                ALT_KEY_TXT string
                )
                ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
                STORED AS TEXTFILE;"


;;
###Venue
"vn")

         echo "CREATE TABLE IF NOT EXISTS ${HIVE_DB}.${EAL_DELTA_TAB_KEYED} (
                ${SURROGATE_KEY} STRING,
                IRI_STORE_NUMBER STRING,
                LEGACY_SEQ_KEY STRING,
                ADDRESS_ID STRING,
                COUNTRY_CODE STRING,
                SOURCE_ID STRING,
                CLIENT_ID STRING,
                ALT_KEY_TXT STRING)
                ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
                STORED AS TEXTFILE;"



echo
echo "Drop Table: ${HIVE_DB}.${EAL_DELTA_TAB_KEYED}"
echo

hive -e "use ${HIVE_DB};DROP TABLE IF EXISTS ${HIVE_DB}.${EAL_DELTA_TAB_KEYED}"


echo
echo "Creating Table: ${EAL_DELTA_TAB_KEYED}"
echo


hive -e "use ${HIVE_DB};CREATE TABLE IF NOT EXISTS ${HIVE_DB}.${EAL_DELTA_TAB_KEYED}(
                ${SURROGATE_KEY} STRING,
                IRI_STORE_NUMBER STRING,
                LEGACY_SEQ_KEY STRING,
                ADDRESS_ID STRING,
                COUNTRY_CODE STRING,
                SOURCE_ID STRING,
                CLIENT_ID STRING,
                ALT_KEY_TXT STRING)
                ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
                STORED AS TEXTFILE;"



;;
esac

echo "************************************************************************************************************"
echo "Initializing Tables used in EAL Process"
echo "************************************************************************************************************"

#sh ${SCRIPTDIR}/initialize_mdm_schema_tables.sh ${CONFIG_FILE}
#sh ${SCRIPTDIR}/initialize_non_mdm_schema_tables.sh ${CONFIG_FILE}

echo
echo "**********************************************************************************"
echo

echo
echo "creating text table"
echo


case $DIM_ABBR_LWR in 
"it") 

echo
echo "SELECT DISTINCT 'CA|SAVEON|00|00|00000|00000|'||'SAVEON'||'~' ||NVL(UPC,0) FROM ${EAL_DELTA_TAB};"
echo


echo
echo "Creating the PreTrim.dat file..."
echo

touch ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_pretrim.dat

hive -e " use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;SELECT DISTINCT CONCAT('CA|SAVEON|00|00|00000|00000|','SAVEON','~',NVL(UPC,0)) FROM ${HIVE_DB}.${EAL_DELTA_TAB};" > ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_pretrim.dat

echo
echo "*********************************************"
echo "Moved Data To : ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_pretrim.dat"
echo "*********************************************"
echo

sed '/^$/d' ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_pretrim.dat > ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}.dat
FILE_COUNT=`wc -l ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}.dat | cut -d " " -f1`

echo
echo "Record count in file ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}.dat : ${FILE_COUNT}"
echo

echo
echo "Moving dat file to bulk key directory for bulk keying"
echo

mv ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}.dat ${BULK_PATH}/SAVEON_ADHOC_ITEM_KEY_${FILE_DATE}.dat

echo
echo " ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}.dat moved to ${BULK_PATH}/SAVEON_ADHOC_ITEM_KEY_${FILE_DATE}.dat"
echo

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
echo


echo "sh /opt/ildbld/${ENV}/etl_util/ora_util/ora_spool_export.sh sv05mdmp bldload bdload4 ${ENV} ${SCRIPTDIR}/extract_item_keyed.sql ${DELTA_FILE_PATH} ${DELTA_FILE_LWR}_keyed.dat"


echo "sh /opt/ildbld/${ENV}/etl_util/ora_util/ora_spool_export.sh ${MDM_DB} ${MDM_USER} ${MDM_PASSWORD} ${ENV} ${SCRIPTDIR}/extract_item_keyed.sql ${DELTA_FILE_PATH} ${DELTA_FILE_LWR}_keyed.dat"


sh /opt/ildbld/${ENV}/etl_util/ora_util/ora_spool_export.sh ${MDM_DB} ${MDM_USER} ${MDM_PASSWORD} ${ENV} ${SCRIPTDIR}/extract_item_keyed.sql ${DELTA_FILE_PATH} ${DELTA_FILE_LWR}_keyed.dat

sleep 60

testfilenotfound ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_keyed.dat

if [ $? -ne 0 ]
then
   echo
 echo "Error in extracting item Keys"
exit -1;
fi


;;
"vn")

echo "SELECT DISTINCT CONCAT (TRIM('CA'),'|','SAVEON','|','SAVEON','~',NVL(LOCATION_ID,0)) FROM ${HIVE_DB}.${EAL_DELTA_TAB};"


echo
echo "Creating PreTrim.dat file for Venue Dimension"
echo

touch ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_pretrim.dat

hive -e " use ${HIVE_DB};SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;SELECT DISTINCT CONCAT (TRIM('CA'),'|','SAVEON','|','SAVEON','~',NVL(LOCATION_ID,0)) FROM ${HIVE_DB}.${EAL_DELTA_TAB};" > ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_pretrim.dat

echo
echo "*********************************************"
echo "Moved Data To : ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_pretrim.dat"
echo "*********************************************"
echo


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

echo
echo " Bulk keying process completed"
echo

#testfilenotfound  /lddata/dim/prd/bulkkey/SOBEYS_ADHOC_VENUE_KEY_${FILE_DATE}.dat

echo
echo "**********************************************************************************"
echo

### Calling Venue MDM Keying scripts

echo
echo "The Process of Generating Keys from MDM is started @ `date`"
echo "sh /opt/ildbld/${ENV}/etl_util/ora_util/ora_spool_export.sh sv05mdmp bldload bdload4 ${ENV} ${SCRIPTDIR}/extract_venue_keyed.sql ${DELTA_FILE_PATH} ${DELTA_FILE_LWR}_keyed.dat"
echo

sh /opt/ildbld/${ENV}/etl_util/ora_util/ora_spool_export.sh ${MDM_DB} ${MDM_USER} ${MDM_PASSWORD} ${ENV} ${SCRIPTDIR}/extract_venue_keyed.sql ${DELTA_FILE_PATH} ${DELTA_FILE_LWR}_keyed.dat

sleep 60

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


hive -e " use ${HIVE_DB};LOAD DATA LOCAL INPATH '${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_keyed.dat' OVERWRITE INTO TABLE ${HIVE_DB}.${EAL_DELTA_TAB_KEYED};"

echo
echo "LOAD DATA LOCAL INPATH '${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_keyed.dat' OVERWRITE INTO TABLE ${HIVE_DB}.${EAL_DELTA_TAB_KEYED};"
echo
#echo
#echo "${ETL_UTIL_PATH}/ora_util/ora_sqlldr.sh $WH_POSTX_DATABASE $WH_POSTX_USER $WH_POSTX_PASSWORD ${EAL_DELTA_TAB_KEYED} ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_keyed.dat true ${SCRIPTENV}"
#echo


#sh ${ETL_UTIL_PATH}/ora_util/ora_sqlldr.sh $WH_POSTX_DATABASE $WH_POSTX_USER $WH_POSTX_PASSWORD ${EAL_DELTA_TAB_KEYED} ${DELTA_FILE_PATH}/${DELTA_FILE_LWR}_keyed.dat true ${SCRIPTENV}

echo
echo "The keyed file has been loaded to table ${EAL_DELTA_TAB_KEYED} completed @ `date`"
echo


echo
echo "***********************************************************************************"
echo
echo "Script execution completed @ `date`"

