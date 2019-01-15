#!/bin/ksh
########################################################################################################################
#
# Purpose: Loading various Saveon transaction related file to Hive for fact Loading
#
# Change History Record:
#
# ======================================================================================================================
# DATE        |   AUTHOR NAME        |    CHANGE DESCRIPTION | JIRA_ID
# ======================================================================================================================
# 2018-04-10 |   Siddharth Sahu      |    New script created |
# 2018-08-20 |  Shibashish Tripathy  |Modified : new parameters added and sqoop job created |

 ----------------------------------------------------------------------------------------------------------------------
########################################################################################################################

echo
echo "**********************************************************************************"
echo
echo "Job name - $0"
echo
echo "Number of Parameters - $#"
echo

#Checks the number of Parameters passed
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
        echo "Usage: sh $0 <CONFIG_FILE>"
        echo "***************************************************************************"
        echo ""
        exit 1
fi

echo
echo "Script $0 execution started @ `date`"
echo



CONFIG_FILE=${1}

. ${CONFIG_FILE}

TRANS_SRC_FILE=${SOURCE_DIR}/${TRANS_FILE}
PRODUCT_SRC_FILE=${SOURCE_DIR}/${PRODUCT_FILE}
BASKET_SUMMARY_SRC_FILE=${SOURCE_DIR}/${BASKET_SUMMARY_FILE}
ITEM_MOVE_SRC_FILE=${SOURCE_DIR}/${ITEM_MOVE_FILE}
TENDER_SRC_FILE=${SOURCE_DIR}/${TENDER_FILE}
storesIn_SRC_FILE=${SOURCE_DIR}/${STORESIN_FILE}
SHRINK_SRC_FILE=${SOURCE_DIR}/${SHRINK_FILE}
FRESON_SRC_FILE=${SOURCE_DIR}/${FRESON_FILE}
POINTS_SRC_FILE=${SOURCE_DIR}/${POINTS_FILE}
STORE_MAPPING_SRC_FILE=${SOURCE_DIR}/${STORE_MAPPING_FILE}
PLANOGRAM_ITEM_SRC_FILE=${SOURCE_DIR}/${PLANOGRAM_ITEM_FILE}
STORE_PLANOGRAM_SRC_FILE=${SOURCE_DIR}/${STORE_PLANOGRAM_FILE}
PRODUCT_PLANOGRAM_SRC_FILE=${SOURCE_DIR}/${PRODUCT_PLANOGRAM_FILE}
PRODUCTCODE_PLANOGRAM_SRC_FILE=${SOURCE_DIR}/${PRODUCTCODE_PLANOGRAM_FILE}
FORECAST_SRC_FILE=${SOURCE_DIR}/${FORECAST_FILE}
INVENTORY_SRC_FILE=${SOURCE_DIR}/${INVENTORY_FILE}
PURCHASE_SRC_FILE=${SOURCE_DIR}/${PURCHASE_FILE}
TRANS_SRC_STG_TAB=trans_src_stg
PRODUCT_SRC_STG_TAB=product_src_stg
BASKET_SRC_STG_TAB=basket_src_stg
ITEM_MOVE_SRC_STG_TAB=item_move_src_stg
TENDER_SRC_STG_TAB=tender_src_stg
storesIn_SRC_STG_TAB=storesIn_src_stg
FACT_SRC_SHRINK_RAW=OFG_SHRINK
SHRINK_SRC_STG_TAB=shrink_src_stg
POINT_SRC_STG_TAB=point_src_stg
FORECAST_SRC_STG_TAB=forecast_src_stg
INVENTORY_SRC_STG_TAB=inventory_src_stg
PURCHASE_SRC_STG_TAB=purchase_src_stg
STORE_MAPPING_TAB=store_mapping
SAVEON_PL_ITEM_TAB=saveon_pl_item
SAVEON_PL_STORE_TAB=saveon_pl_store
SAVEON_PL_PRODUCT_TAB=saveon_pl_product
SAVEON_PRODUCTCODE_SRC_TAB=saveon_productcode_src
FRESON_SRC_STG_TAB=freson_src_stg

echo "Unzipping any zipped files in source directory"
gunzip -f ${SOURCE_DIR}/*.gz
echo "Unzipping done"

echo "* Cleanup - Moving old files in ${RAW_DIR} to ${ARCHIVE_DIR} *"
mv ${RAW_DIR}/* ${ARCHIVE_DIR}


echo "Creating staging tables to load the input files if not exists in Hive DB."
echo

hive -e "CREATE TABLE IF NOT EXISTS ${HIVE_DB}.${TRANS_SRC_STG_TAB}
(
   dayofpurchase     STRING,
   transactionnumber STRING,
   productcode       BIGINT,
   spending          STRING,
   quantity          STRING,
   weight            STRING,
   discount          STRING,
   poscode           STRING,
   sx                STRING,
   cx                STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;"

if [ $? -ne 0 ]
   then
      echo "Error while creating the table: ${TRANS_SRC_STG_TAB} in Hive DB: ${HIVE_DB}. Aborting the program."
      echo
      exit 255
fi

#hive -e "CREATE TABLE IF NOT EXISTS ${HIVE_DB}.${SHRINK_SRC_STG_TAB}


hive -e "CREATE TABLE IF NOT EXISTS ${HIVE_DB}.${PRODUCT_SRC_STG_TAB}
(
   px             STRING,
   productcode    BIGINT,
   description    STRING,
   familycode     STRING,
   lastupdatedate STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;"

if [ $? -ne 0 ]
then
   echo "Error while creating the table: ${PRODUCT_SRC_STG_TAB} in Hive DB: ${HIVE_DB}. Aborting the program."
   echo
   exit 255
fi

hive -e "CREATE TABLE IF NOT EXISTS ${HIVE_DB}.${BASKET_SRC_STG_TAB}
(
   dayofpurchase_dt  STRING,
   transactionnumber STRING,
   dayofpurchase     STRING,
   fiscaldate        STRING,
   storenumber       STRING,
   cx                STRING,
   registerline      STRING,
   cashier           STRING,
   spending          STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;"

if [ $? -ne 0 ]
then
   echo "Error while creating the table: ${BASKET_SRC_STG_TAB} in Hive DB: ${HIVE_DB}. Aborting the program."
   echo
   exit 255
fi

hive -e "CREATE TABLE IF NOT EXISTS ${HIVE_DB}.${ITEM_MOVE_SRC_STG_TAB}
(
   store_number         STRING,
   item_id              STRING,
   scan_code            BIGINT,
   sale_date            STRING,
   total_sales_dollars  STRING,
   total_sales_units    STRING,
   points_awarded       STRING,
   points_redeemed      STRING,
   scan_weight          STRING,
   retail_metric        STRING,
   landed_branch_cost   STRING,
   own_brand_ind        STRING,
   brand                STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;"

if [ $? -ne 0 ]
then
   echo "Error while creating the table: ${ITEM_MOVE_SRC_STG_TAB} in Hive DB: ${HIVE_DB}. Aborting the program."
   echo
   exit 255
fi

hive -e "CREATE TABLE IF NOT EXISTS ${HIVE_DB}.${TENDER_SRC_STG_TAB}
(
   dayofpurchase     STRING,
   transactionnumber STRING,
   spending          STRING,
   poscode           STRING,
   sx                STRING,
   cx                STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;"

if [ $? -ne 0 ]
then
   echo "Error while creating the table: ${TENDER_SRC_STG_TAB} in Hive DB: ${HIVE_DB}. Aborting the program."
   echo
   exit 255
fi

hive -e "CREATE TABLE IF NOT EXISTS ${HIVE_DB}.${storesIn_SRC_STG_TAB}
(
   sx    STRING,
   storename   STRING,
   storenumber STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;"

if [ $? -ne 0 ]
then
   echo "Error while creating the table: ${storesIn_SRC_STG_TAB} in Hive DB: ${HIVE_DB}. Aborting the program."
   echo
   exit 255
fi

hive -e "CREATE TABLE IF NOT EXISTS ${HIVE_DB}.${FORECAST_SRC_STG_TAB}
(
  Target_Week STRING,
  Retail_Item_CD STRING,
  Location_ID STRING,
  Base_sales_qty STRING,
  Fsct_Lift STRING,
  Fcst_Manual_Qty_Lift STRING,
  warehouseid STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;"

if [ $? -ne 0 ]
then
   echo "Error while creating the table: ${FORECAST_SRC_STG_TAB} in Hive DB: ${HIVE_DB}. Aborting the program."
   echo
   exit 255
fi

echo "All the staging tables are available in Hive DB to load the data."

hive_truncate_table()
{
hive -e "TRUNCATE TABLE $1.$2;"

if [ $? -ne 0 ]
then
   echo "Error while truncating the table: $2 in Hive DB: $1. Aborting the program."
   echo
   exit 255
else
   echo "Table $1.$2 truncated."
   echo
fi
}

hive_table_load()
{
for in_file in $1*
do 
   echo "Loading file: $in_file into table $2.$3."
   echo
   
   hive -e "LOAD DATA LOCAL INPATH '$in_file' INTO TABLE $2.$3"

   if [ $? -ne 0 ]
   then
      echo "Error while loading file: $1 into the table: $2.$3. Aborting the program."
      echo
      exit 255
   else
      echo "File: $in_file is loaded to table $2.$3."
      echo
   fi
done
}

file_process()
{
SRC_FILE=$1
SRC_HIVE_DB=$2
SRC_STG_TAB=$3

if [ `ls ${SRC_FILE}* | wc -l` -gt 0 ]
then
   echo "Files: ${SRC_FILE} are available to load."
   echo
   echo "Truncating the table: ${SRC_STG_TAB} to load data."
   echo

hive_truncate_table ${SRC_HIVE_DB} ${SRC_STG_TAB}
   
   echo "Loading files to hive table: ${SRC_HIVE_DB}.${SRC_STG_TAB}."
   echo
   
hive_table_load ${SRC_FILE} ${SRC_HIVE_DB} ${SRC_STG_TAB}

   echo "All available files are loaded successfully."
   echo

   echo "File: ${SRC_FILE} are available to load in Hadoop."
   echo 
 
   echo "Adding file: ${SRC_FILE} to ${SRC_STG_TAB}"
   echo

hadoop fs -put /${SOURCE_DIR}/${SRC_FILE} 'maprfs:/${HADOOP_DIR}/${SRC_HIVE_DB}/${SRC_STG_TAB}'

   echo "All available files are loaded successfully."
   echo

else
   echo "No files: ${SRC_FILE} are available to load. Aborting the program."
   echo
   exit 255
fi
}

file_process_adhoc()
{
SRC_FILE=$1
SRC_HIVE_DB=$2
SRC_STG_TAB=$3

if [ `ls ${SRC_FILE}* | wc -l` -gt 0 ]
then
   echo "Files: ${SRC_FILE} are available to load."
   echo
   echo "Truncating the table: ${SRC_STG_TAB} to load data."
   echo

hive_truncate_table ${SRC_HIVE_DB} ${SRC_STG_TAB}

   echo "Loading files to hive table: ${SRC_HIVE_DB}.${SRC_STG_TAB}."
   echo

hive_table_load ${SRC_FILE} ${SRC_HIVE_DB} ${SRC_STG_TAB}

   echo "All available files are loaded successfully."
   echo

   echo "File: ${SRC_FILE} are available to load in Hadoop."
   echo

   echo "Adding file: ${SRC_FILE} to ${SRC_STG_TAB}"
   echo

hadoop fs -put /${SOURCE_DIR}/${SRC_FILE} 'maprfs:/${HADOOP_DIR}/${SRC_HIVE_DB}/${SRC_STG_TAB}'

   echo "All available files are loaded successfully."
   echo

else
   echo "Optional files: ${SRC_FILE} are not available to load. Skipping the file load."
   echo
   
fi
}

file_process ${TRANS_SRC_FILE} ${HIVE_DB} ${TRANS_SRC_STG_TAB}
#file_process ${PRODUCT_SRC_FILE} ${HIVE_DB} ${PRODUCT_SRC_STG_TAB}
file_process ${BASKET_SUMMARY_SRC_FILE} ${HIVE_DB} ${BASKET_SRC_STG_TAB}
file_process ${FRESON_SRC_FILE} ${HIVE_DB} ${FRESON_SRC_STG_TAB}
file_process ${ITEM_MOVE_SRC_FILE} ${HIVE_DB} ${ITEM_MOVE_SRC_STG_TAB}
file_process ${SHRINK_SRC_FILE} ${HIVE_DB} ${SHRINK_SRC_STG_TAB}
file_process ${POINTS_SRC_FILE} ${HIVE_DB} ${POINT_SRC_STG_TAB}
file_process_adhoc ${storesIn_SRC_FILE} ${HIVE_DB} ${storesIn_SRC_STG_TAB}
file_process_adhoc ${FORECAST_SRC_FILE} ${HIVE_DB} ${FORECAST_SRC_STG_TAB}
file_process_adhoc ${INVENTORY_SRC_FILE} ${HIVE_DB} ${INVENTORY_SRC_STG_TAB}
file_process_adhoc ${PURCHASE_SRC_FILE} ${HIVE_DB} ${PURCHASE_SRC_STG_TAB}
file_process_adhoc ${STORE_MAPPING_SRC_FILE} ${HIVE_DB} ${STORE_MAPPING_TAB}
file_process_adhoc ${PLANOGRAM_ITEM_SRC_FILE} ${HIVE_DB} ${SAVEON_PL_ITEM_TAB}
file_process_adhoc ${STORE_PLANOGRAM_SRC_FILE} ${HIVE_DB} ${SAVEON_PL_STORE_TAB}
file_process_adhoc ${PRODUCT_PLANOGRAM_SRC_FILE} ${HIVE_DB} ${SAVEON_PL_PRODUCT_TAB}
file_process_adhoc ${PRODUCTCODE_PLANOGRAM_SRC_FILE} ${HIVE_DB} ${SAVEON_PRODUCTCODE_SRC_TAB}

echo "Moving the files to archive directory: ${ARCHIVE_DIR}."
echo 

mv ${TRANS_SRC_FILE}* ${ARCHIVE_DIR}
#mv ${storesIn_SRC_FILE}* ${ARCHIVE_DIR}
mv ${FRESON_SRC_FILE}* ${ARCHIVE_DIR}
mv ${BASKET_SUMMARY_SRC_FILE}* ${ARCHIVE_DIR}
mv ${ITEM_MOVE_SRC_FILE}* ${ARCHIVE_DIR}
mv ${TENDER_SRC_FILE}* ${ARCHIVE_DIR}
mv ${FORECAST_SRC_FILE}* ${ARCHIVE_DIR}
mv ${POINTS_SRC_FILE}* ${ARCHIVE_DIR}
#echo "Moving the ${FACT_SRC_S} to ${RAW_DIR} directory"
echo

mv ${SHRINK_SRC_FILE}* ${RAW_DIR}

echo "Files are moved to archive directory."
echo

echo "File ${SHRINK_SRC_FILE} moved to ${RAW_DIR} directory"
echo

echo "Calling HQL: ${FINAL_TRANS_MERGE_HQL} to generate final Transaction file in directory: ${RAW_DIR}."
echo

hive -hiveconf hive_db=${HIVE_DB} -f ${FINAL_TRANS_MERGE_HQL} | sed 's/[\t]/|/g; s/NULL//g' > ${RAW_DIR}/${FACT_SRC_TRAN_FILE}_`date +'%Y%m%d'`.dat

echo "Calling HQL: ${FINAL_SHRINK_MERGE_HQL} to generate final Transaction file in directory: ${RAW_DIR}."
echo

hive -hiveconf hive_db=${HIVE_DB} -f ${FINAL_SHRINK_MERGE_HQL} | sed 's/[\t]/|/g; s/NULL//g' > ${RAW_DIR}/${FACT_SRC_SHRINK_FILE}_`date +'%Y%m%d'`.dat


echo "Calling HQL: ${FINAL_FRESON_MERGE_HQL} to generate final Transaction file in directory: ${RAW_DIR}."
echo

hive -hiveconf hive_db=${HIVE_DB} -f ${FINAL_FRESON_MERGE_HQL} | sed 's/[\t]/|/g; s/NULL//g' > ${RAW_DIR}/${FACT_SRC_FRESON_FILE}_`date +'%Y%m%d'`.dat



cat ${RAW_DIR}/${FACT_SRC_TRAN_FILE}_`date +'%Y%m%d'`.dat ${RAW_DIR}/${FACT_SRC_SHRINK_FILE}_`date +'%Y%m%d'`.dat ${RAW_DIR}/${FACT_SRC_FRESON_FILE}_`date +'%Y%m%d'`.dat > ${RAW_DIR}/${FACT_SRC_FILE}_`date +'%Y%m%d'`.dat



echo "get parameters for adhoc hql"
CURRENT_WEEK=`hive -e "SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
 set hive.execution.engine=tez;
 set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
select distinct tm.tm_dim_key from $HIVE_DB.time_stg t
join $HIVE_DB.tm_dim_day tm on (tm.day_dsc=from_unixtime(unix_timestamp(t.flyer_end_date,'yyyy.MM.dd'), 'MM-dd-yyyy'))
where tdate=from_unixtime(unix_timestamp() -2*60*60*24, 'yyyy.MM.dd');"`

echo "current_week is $CURRENT_WEEK"

FOUR_WEEK=`hive -e "SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
 set hive.execution.engine=tez;
 set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
select distinct tm.tm_dim_key from $HIVE_DB.time_stg t
join $HIVE_DB.tm_dim_day tm on (tm.day_dsc=from_unixtime(unix_timestamp(t.flyer_end_date,'yyyy.MM.dd'), 'MM-dd-yyyy'))
where tdate=from_unixtime(unix_timestamp() -2*60*60*24, 'yyyy.MM.dd');"`

echo "four_week_ago is $CURRENT_WEEK"




echo "Calling HQL: ${FILEPREP_ADHOC_HQL} to load_adhoc_files in directory: ${RAW_DIR}."
hive --hiveconf current_week=${CURRENT_WEEK} --hiveconf four_week=${FOUR_WEEK} -f ${FILEPREP_ADHOC_HQL}
echo

echo "Final Transaction file: ${RAW_DIR}/saveon_final_trans_`date +'%Y%m%d'`.dat is generated successfully."
echo


echo "Script Execution Completed @ `date`"

exit 0
