# Change History Record:
#
# =======================================================================================================================================================
# Version | DATE       |   Modifier         |   Change made                                                                           | JIRA-ID
# ======================================================================================================================================================
# v1.0    | 09/10/2018 | Shibashish         | 1. New Script created                                                                   | 
# v1.1    | 10/09/2018 | Inno        	    | 2. Parameterized hive path and changed sqoop server parameters                          |
# ======================================================================================================================================================
##########################################################################################################################################################

echo "######################################################################################";
echo
echo "Job name  - $0";
echo
echo "Number of Parameters - $#";
echo

CONFIG_FILE=${1}

echo ${CONFIG_FILE}

. ${CONFIG_FILE}

#. $ERRORTRAPS

#syntax check
if [ $# -ne 1 ] 
then
    echo
    echo "***************************************************************************"
    echo
    echo "ERROR MESSAGE:"
    echo "---------------"
    echo
    echo "#ERROR: Error due to wrong number of parameters passed to the script......"
    echo ""
    echo "Usage: sh $0 <CONFIG_FILE>"
    echo "***************************************************************************"
    echo ""
    exit 1
fi

#set -vx

STARTDATE=`date`
JOBTYPE="Sqoop export "
CURR_TSP=`date +"%Y%m%d_%H%M%S"`

#########################################################


echo
echo "Started ${JOBTYPE} : ${STARTDATE}"
echo
echo

echo
#echo "Truncate tables in hive for import"
#hive -f ${SCRIPTDIR}/truncate_import_tables.hql
#echo "Truncate completed"
echo

echo
echo "run sqoop import prep steps"
#sh ${SCRIPTDIR}/sqoop_import_prep.sh ${CONFIG_FILE}
echo "sqoop import prep done"
echo



echo "sqooping OFFER_SRC"
echo
echo hadoop fs -rmr "${HIVE_PATH}/offer_src_stg_1/"
sqoop import \
--connect "jdbc:oracle:thin:@${MART_SERVER}:1521/${MART_DB}" \
--username ${MART_USER} \
--password ${MART_PASSWD} \
--table OFFER_SRC_STG \
--hive-database ${HIVE_DB} \
--hive-table OFFER_SRC_STG \
--hive-import \
--hive-overwrite \
--target-dir "${HIVE_PATH}/offer_src_stg_1" \
-m 1 \


if [ "$?" != "0" ]
then
echo "error while loading sqoop staging"
exit 1
else
echo "success"
fi

echo "Load into OFFER_SRC"

echo "sqooping PL_SRC"
echo

hadoop fs -rmr "${HIVE_PATH}/pl_src_stg_1/"
sqoop import \
--connect "jdbc:oracle:thin:@${MART_SERVER}:1521/${MART_DB}" \
--username ${MART_USER} \
--password ${MART_PASSWD} \
--table PL_SRC_STG \
--hive-database ${HIVE_DB} \
--hive-table PL_SRC_STG \
--hive-import \
--hive-overwrite \
--target-dir "${HIVE_PATH}/pl_src_stg_1" \
-m 1 \


if [ "$?" != "0" ]
then echo "error while loading sqoop pl_src_staging"
exit 1
else echo "success"
fi

echo "Load into saveon_retail_metric"

hadoop fs -rmr "${HIVE_PATH}/saveon_retail_metric_1/"
sqoop import \
--connect "jdbc:oracle:thin:@${SQOOP_SERVER}:1521/${SQOOP_DB}" \
--username ${SQOOP_USER} \
--password ${SQOOP_PASSWORD} \
--table SAVEON_RETAIL_METRIC \
--hive-database ${HIVE_DB} \
--hive-table SAVEON_RETAIL_METRIC \
--hive-import \
--hive-overwrite \
--target-dir "${HIVE_PATH}/saveon_retail_metric_1" \
-m 1 \


if [ "$?" != "0" ]
then echo "error while loading sqoop saveon_retail_metric"
exit 1
else echo "success"
fi

echo "Load into saveon_productcode_mapping"

hadoop fs -rmr "${HIVE_PATH}/saveon_productcode_mapping_1/"
sqoop import \
--connect "jdbc:oracle:thin:@${SQOOP_SERVER}:1521/${SQOOP_DB}" \
--username ${SQOOP_USER} \
--password ${SQOOP_PASSWORD} \
--table SAVEON_PRODUCTCODE_MAPPING \
--hive-database ${HIVE_DB} \
--hive-table SAVEON_PRODUCTCODE_MAPPING \
--hive-import \
--hive-overwrite \
--target-dir "${HIVE_PATH}/saveon_productcode_mapping_1" \
-m 1 \


if [ "$?" != "0" ]
then echo "error while loading sqoop saveon_productcode_mapping"
exit 1
else echo "success"
fi


echo  "Script $0 execution completed @  `date`"
echo 
