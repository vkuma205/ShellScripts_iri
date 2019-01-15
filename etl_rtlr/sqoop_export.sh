# Change History Record:
#
# =======================================================================================================================================================
# Version | DATE       |   Modifier         |   Change made                                                                           | JIRA-ID
# ======================================================================================================================================================
# v1.0    | 04/24/2018 | Inno               | 1. New Script created                                                                   | 
# v2.0    | 05/30/2018 | Vinod Gardas       | 2. Edited logs as per QA                                                                |
# v3.0    | 06/05/2018 | Vinod Gardas       | 3. Added Consumer export                                                                |
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

#. ${ERRORTRAPS}

sh ${SCRIPTDIR}/initialize_hadoop_tables.sh ${CONFIG_FILE}

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

STARTDATE=`date`
JOBTYPE="Sqoop export "
CURR_TSP=`date +"%Y%m%d_%H%M%S"`

#########################################################


echo
echo "Started ${JOBTYPE} : ${STARTDATE}"
echo
echo


echo "sqooping All dimension keys attributes"

echo "sqooping consumer dimension keys"
sqoop export \
--connect jdbc:oracle:thin:@${SQOOP_SERVER}:1521/${SQOOP_DB} \
--username ${SQOOP_USER} \
--password ${SQOOP_PASSWORD} \
--table SAVEON_CN_ALL_KEYS \
--export-dir ${SQOOP_HIVE_PATH}/saveon_cn_all_keys_new \
--input-fields-terminated-by "\t" \
--input-lines-terminated-by "\n" \
--verbose -m 8 \
--input-null-string '\\N' \
--input-null-non-string '\\N'

echo
echo "Consumer keys had been moved from Hive to Oracle table: SAVEON_CN_ALL_KEYS"
echo

echo
echo "Sqooping ITEM dimension keys has started..."
echo

sqoop  export \
--connect jdbc:oracle:thin:@${SQOOP_SERVER}:1521/${SQOOP_DB} \
--username ${SQOOP_USER} \
--password ${SQOOP_PASSWORD} \
--table SAVEON_IT_ALL_KEYS \
--export-dir ${SQOOP_HIVE_PATH}/saveon_it_all_keys_new/ \
--input-fields-terminated-by "|" \
--input-lines-terminated-by "\n" \
--verbose -m 8 \
--input-null-string '\\N' \
--input-null-non-string '\\N'

echo
echo "Item Keys had been moved from Hive to Oracle table: SAVEON_IT_ALL_KEYS"
echo
echo
echo "sqooping VENUE dimension keys has started..."
echo

sqoop  export \
--connect jdbc:oracle:thin:@${SQOOP_SERVER}:1521/${SQOOP_DB} \
 --username ${SQOOP_USER} \
--password ${SQOOP_PASSWORD} \
--table SAVEON_VN_ALL_KEYS \
--export-dir ${SQOOP_HIVE_PATH}/saveon_vn_all_keys_new/ \
--input-fields-terminated-by "\t" \
--input-lines-terminated-by "\n" \
--verbose -m 8 \
--input-null-string '\\N' \
--input-null-non-string '\\N'

echo
echo "Venue Keys had been moved from Hive to Oracle table: SAVEON_VN_ALL_KEYS"
echo

echo "Script $0 execution completed @  `date`"
