
# Change History Record:
#
# =======================================================================================================================================================
# Version | DATE       |   Modifier         |   Change made                                                                           | JIRA-ID
# ======================================================================================================================================================
# v1.0    | 05/27/2016 | RAGHAVENDRA B      | 1. New Script created                                                                   | ETLR-84

# v2.0    | 08/26/2016 | MUSANZIKWA I       | 1. Added file extension parameter for non dat files                                     |


# v2.0    | 04/11/2017 | MUSANZIKWA I       | 1. Added fetching parameters from file

# ======================================================================================================================================================
##########################################################################################################################################################

echo "######################################################################################";
echo
echo "Job name  - $0";
echo
echo "Number of Parameters - $#";
echo

CONFIG_FILE=${1}

. ${CONFIG_FILE}


#syntax check
if [ ${#} -ne 1 ]; then
    echo "#ERROR: Error due to wrong number of parameters passed to the script......"
    echo "Usage:$0 CONFIG_FILE SOURCE_FILE TARGET_TABLE META_TABLE";
    echo
exit -1
fi

STARTDATE=`date`
JOBTYPE="RAW File Load to Stage Table "
CURR_TSP=`date +"%Y%m%d_%H%M%S"`

#########################################################


echo
echo "Started ${JOBTYPE} : ${STARTDATE}"
echo
echo

hive -e "use ${HIVE_DB}; select distinct src_consumer_dim_key from ${SAVEON_EAL_TABLE} where src_consumer_dim_key<>-1 ;"  > ${WB_PATH}/wb_stage/${CLIENT}postx_cn_all_keys.dat

echo
echo "Script $0 execution completed @  `date`"

