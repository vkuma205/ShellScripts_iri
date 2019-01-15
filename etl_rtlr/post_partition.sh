
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
      echo "Usage: sh $0 <CONFIG_FILE>"
      echo "*****************************************************************************"
      echo ""
      exit -1
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
echo 


#${PYTHON_PATH} /opt/ildbld/${SCRIPTENV}/etl_rtlr/wb_extract/create_and_load.py ${HIVE_DB} ${SAVEON_PART_TABLE}  ${WB_PATH}/cn_part_map/${CLIENT}postx_cn_partition.dat

cd ${FSP_DIR}

echo
echo "Current working directory for FSP extracts changed to ${FSP_DIR}"
echo
echo "Loading the data into Wb..."
echo

sh ${FSP_DIR}/fsp_wb_extract.sh  ${WB_PATH}config/wb_data_prep_${CLIENT}postx${MART}.cfg  ${LOGDIR}/fsp_wb_extract_$$.log ${FACT_STAGE_DIR}/${CLIENT}postx${MART}/fact_stage/ ${NUM_PART} ${DOP} yes no no 


echo "sh ${FSP_DIR}fsp_wb_extract.sh  ${WB_PATH}config/wb_data_prep_${CLIENT}postx${MART}.cfg  ${LOGDIR}fsp_wb_extract_$$.log ${FACT_STAGE_DIR}/${CLIENT}postx${MART}/fact_stage/ ${NUM_PART} ${DOP} yes no no "

echo
echo "*****************************************************************"
echo

echo
echo "Script $0 execution completed @  `date`"

