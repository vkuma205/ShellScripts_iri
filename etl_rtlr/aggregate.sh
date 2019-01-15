#!/bin/bash
########################################################################################################################
#
# Purpose:
#
# Change History Record:
#
# ======================================================================================================================
# DATE        |   AUTHOR NAME        |    CHANGE DESCRIPTION | JIRA_ID
# ======================================================================================================================
# 06 April 2018 |  Siddharth       |    New script created |
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

echo
echo "**********************************************************************************"
echo

JOB_STRT_TS=`date '+%Y-%m-%d_%H%M%S'`
export JOB_STRT_TS=${JOB_STRT_TS}_$$

echo
echo "Creating the table: saveon_cn_all_keys"
echo
echo "Inserting the records into saveon_cn_all_keys from pos_tx_fact"
echo
echo "Creating the table: saveon_it_all_keys"
echo
echo "Inserting the records into saveon_it_all_keys from pos_tx_fact"
echo
echo "Creating the table: saveon_vn_all_keys"
echo
echo "Inserting the records into saveon_vn_all_keys from pos_tx_fact" 
echo

hive -hiveconf db=${HIVE_DB} -f ${AGGREGATE_SQL_PATH}

echo ""
echo
echo "Creating all the dimension tables and inserting the records had completed."
echo
echo "**********************************************************************************"
echo

echo
echo "Script execution completed @ `date`"
