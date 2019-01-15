#!/bin/bash
########################################################################################################################
#
# Purpose: Join Item Table with other datasets for SAVEON
#
# Change History Record: 
#
# ======================================================================================================================
# DATE        |   AUTHOR NAME        |    CHANGE DESCRIPTION | JIRA_ID 
# ======================================================================================================================
# 16 Aug 2018 |   Vinod Gardas       |    New script created |i
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
        echo "Usage: sh $0 <CONFIG_FILE> <DIM_ABBR>"
        echo "***************************************************************************"
        echo ""
        exit 1
fi

echo
echo "Script $0 execution started @ `date`"
echo

CONFIG_FILE=${1}
DIM_ABBR=${2}

. ${CONFIG_FILE}

echo
echo "**********************************************************************************"
echo

JOB_STRT_TS=`date '+%Y-%m-%d_%H%M%S'`
export JOB_STRT_TS=${JOB_STRT_TS}_$$

DIM_ABBR_UPR=`echo ${DIM_ABBR} | awk '{print toupper($0)}'`

sqlplus -s ${WH_POSTX_USER}/${WH_POSTX_PASSWORD}@${WH_POSTX_DATABASE} <<ENDSQL
               WHENEVER SQLERROR EXIT 1
               SET ECHO ON
               @ ${SCRIPTDIR}/${DIM_ABBR_UPR}.sql
ENDSQL

testdberror $?

echo
echo "Script execution completed @ `date`"

