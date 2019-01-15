# Purpose: To update mart details in config file
#
# Change History Record:
#
# ======================================================================================================================
# DATE      |   AUTHOR NAME        |    CHANGE DESCRIPTION |JIRA_ID
# ======================================================================================================================

# 22 Oct 2018 |   Innocent Musanzikwa  |    New script created | SAC-386
# ----------------------------------------------------------------------------------------------------------------------
########################################################################################################################


echo
echo "######################################################################################"
echo
echo "Job name - $0"
echo
echo "Number of Parameters - $#"
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
     echo "Usage: sh $0 <config_file>"
        echo "***************************************************************************"
        echo ""
        exit 1
fi

echo
echo "Script $0 execution started @ `date`"
echo


CONFIG_FILE=${1}

. ${CONFIG_FILE}

#initialize error trap functions to check the availability of files and directory

echo "***************************************************************************"
        echo
        echo "CHANGING MART DETAILS"
        echo "-----------------------"
        echo

ACTIVE_MART=` hive -e "SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
 set hive.execution.engine=tez;
 set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
select m.mart from $HIVE_DB.tm_dim_day d
join $HIVE_DB.saveon_week_mart m on d.iri_week=m.iri_week
where day_dsc= from_unixtime(unix_timestamp(), 'MM-dd-yyyy');"`

echo "Active Mart for this week is $ACTIVE_MART"

if grep 'export MART=A' ${CONFIG_FILE}
then
  `sed -i "s/export MART=A/export MART=${ACTIVE_MART}/" ${CONFIG_FILE} `
  echo "config file mart changed from export MART=A to export MART=$ACTIVE_MART"
else
  echo ""
  `sed -i "s/export MART=B/export MART=${ACTIVE_MART}/" ${CONFIG_FILE}`
  echo "config file mart changed from export MART=B to export MART=$ACTIVE_MART"

fi

echo "***************************************************************************"




