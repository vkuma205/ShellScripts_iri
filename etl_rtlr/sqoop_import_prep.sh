#!/bin/bash

########################################################################################################################
#
# Purpose:Clean up tables in oracle to be imported to hive
#
# Change History Record:
#
# ======================================================================================================================
# DATE        |   AUTHOR NAME        |    CHANGE DESCRIPTION | JIRA ID
# ======================================================================================================================
# 01 Oct 2018 |   Inno     | New script created    |
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
        echo "Usage: sh $0 <CONFIG_FILE>"
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


echo "Post Delta to Master processing begun"
echo "**********************************************************************************"
echo


echo "sqlplus $SQOOP_USER/$SQOOP_PASSWORD@$SQOOP_DB<<ENDSQL
                WHENEVER SQLERROR EXIT 1
                SET ECHO ON
                @ ${SCRIPTDIR}//sqoop_import_prep.sql
ENDSQL

"

sqlplus $SQOOP_USER/$SQOOP_PASSWORD@$SQOOP_DB<<ENDSQL
                WHENEVER SQLERROR EXIT 1
                SET ECHO ON

                ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
                ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
                ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

                @ ${SCRIPTDIR}/sqoop_import_prep.sql


ENDSQL

testdberror $?


echo "Post Delta to Master processing completed"
echo "**********************************************************************************"
echo

#######################################################################

echo
echo "Script execution completed @ `date`"

