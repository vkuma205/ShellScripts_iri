####################################################################
#
# Script Name: fact_process.sh
# Purpose: moves one file after the other for loading purposes
# Change History Record:
#
# =============================================================================================
# DATE      |   Modifier           |   Change made                  | Bug Number [If Available]
# =============================================================================================
# 08/09/2016|  Innocent Musanzikwa | New Script created             |
# ---------------------------------------------------------------------------------------------
#
#
###############################################################################################

echo
echo "######################################################################################";
echo
echo "Job name  - $0*";
echo
echo "Number of Parameters - $#";
echo

if [ ${#} -ne 2 ]; then
    echo "#ERROR: Error due to wrong number of parameters passed to the script......"
    exit -1
fi

SRCDIR=$1
DESTDIR=$2
YEAR=201600
COUNTER=2
MAXCOUNTER=13

while [  $YEAR -lt 201900 ]; 
do
  if [ ${YEAR} == 201800 ]; 
  then
     MAXCOUNTER=10
  fi
  
  while [ $COUNTER -lt $MAXCOUNTER ]; 
  do
     echo "The month is " $(($YEAR + $COUNTER)) 
 
     echo "mv $SRCDIR/*$(($YEAR + $COUNTER))* $DESTDIR "
     cp $SRCDIR/*$(($YEAR + $COUNTER))* $DESTDIR

     echo "sh /opt/ildbld/prd/workaround/etl_rtlr/saveon/fileprep_reload.sh /opt/ildbld/prd/workaround/etl_rtlr/saveon/saveon_hadoop_load_main.cfg  $(($YEAR + $COUNTER))"
     sh /opt/ildbld/prd/workaround/etl_rtlr/saveon/fileprep_reload.sh /opt/ildbld/prd/workaround/etl_rtlr/saveon/saveon_hadoop_load_main.cfg  $(($YEAR + $COUNTER)) > /opt/ildbld/prd/logs/etl_rtlr/saveon/fileprep_$(($YEAR + $COUNTER))01.log 

     echo "/opt/anaconda/bin/python /opt/ildbld/prd/etl_rtlr/hdp_ldr/2.6/scripts/fsp_main_script.py saveon externaldata01 NO > /opt/ildbld/prd/logs/etl_rtlr/saveon/fsp_main_script_saveon_$(($YEAR + $COUNTER))01.log "
     /opt/anaconda/bin/python /opt/ildbld/prd/etl_rtlr/hdp_ldr/2.6/scripts/fsp_main_script.py saveon externaldata01 NO > /opt/ildbld/prd/logs/etl_rtlr/saveon/fsp_main_script_saveon_$(($YEAR + $COUNTER))01.log

     let COUNTER=COUNTER+1 
  done
  COUNTER=1
  let YEAR=$(($YEAR+100))
done

#for i in ${SRCDIR}/IRI_TXNITEM*
#do
#mv $i $DESTDIR
#sh saveon_hadoop_run.sh /opt/ildbld/dev/etl_rtlr/saveon/scripts/saveon_hadoop_load.cfg > /tmp/saveon_reload_$$.log
#echo "
#FILE_NAME: $i
#"
#done
