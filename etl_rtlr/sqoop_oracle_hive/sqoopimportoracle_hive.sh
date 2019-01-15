#!/bin/bash

while read -r LINE; do


sqoop import --connect "jdbc:oracle:thin:@(description=(address=(protocol=tcp)(host=ex04-scan1.ch3.prod.i.com)(port=1521))(connect_data=(service_name=sv01dwhp)))" --username "WH_POSTX_SAVEON_P1" --password "yH1z9OBQ" --table "$LINE" --m 1 --hive-database "wh_postx_saveon_p1" --hive-import --hive-overwrite --target-dir "maprfs:/user/hive/warehouse/wh_postx_saveon_p1.db/"$LINE""

done < TableList
