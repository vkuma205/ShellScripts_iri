#!/bin/bash

hadoop fs -rmr maprfs:/user/hive/warehouse/temp_11_10/

query="select vn_dim_key,      
sx,                   
storename,             
location_id,                 
opening_date,          
closing_date,          
location_desc,         
address_line_1,        
address_line_2,        
town_code,             
province_code,         
postal_code,           
division_desc,         
rm_region_id,          
pricing_zone_id,       
location_name from wh_postx_saveon_p1.eal_dk_saveon_vn_mstr where \$CONDITIONS"

echo $query
sqoop import --connect "jdbc:oracle:thin:@(description=(address=(protocol=tcp)(host=ex04-scan1.ch3.prod.i.com)(port=1521))(connect_data=(service_name=sv01dwhp)))" --username "WH_POSTX_SAVEON_P1" --password "yH1z9OBQ" --query "$query" --hive-import --hive-database "wh_postx_saveon_p1" --hive-table "eal_dk_saveon_vn_mstr" --m 1 --hive-overwrite --target-dir "maprfs:/user/hive/warehouse/temp_11_10/"

