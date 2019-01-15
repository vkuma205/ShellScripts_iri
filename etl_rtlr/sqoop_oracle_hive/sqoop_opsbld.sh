#!/bin/bash

hadoop fs -rmr maprfs:/user/hive/warehouse/temp_11_10/
i
query="select container_id,        
client,
dim_name,   
dim_abbr,   
dim_id, 
file_name_prefix,  
column_meta,      
natural_key,
surrogate_key, 
key_srvc_name, 
eal_key_spec_name, 
eal_attr_spec_name, 
key_srvc_param,
cntry_cd,
eal_attr_load_type,   
raw_column_meta,        
stg_column_meta from OPSBLD_IRI_P1.meta_key_definition where \$CONDITIONS"

echo $query
sqoop import --connect "jdbc:oracle:thin:@(description=(address=(protocol=tcp)(host=ex04-scan1.ch3.prod.i.com)(port=1521))(connect_data=(service_name=sv01dwhp)))" --username "OPSBLD_IRI_P1" --password "Pass9rx4#" --query "$query" --hive-import --hive-database "wh_postx_saveon_p1" --hive-table "meta_key_definition" --m 1 --hive-overwrite --target-dir "maprfs:/user/hive/warehouse/temp_11_10/"

