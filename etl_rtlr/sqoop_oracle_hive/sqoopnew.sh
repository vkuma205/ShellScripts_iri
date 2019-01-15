#!/bin/bash

hadoop fs -rmr maprfs:/user/hive/warehouse/temp_11_10/

query="select it_dim_key,
  item_id,
  retail_item_cd,
  item_status_cd,
  ap_vendor_id,
  item_desc,
  dept_id,
  dept_desc,
  class_id,
  class_desc,
  cat_id,
  cat_desc,
  merchandiser_number,
  merchandiser_nm,
  retail_metric,
  retail_pack,
  retail_size,
  merch_group,
  family_cd,
  lead_item_retail_cd,
  brand_nm,
  padded_upc,
  padded_retail_item_cd,
  upc,
  ap_vendor_description,
  high_level_departments,
  family_cd_desc,
  iri_brand_desc,
  iri_vendor_desc,
  first_scan_date,
  last_scan_date from wh_postx_saveon_p1.eal_dk_saveon_it_mstr where \$CONDITIONS"

echo $query
sqoop import --connect "jdbc:oracle:thin:@(description=(address=(protocol=tcp)(host=ex04-scan1.ch3.prod.i.com)(port=1521))(connect_data=(service_name=sv01dwhp)))" --username "WH_POSTX_SAVEON_P1" --password "yH1z9OBQ" --query "$query" --hive-import --hive-database "wh_postx_saveon_p1" --hive-table "eal_dk_saveon_it_mstr" --m 1 --hive-overwrite --target-dir "maprfs:/user/hive/warehouse/temp_11_10/"

