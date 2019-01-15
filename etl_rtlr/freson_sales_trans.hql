use wh_postx_saveon_p1;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx3277m;
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts=-Xmx3277m;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;

SELECT FROM_UNIXTIME(UNIX_TIMESTAMP(sale_date ,'yyyy.MM.dd HH:mm:ss'), 'yyyy.MM.dd') as transaction_date,
FROM_UNIXTIME(UNIX_TIMESTAMP(sale_date ,'yyyy.MM.dd HH:mm:ss'), 'HH:mm:ss') as transaction_time,
concat(store_number,item_id) as trans_number,
scan_code as retailer_upc,
scan_code as product_key,
total_sales_dollars as item_gross_amount,
total_sales_dollars as item_net_amount,
total_sales_dollars/total_sales_units as item_list_price,
total_sales_dollars/total_sales_units as item_net_price,
total_sales_units as item_quantity,
scan_weight as item_volume,
0 as total_discount,
store_number as venue_id,
0 as line_id,
'-1' as customer_id,
0 as poscode,
item_id as item,
total_sales_dollars,
total_sales_units,
points_awarded,
points_redeemed,
scan_weight,
retail_metric,
landed_branch_cost,
own_brand_ind,
brand,
0 shrink_units,
0 shrink_weight,
0 shink_cost,
0 shrink_extended_cost,
0 REASON_CODE
FROM freson_src_stg
;
