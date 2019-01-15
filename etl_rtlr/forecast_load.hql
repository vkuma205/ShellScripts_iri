set forecast_start_week;
set forecast_end_week;

use wh_postx_saveon_p1;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; 
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
INSERT OVERWRITE table causal_new partition(tm_dim_key_day)
SELECT 
cast(from_unixtime(unix_timestamp(f.target_week,'yyyy.MM.dd'), 'YYYYMMdd') as int) as transaction_date, 
"12:00:00" As transaction_time,
f.scr_consumer_dim_key,
f.src_venue_dim_key,
nvl(f.src_item_dim_key,-1) AS src_item_dim_key,
f.trans_number, 
f.retailer_product_identifier,
0 As venue_number, 
12522 As pl_dim_key,
970472 As of_dim_key,
1 As basket_attr_dim_key,
0 AS item_gross_amount,
0 AS item_net_amount ,
0 AS item_volume_quantity,
0 AS item_quantity,
0 AS scan_dollars,
0 As scan_units,
0 As points_awarded,
0 As points_redeemed,
0 As landed_branch_cost,
0 As shrink_units,
0 As shrink_weight,
0 As shrink_cost,
0 As shrink_dollars,
0 As ecommerce_units,
0 As ecommerce_dollars,
0 As forecast_dollars,
0 AS forecast_units,
0 As retailmetric,
base as forecast_base_units,
nvl(cast (i.inventory_qty as bigint),0) as forecast_inv_units,
nvl(cast (p.ordered_qty as bigint),0)  as forecast_ordered_units,
f.tm_dim_key_day
FROM
wh_postx_saveon_p1.pos_forecast_fact F
left outer join wh_postx_saveon_p1.pos_inventory_fact i on f.retailer_product_identifier=i.retailer_product_identifier and f.tm_dim_key_day=i.tm_dim_key_day
left outer join wh_postx_saveon_p1.pos_purchase_fact p on f.retailer_product_identifier=p.retailer_product_identifier and f.tm_dim_key_day=p.tm_dim_key_day
where f.tm_dim_key_day between '${hiveconf:forecast_start_week}' and '${hiveconf:forecast_end_week}';


