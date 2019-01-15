use wh_postx_saveon_p1;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
insert overwrite table pos_tx_agg partition(tm_dim_key_day)
select f.transaction_date ,
transaction_time ,
src_consumer_dim_key ,
src_venue_dim_key ,
src_item_dim_key ,
trans_number ,
f.retailer_product_identifier ,
venue_number ,
sum(nvl(cast(item_gross_amount * 100 as bigint),0) ),
sum(nvl(cast(item_net_amount * 100 as bigint),0)) ,
sum(nvl(cast(item_volume_quantity as bigint),0) ),
sum(nvl(cast(item_quantity as bigint),0)),
max(nvl(cast(custom_col5 * 100 as bigint),0)) scan_dollars ,
max(nvl(cast(custom_col6 as bigint),0)) scan_units ,
0 points_awarded ,
0 points_redeemed ,
max(nvl(pl.pl_dim_key,12522)) scan_weight , 
max(nvl(cast(custom_col11 * 100 as bigint),0)) landed_branch_cost ,
sum(nvl(cast(custom_col14 as bigint),0)) shrink_units ,
sum(nvl(cast(custom_col15 * 1000 as bigint),0)) shrink_weight ,
sum(nvl(cast(custom_col16 * 100 as bigint),0)) shink_cost ,
sum(nvl(cast(custom_col17 * 100 as bigint),0))  shrink_dollars ,
sum(case when cast(f.retailer_product_identifier as bigint) in (18186,29914,54857,60558,21818600000,25302100000,25485700000) then nvl(cast(item_quantity as bigint),0) else 0 end ) ecommerce_units ,
sum(case when cast(f.retailer_product_identifier as bigint) in (18186,29914,54857,60558,21818600000,25302100000,25485700000) then nvl(cast(item_gross_amount * 100 as bigint),0) else 0 end ) ecommerce_dollars ,
0 forecast_dollars ,
0 forecats_units ,
max(nvl(m.retailmetric,0)),
f.tm_dim_key_day
from freson_pos_tx_fact f
left join plano_src_stg pl on pl.locationid =f.venue_number and pl.retailer_product_identifier=f.retailer_product_identifier
left join saveon_retail_metric m on f.retailer_product_identifier=m.retailer_product_identifier
where f.tm_dim_key_day>=40875
group by f.transaction_date ,
transaction_time ,
src_consumer_dim_key ,
src_venue_dim_key ,
src_item_dim_key ,
trans_number ,
f.retailer_product_identifier ,
venue_number ,f.tm_dim_key_day distribute by f.tm_dim_key_day;


set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
insert into causal partition(tm_dim_key_day)
select transaction_date, transaction_time, src_consumer_dim_key, src_venue_dim_key, src_item_dim_key, trans_number, retailer_product_identifier, venue_number, pl_dim_key, of_dim_key, 1 as basket_attr_dim_key, item_gross_amount, item_net_amount, item_volume_quantity, item_quantity,
scan_dollars, scan_units, points_awarded, points_redeemed, landed_branch_cost, shrink_units, shrink_weight, shink_cost, shrink_dollars, ecommerce_units, ecommerce_dollars, forecast_dollars, forecats_units, retailmetric,tm_dim_key_day
from(
select f.*,nvl(o.of_dim_key,970472) of_dim_key,row_number() over(partition by f.tm_dim_key_day,f.trans_number,f.retailer_product_identifier,f.item_gross_amount order by sm.group_rank) as rn
from pos_tx_agg  f
left join store_mapping sm on f.venue_number=sm.locationid
left join (select * from of_src_stg where enddatekey >= 20160102)  o on o.retailer_product_identifier=f.retailer_product_identifier and o.locationid=sm.master_location_id
where f.tm_dim_key_day>=40875
) x where rn=1;

