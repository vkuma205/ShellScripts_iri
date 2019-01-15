use wh_postx_saveon_p1;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
insert overwrite table causal_new partition(tm_dim_key_day)
select transaction_date, transaction_time, src_consumer_dim_key, src_venue_dim_key, src_item_dim_key, trans_number, retailer_product_identifier, venue_number, pl_dim_key, of_dim_key, BASKET_ATTR_DIM_KEY as basket_attr_dim_key, item_gross_amount, item_net_amount, item_volume_quantity, item_quantity,
scan_dollars, scan_units, points_awarded, points_redeemed, landed_branch_cost, shrink_units, shrink_weight, shink_cost, shrink_dollars, ecommerce_units, ecommerce_dollars, forecast_dollars, forecats_units, retailmetric,tm_dim_key_day
from(
select f.*,NVL(b.BASKET_ATTR_DIM_KEY,1) BASKET_ATTR_DIM_KEY,nvl(o.of_dim_key,970472) of_dim_key,row_number() over(partition by f.tm_dim_key_day,f.trans_number,f.retailer_product_identifier,f.item_gross_amount order by sm.group_rank) as rn
from pos_tx_agg  f
left join store_mapping sm on f.venue_number=sm.locationid
left join (select * from of_src_stg where enddatekey >= 20180824)  o on o.retailer_product_identifier=f.retailer_product_identifier and o.locationid=sm.master_location_id
left join saveon_basket_attr_dim b on cast(regexp_replace(substr(f.transaction_time,0,2),':','') as int)=b.HOUR_ID_DIM_DSC and (case when cast(from_unixtime(unix_timestamp(cast(f.transaction_date as string),'yyyyMMdd'),'u')+1 as int) = '8' then 1 else  cast(from_unixtime(unix_timestamp(cast(f.transaction_date as string),'yyyyMMdd'),'u')+1 as int) end)=b.DAY_OF_WEEK
where f.tm_dim_key_day >=41811 and ((item_gross_amount<>0 and item_quantity<>0) or shrink_dollars <> 0)
) x where rn=1
distribute by tm_dim_key_day;

