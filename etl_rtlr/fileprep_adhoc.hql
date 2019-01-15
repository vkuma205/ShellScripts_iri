set current_week;
set four_week;

--truncate table saveon_productcode_mapping;
--insert  into saveon_productcode_mapping select * from saveon_productcode_src where retailitemid<>0;
use wh_postx_saveon_p1;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
 set hive.execution.engine=tez;
 set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
insert into pos_txpoint_fact partition(tm_dim_key_day)
select from_unixtime(unix_timestamp(dayofpurchase_dt,'yyyy.MM.dd'), 'YYYYMMdd'), transactionnumber, incentive_id, sum(pointsearned), sum(pointsredeemed), productcode as retailer_product_identifier, dept, tm_dim_key
from point_src_stg
left outer join tm_dim_day as tm on (tm.day_dsc=from_unixtime(unix_timestamp(dayofpurchase_dt,'yyyy.MM.dd'), 'MM-dd-YYYY')) where productcode is not null
group by dayofpurchase_dt,transactionnumber, incentive_id,productcode , dept, tm_dim_key 
distribute by tm_dim_key;


set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
truncate table plano_src_stg;
insert overwrite table plano_src_stg partition(locationid)
select max(pl.pl_dim_key)pl_dim_key ,cast(li.retailer_product_identifier as bigint)upc_id,s.locationid
from saveon_pl_item i
join saveon_pl_product pp on i.ItemID=pp.ItemID
join saveon_productcode_mapping li on li.retailitemid=pp.RetailItemID
join saveon_pl_store s on i.PlanoID=s.PlanoID
join pl_src_stg pl on pl.planoid=i.PlanoID
group by s.locationid,cast(li.retailer_product_identifier as bigint);


set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
truncate table of_src_stg;
insert into of_src_stg partition (enddatekey)
select distinct of_dim_key ,
  offerid ,
  locationid ,
  retailer_product_identifier,
  startdatekey ,
  region_cd,
  enddatekey from offer_src_stg o
join saveon_productcode_mapping m on o.retailitemid=m.retailitemid
distribute by (enddatekey);


set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
--   truncate table pos_forecast_fact;
insert overwrite table pos_forecast_fact PARTITION (tm_dim_key_day)
   select
   fsp.target_week , 
  p.retailer_product_identifier ,
  m.key_value as  src_item_dim_key ,
  max(fsp.location_id) as venue_number ,
  max(m1.key_value) as src_venue_dim_key ,
  1060 as src_consumer_dim_key ,
  sum(cast (fsp.base_sales_qty  + fsct_lift as bigint)), 
  0  system_lift , 
  0 ,
  retail_item_cd,
  max(concat(fsp.location_id,retail_item_cd)),
  tm.tm_dim_key
  from  forecast_src_stg as fsp 
  join saveon_productcode_mapping p on fsp.retail_item_cd=p.retailitemid
  left outer join master_key_lookup as m on (m.alt_key=concat(upper('saveon'),'~',p.retailer_product_identifier)) and (m.dimension='item') and (m.retailer='saveon')  
  left outer join master_key_lookup as m1 on (m1.alt_key=concat(upper('saveon'),'~',fsp.location_id)) and (m1.dimension='venue') and (m1.retailer='saveon') 
  join wh_dimension.time_dim_day as tm on (tm.tm_end_date=from_unixtime(unix_timestamp(target_week,'yyyy.MM.dd'), 'MM-dd-yy'))
  left outer join store st on st.location_id=fsp.location_id
  where tm.tm_dim_key>='${hiveconf:current_week}'  
  group by fsp.target_week , 
  p.retailer_product_identifier ,
  m.key_value ,tm.tm_dim_key, st.division_desc,retail_item_cd
;

insert overwrite table pos_inventory_fact PARTITION (tm_dim_key_day)
  select max(fsp.Warehouse_Inventory_DT)
  ,p.retailer_product_identifier
  ,SUM(CAST(Total_Inventory_Qty AS INT))
  ,tm.tm_dim_key
  from inventory_src_stg as fsp 
  join saveon_productcode_mapping p on fsp.retail_item_cd=p.retailitemid
  join time_src_stg  t on t.`date`=fsp.Warehouse_Inventory_DT
  join wh_dimension.time_dim_day as tm on (tm.tm_end_date=from_unixtime(unix_timestamp(t.flyer_end_date,'yyyy.MM.dd'), 'MM-dd-yy'))
  where tm.tm_dim_key='${hiveconf:current_week}'  
  GROUP BY 
  p.retailer_product_identifier
  ,tm.tm_dim_key;

 insert overwrite table pos_purchase_fact PARTITION (tm_dim_key_day)
  select max(fsp.Arrival_DT)
  ,p.retailer_product_identifier
  ,SUM(CAST(Ordered_Qty AS INT) * CAST(Retail_Multiple_Qty AS int) )
  ,tm.tm_dim_key
  from purchase_src_stg as fsp 
  join saveon_productcode_mapping p on fsp.retail_item_cd=p.retailitemid
  join time_src_stg  t on t.`date`=fsp.Arrival_DT
  join wh_dimension.time_dim_day as tm on (tm.tm_end_date=from_unixtime(unix_timestamp(t.flyer_end_date,'yyyy.MM.dd'), 'MM-dd-yy'))
  where tm.tm_dim_key='${hiveconf:current_week}'
  GROUP BY 
  p.retailer_product_identifier
  ,tm.tm_dim_key;

insert overwrite table  pos_forecast_fact PARTITION (tm_dim_key_day)
 select  from_unixtime(unix_timestamp(tm_end_date,'MM-dd-yy'),'yyyy.MM.dd' ) as target_week , 
  100 retailer_product_identifier ,
  m.key_value as  src_item_dim_key ,
  965 as venue_number ,
  max(m1.key_value) as src_venue_dim_key ,
  1060 as src_consumer_dim_key ,
  0 as base,
  0 as system_lift,
  0 as manual_lift,
  1764211 as retail_item_cd,
  1 as trans_number,
  tm.tm_dim_key
  from
  wh_dimension.time_dim_day tm
   left outer join master_key_lookup as m on (m.alt_key=concat(upper('saveon'),'~',100)) and (m.dimension='item') and (m.retailer='saveon')  
  left outer join master_key_lookup as m1 on (m1.alt_key=concat(upper('saveon'),'~',965)) and (m1.dimension='venue') and (m1.retailer='saveon') 
  left outer join forecast_src_stg as fsp on (tm.tm_end_date=from_unixtime(unix_timestamp(fsp.target_week,'yyyy.MM.dd'), 'MM-dd-yy'))
  left outer join store st on st.location_id=fsp.location_id
  where fsp.target_week is null and tm.tm_dim_key between 41964 and 41969
  group by from_unixtime(unix_timestamp(tm_end_date,'MM-dd-yy'),'yyyy.MM.dd' ),tm.tm_dim_key,
  m.key_value,retail_item_cd;


truncate table plano_no_scan;
 insert into table plano_no_scan
 select distinct m.key_value item_dim_key, m1.key_value venue_dim_key,i.planoid
from saveon_pl_item i
join saveon_pl_product pp on i.ItemID=pp.ItemID
join saveon_productcode_mapping li on li.retailitemid=pp.RetailItemID
join saveon_pl_store s on i.PlanoID=s.PlanoID
join pl_src_stg pl on pl.planoid=i.PlanoID
join master_key_lookup m on m.alt_key=concat('SAVEON~',cast(li.retailer_product_identifier as bigint)) and m.dimension='item'
join master_key_lookup m1 on m1.alt_key=concat('SAVEON~',s.locationid) and m1.dimension='venue';

truncate table plano_no_sales;
insert into plano_no_sales select ps.* from plano_no_scan ps left join
 (select distinct src_item_dim_key, src_venue_dim_key from pos_tx_fact where tm_dim_key_day >='${hiveconf:four_week}') f on ps.item_dim_key=f.src_item_dim_key and ps.venue_dim_key=f.src_venue_dim_key
 where f.src_item_dim_key is null;
