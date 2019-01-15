use wh_postx_saveon_p1;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.execution.engine=tez;
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx3277m;
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts=-Xmx3277m;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;

SELECT tr.dayofpurchase as transaction_date,
FROM_UNIXTIME(UNIX_TIMESTAMP(bs.dayofpurchase ,'yyyy.MM.dd HH:mm:ss'), 'HH:mm:ss') as transaction_time,
tr.transactionnumber as trans_number,
tr.productcode as retailer_upc,
tr.productcode as product_key,
ROUND((tr.spending/100),2) as item_gross_amount,
ROUND(((tr.spending-tr.discount)/100),2) as item_net_amount,
ROUND(((tr.spending/100)/tr.quantity),2) as item_list_price,
ROUND(((tr.spending-tr.discount)/tr.quantity/100),2) as item_net_price,
tr.quantity as item_quantity,
tr.weight as item_volume,
ROUND((tr.discount/100),2) as total_discount,
nvl(s.storenumber,bs.storenumber) as venue_id,
bs.registerline as line_id,
tr.cx as customer_id,
tr.poscode as poscode,
im.item_id as item,
im.total_sales_dollars,
im.total_sales_units,
im.points_awarded,
im.points_redeemed,
im.scan_weight,
im.retail_metric,
im.landed_branch_cost,
im.own_brand_ind,
im.brand,
0 shrink_units,
0 shrink_weight,
0 shink_cost,
0 shrink_extended_cost,
0 REASON_CODE
FROM trans_src_stg tr
LEFT OUTER JOIN (select sx, max(storenumber) storenumber from storesin_src_stg group by sx) s on s.sx=tr.sx
LEFT OUTER JOIN basket_src_stg bs
ON FROM_UNIXTIME(UNIX_TIMESTAMP(bs.dayofpurchase_dt ,'yyyyMMdd'), 'yyyy.MM.dd') = tr.dayofpurchase
AND bs.transactionnumber = tr.transactionnumber
AND bs.storenumber=s.storenumber
AND bs.cx=tr.cx
LEFT OUTER JOIN (select  sale_date,store_number,scan_code,
max(item_id) as item_id,
max(total_sales_dollars) total_sales_dollars,
max(total_sales_units) total_sales_units,
max(points_awarded) points_awarded, 
max(points_redeemed) points_redeemed,
max(scan_weight) scan_weight,
max(retail_metric) retail_metric,
max(landed_branch_cost) landed_branch_cost,
max(own_brand_ind) own_brand_ind,
max(brand) brand
from item_move_src_stg
group by sale_date,store_number,scan_code) im
ON FROM_UNIXTIME(UNIX_TIMESTAMP(im.sale_date ,'yyyy.MM.dd HH:mm:ss'), 'yyyy.MM.dd') = tr.dayofpurchase
AND im.store_number = bs.storenumber
AND im.scan_code = tr.productcode
;

