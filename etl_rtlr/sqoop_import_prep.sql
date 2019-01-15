truncate table saveon_productcode_mapping;

insert into saveon_productcode_mapping select distinct retail_item_cd, upc from eal_dk_saveon_it_mstr where retail_item_cd <> 0;

commit;

truncate table saveon_retail_metric;

insert into saveon_retail_metric  select distinct upc, nvl(retail_metric,0)retail_metric from eal_dk_saveon_it_mstr;

commit;

