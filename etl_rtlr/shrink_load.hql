use wh_postx_saveon_p1;

ADD FILE /externaldata01/prd/saveon/rtlr_data/common/fact_data_std.py;set hive.exec.dynamic.partition.mode=nonstrict;

 use wh_postx_saveon_p1;  insert into table pos_tx_fact partition(data_supplier_name,retailer_name,tm_dim_key_day)  select TRANSFORM (0,
field1,
field2,
'N/A',
'N/A',
field3,
0,
field13,
field15,
0,
field4,
0,
field5,
field6,
field7,
0.0,
0,
field8,
field9,
field10,
0,
0,
0,
field11,
0,
0,
0,
0.0,
0,
0.0,
'N/A',
0.0,
'N/A',
0.0,
'N/A',
0.0,
'N/A',
0.0,
'N/A',
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
'N/A',
'N/A',
'N/A',
'N/A',
0,
0,
'N/A',
0,
'N/A',
nvl(m.alt_key,-1),
nvl(m.key_value,-1),
0,
0,
nvl(concat(upper(retailer_name),'~',field13),-1),
nvl(m1.key_value,-1),
0,
0,
0,
nvl(m2.alt_key,-1),
nvl(m2.key_value,-1),
0,
nvl(tm.TM_DIM_KEY_WEEK,-1),
0,
0,
0,
0,
0,
'N/A',
'N/A',
0,
field12,
field14,
field16,
field17,
field18,
field19,
field20,
field21,
field22,
field23,
field24,
field25,
field26,
field27,
field28,
field29,
field30,
field31,
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
'N/A',
file_id,
if((field15!='0' and field15!='-1' and  field15 is not null and field15!=''),'YES','NO'),
if((mc.loyalty_card_number is not null ) ,'YES','NO') ,
update_flag,
'SAVEON',
'SAVEON',
nvl(tm.TM_DIM_KEY,-1)) USING 'fact_data_std.py yyyy.MM.dd HHMISS ' AS ( basket_identifier bigint,
transaction_date string,
transaction_time string,
hour_of_day_key string,
day_of_week_key string,
trans_number decimal(20,0),
terminal_identifier bigint,
venue_number bigint,
loyalty_card_number string,
line_identifier bigint,
retailer_upc_ean string,
iri_upc bigint,
retailer_product_identifier string,
item_gross_amount decimal(32,8),
item_net_amount decimal(32,8),
total_discount_amount decimal(32,8),
item_non_volume_based_quantity bigint,
item_list_price decimal(32,8),
item_net_price decimal(32,8),
item_quantity bigint,
hicone_quantity bigint,
non_hicone_quantity bigint,
hicone_item_flag bigint,
item_volume_quantity decimal(32,8),
item_volume_count bigint,
item_deal_quantity bigint,
item_sale_quantity bigint,
item_sale_amount decimal(32,8),
supplier_identifier bigint,
discount_1_amt decimal(32,8),
discount_1_type string,
discount_2_amt decimal(32,8),
discount_2_type string,
discount_3_amt decimal(32,8),
discount_3_type string,
discount_4_amt decimal(32,8),
discount_4_type string,
discount_5_amt decimal(32,8),
discount_5_type string,
pos_weekly_causal_bit_map_key bigint,
total_causal bigint,
any_merch_with_sp bigint,
no_merch_with_sp bigint,
any_feat bigint,
feat_and_dsply bigint,
feat_and_or_dsply bigint,
feat_only bigint,
dsply_only bigint,
any_dsply bigint,
any_prc_rdctn bigint,
prc_rdctn_only bigint,
any_splpck bigint,
any_merch_without_sp bigint,
no_merch_without_sp bigint,
spl_pack_only bigint,
cwb_venue_key bigint,
cwb_causal_key bigint,
cwb_trip_mission_key bigint,
cwb_trip_type_key bigint,
cwb_offset_id bigint,
cwb_key_account bigint,
cwb_outlet_code string,
cwb_iri_week string,
cwb_day_key string,
cwb_minute_id string,
derived_system bigint,
derived_vendor bigint,
derived_item string,
derived_gen bigint,
derived_upc10 string,
item_alt_key_txt string,
src_item_dim_key bigint,
synd_item_dim_key bigint,
pre_synd_item_dim_key bigint,
venue_alt_key_txt string,
src_venue_dim_key bigint,
synd_venue_dim_key bigint,
pre_synd_venue_dim_key bigint,
basket_attr_dim_key bigint,
consumer_alt_key_txt string,
src_consumer_dim_key bigint,
pre_consumer_dim_key bigint,
tm_dim_key_week bigint,
iri_experian_hh_dim_key bigint,
experian_id bigint,
tm_mdm_hour_key bigint,
return_flag bigint,
status bigint,
ordering_gtin string,
ordering_gtin_key string,
freshlook_flag bigint,
custom_col1 string,
custom_col2 string,
custom_col3 string,
custom_col4 string,
custom_col5 string,
custom_col6 string,
custom_col7 string,
custom_col8 string,
custom_col9 string,
custom_col10 string,
custom_col11 string,
custom_col12 string,
custom_col13 string,
custom_col14 string,
custom_col15 string,
custom_col16 string,
custom_col17 string,
custom_col18 string,
custom_col19 string,
custom_col20 string,
custom_col21 string,
custom_col22 string,
custom_col23 string,
custom_col24 string,
custom_col25 string,
custom_col26 string,
custom_col27 string,
custom_col28 string,
custom_col29 string,
custom_col30 string,
custom_col31 string,
custom_col32 string,
custom_col33 string,
custom_col34 string,
custom_col35 string,
custom_col36 string,
custom_col37 string,
custom_col38 string,
custom_col39 string,
custom_col40 string,
custom_col41 string,
custom_col42 string,
custom_col43 string,
custom_col44 string,
custom_col45 string,
custom_col46 string,
custom_col47 string,
custom_col48 string,
custom_col49 string,
custom_col50 string,
custom_col51 string,
custom_col52 string,
custom_col53 string,
custom_col54 string,
custom_col55 string,
custom_col56 string,
custom_col57 string,
custom_col58 string,
custom_col59 string,
custom_col60 string,
retailer_supplier_name string,
file_id string,
is_member_card_txn string,
is_manager_card_txn string,
update_flag string,
data_supplier_name string,
retailer_name string,
tm_dim_key_day bigint) from pos_tx_stage as fsp  left outer join (select distinct loyalty_card_number from  wh_postx_saveon_p1.manager_lookup where retailer_name='saveon' and provider_name='saveon') mc on field15=mc.loyalty_card_number left outer join master_key_lookup as m on (m.alt_key=concat(upper(retailer_name),'~',field4)) and (m.dimension='item') and (m.retailer='saveon')  left outer join master_key_lookup as m1 on (m1.alt_key=concat(upper(retailer_name),'~',field13)) and (m1.dimension='venue') and (m1.retailer='saveon') left outer join master_key_lookup as m2 on (m2.alt_key=(CASE WHEN (field15='0' or field15='-1' or  field15 is null or field15='') THEN concat(concat(concat('-',upper(retailer_name),'_',CAST(CAST ( field13 AS BIGINT ) AS STRING )),'_',month(from_unixtime(unix_timestamp(field1,'yyyy.MM.dd'),'yyyy-MM-dd')))) WHEN (mc.loyalty_card_number is null and field15!='0' and field15!='-1' and  field15 is not null and field15!='') then field15 WHEN (mc.loyalty_card_number is  not null and field15!='0' and field15!='-1' and  field15 is not null and field15!='' ) then concat(field15,'_',concat(concat('-',upper(fsp.retailer_name),'_',CAST(CAST ( field13 AS BIGINT ) AS STRING )),'_',month(from_unixtime(unix_timestamp(field1,'yyyy.MM.dd'),'yyyy-MM-dd')))) end )) and (m2.dimension='consumer') and (m2.retailer='saveon')  left outer join wh_dimension.time_dim_day as tm on (tm.tm_end_date=from_unixtime(unix_timestamp(field1,'yyyy.MM.dd'), 'MM-dd-yy')) where fsp.provider_name='saveon' and fsp.retailer_name='saveon' distribute by tm_dim_key_day  sort by src_consumer_dim_key,trans_number,src_item_dim_key;

