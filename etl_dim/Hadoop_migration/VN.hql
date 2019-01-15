use wh_postx_saveon_p1;

SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

--delete from saveon_vn_stg s where location_id not in (
--select distinct location_id from saveon_vn_all_keys);
--commit;

drop table if exists saveon_store_rank;

create table saveon_store_rank AS
select location_id,location_desc STORENAME ,location_name,DIVISION_DESC, RANK() OVER (PARTITION BY location_name ORDER BY NVL(CLOSING_DATE,'9999.12.31') DESC) Ranking
from SAVEON_VN_STG ORDER BY location_name;

drop table if exists saveon_store_attr;

create table saveon_store_attr as
select * from saveon_store_rank where ranking=1;

drop table if exists saveon_store_rank;

drop table if exists SAVEON_VN_STG_MDM;

CREATE TABLE SAVEON_VN_STG_MDM AS SELECT
CASE WHEN TRIM(NVL(a.DIVISION_DESC,s.DIVISION_DESC)) IN ('OWT','PRICESMART','URBAN FARE','COOPERS') THEN 'Save-On-Foods'
WHEN TRIM(NVL(a.DIVISION_DESC,s.DIVISION_DESC)) LIKE 'SOF%'  THEN 'Save-On-Foods'
Else NVL(NVL(a.DIVISION_DESC,s.DIVISION_DESC),'UNKNOWN') END AS SX
,a.storename
,s.location_id
,OPENING_DATE
,CLOSING_DATE
,s.LOCATION_DESC
,ADDRESS_LINE_1
,ADDRESS_LINE_2
,TOWN_CODE
,PROVINCE_CODE
,POSTAL_CODE
,NVL(a.DIVISION_DESC,s.DIVISION_DESC)DIVISION_DESC
,LPAD( CAST(NVL(RM_REGION_ID,0) AS STRING), 3,'0') RM_REGION_ID
,PRICING_ZONE_ID
,concat(LPAD( cast(a.location_id as string), 4,'0'),'-',a.storename)  location_name
,FILE_DATE
FROM SAVEON_VN_STG s
left join saveon_store_attr a on s.location_name=a.LOCATION_NAME
order by a.LOCATION_NAME;

drop table saveon_store_attr;


DROP TABLE IF EXISTS SAVEON_VN_STG;


SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE `saveon_vn_stg`(
`sx` string, 
`storename` string, 
`location_id` int, 
`opening_date` string, 
`closing_date` string, 
`location_desc` string, 
`address_line_1` string, 
`address_line_2` string, 
`town_code` string, 
`province_code` string, 
`postal_code` string, 
`division_desc` string, 
`rm_region_id` int, 
`pricing_zone_id` int, 
`location_name` string, 
`file_date` string)
CLUSTERED BY (location_id)
  into 1 buckets
stored as orc 
tblproperties("transactional"="true",'orc.compress'='ZLIB');
;


INSERT INTO SAVEON_VN_STG SELECT * FROM SAVEON_VN_STG_MDM;


DROP TABLE IF EXISTS SAVEON_VN_STG_MDM;




