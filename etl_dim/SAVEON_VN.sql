WHENEVER SQLERROR EXIT FAILURE;
WHENEVER SQLERROR EXIT FAILURE;

ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

SET SERVEROUTPUT ON;
SET HEADING OFF ECHO OFF FEEDBACK ON PAGES 0 PAGESIZE 0 TERMOUT OFF VERIFY OFF LINESIZE 110;

delete from saveon_vn_stg s where location_id not in (
select distinct location_id from saveon_vn_all_keys);
commit;

--drop table if exists saveon_store_rank;
create table saveon_store_rank AS
select location_id,location_desc STORENAME ,location_name,DIVISION_DESC, opening_date,
closing_date,location_desc,address_line_1,address_line_2,town_code,province_code,postal_code,
rm_region_id,pricing_zone_id, RANK() OVER (PARTITION BY location_name ORDER BY NVL(CLOSING_DATE,'9999.12.31') DESC) Ranking
from SAVEON_VN_STG ORDER BY location_name;
COMMIT;

--drop table if exists saveon_store_attr;
create table saveon_store_attr as
select * from saveon_store_rank where ranking=1;

drop table saveon_store_rank;


CREATE TABLE SAVEON_VN_STG_MDM AS SELECT
CASE WHEN TRIM(NVL(a.DIVISION_DESC,s.DIVISION_DESC)) IN ('OWT','PRICESMART','URBAN FARE','COOPERS') THEN 'Save-On-Foods'
WHEN TRIM(NVL(a.DIVISION_DESC,s.DIVISION_DESC)) LIKE 'SOF%'  THEN 'Save-On-Foods'
Else NVL(NVL(a.DIVISION_DESC,s.DIVISION_DESC),'UNKNOWN') END AS SX
,a.STORENAME AS STORENAME
,s.location_id
,a.OPENING_DATE
,s.CLOSING_DATE
,a.LOCATION_DESC
,a.ADDRESS_LINE_1
,a.ADDRESS_LINE_2
,a.TOWN_CODE
,a.PROVINCE_CODE
,a.POSTAL_CODE
,NVL(a.DIVISION_DESC,s.DIVISION_DESC)DIVISION_DESC
,LPAD( CAST(NVL(a.RM_REGION_ID,0) AS VARCHAR2(100 BYTE)), 3,'0') RM_REGION_ID
,a.PRICING_ZONE_ID
,LPAD( CAST(a.location_id AS VARCHAR2(100 BYTE)), 4,'0') ||' - '||a.STORENAME AS location_name
,LPAD( CAST(a.location_id AS VARCHAR2(100 BYTE)), 4,'0') padded_store_no
,FILE_DATE
FROM SAVEON_VN_STG s
left join saveon_store_attr a on s.location_name=a.LOCATION_NAME
ORDER BY a.LOCATION_NAME;

drop table saveon_store_attr;
COMMIT;

DROP TABLE SAVEON_VN_STG;
COMMIT;
CREATE TABLE SAVEON_VN_STG AS SELECT * FROM SAVEON_VN_STG_MDM;
COMMIT;

DROP TABLE SAVEON_VN_STG_MDM;
COMMIT;



