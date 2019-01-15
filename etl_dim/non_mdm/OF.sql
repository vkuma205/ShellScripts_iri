ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;

create table  saveon_flyervehicle_mstr as
SELECT distinct  PUBLICATION_ID
      ,PUBLICATION_NAME
      ,PUB_ENTITY_ID
      ,PUB_ENTITY_NAME
      ,fv.START_DATE
      ,fv.END_DATE
      --,LOCATIONID
         --,fr1.location_key
         ,nvl(case when LOCATION_ID =7614 then 'Urban Fare' else fr.Region end,'Other') Region
		 , case when fr.Region ='ALG' then '001'
		 when fr.Region ='Base' then '002'
		 when LOCATION_ID =7614 then '003'
		 else '004' end as region_cd
      ,OFFER_ID
      ,fv.RETAIL_ITEM_CD
      ,AD_VEHICLE_ID
      ,AD_VEHICLE_NAME
      ,PAD_SPOT_NUMBER
      ,PAGE_NUMBER
      ,TOTAL_PAGES
      ,SECTION_NUMBER
      ,SECTION_NAME
	  , row_number () over (partition by nvl(case when LOCATION_ID =7614 then 'Urban Fare' else fr.Region end,'Other'),OFFER_ID,RETAIL_ITEM_CD order by PUBLICATION_NAME) as ranking
  FROM SAVEON_FLYERVECHILE_STG  fv
left join saveon_flyer_region  fr
on fr.location_key = fv.LOCATION_ID;
    
    COMMIT;

  delete from saveon_flyervehicle_mstr where ranking> 1;
  COMMIT;

update SAVEON_OFFER_DTL_STG set PROMODESCRIPTION=	case when AdRetail='Bonus Points Coupon' then AWARD_Points||' '||AdRetail 
when AdRetail='Buy A Get Item Free' OR AdRetail='Buy Get' then 'Buy '||first_Buy||' Get '||Free_get||' Free' 
when AdRetail='Buy Get Points MQD' then 'Buy '||MQD_Limit||' Get '||AWARD_Points||' Points' 
when AdRetail='Combo Deal Buy 3 Get Points' OR AdRetail='Points Award' then AWARD_Points||' Points' 
when AdRetail='Redemption' then 'Redeem '||Redeem_Points||' Points' 
when (Min_Purch<='1' OR is_numeric(nvl(Min_Purch,'N'))=0) AND (AdRetail='Spend Get Points' OR AdRetail='Points and Price') AND Sell_Disc_Price is not null  then '$'||CAST(nvl(Sell_Disc_Price,0) AS varchar(10))||' + '||Award_Points||' Points' 
when Min_Purch>'1' AND (AdRetail='Spend Get Points' OR AdRetail='Points and Price') then CAST(Min_Purch AS varchar(10))||' For ' || '$'||CAST(nvl(Sell_Disc_Price,0)AS varchar(10))||' + '||Award_Points||' Points'
when (Min_Purch<='1' OR is_numeric(nvl(Min_Purch,'N'))=0) AND (AdRetail='Spend Get Points' OR AdRetail='Points and Price') AND cents_off_amt is not null  then '$'||CAST(nvl(cents_off_amt,0) AS varchar(10))||' OFF + '||Award_Points||' Points'
when (Min_Purch<='1' OR is_numeric(nvl(Min_Purch,'N'))=0) AND (AdRetail='Spend Get Points' OR AdRetail='Points and Price') AND Percent_off_amt is not null  then CAST(nvl(CAST(Percent_off_amt AS NUMBER),0) AS varchar(10))||'% OFF + '||Award_Points||' Points'
when is_numeric(nvl(Percent_off_amt,'N'))=1 then CAST(Percent_off_amt AS NUMBER)||'% OFF' 
when is_numeric(nvl(cents_off_amt,'N'))=1 then '$'||cents_off_amt||' OFF' 
when is_numeric(nvl(REDEEM_POINTS,'N'))=1 AND NVL(Sell_Disc_Price,'0.00') ='0.00' AND  cents_off_amt IS NULL AND Percent_off_amt IS NULL then 'Redeem '||Redeem_Points||' Points' 
when Min_Purch<='1' OR is_numeric(nvl(Min_Purch,'N'))=0 then '$'||CAST(nvl(Sell_Disc_Price,DISC_PRICE) AS varchar(10))
else CAST(Min_Purch AS varchar(10))||' For $'||CAST(Sell_Disc_Price AS varchar(10)) 
end ;
commit;


truncate table saveon_of_stg;
insert into saveon_of_stg
select distinct d.OFFERID,
d.OFFER_NAME,
d.OFFER_TYPE_ID,
d.ADRETAIL,
REPLACE(d.STARTDATEKEY,'.','') STARTDATEKEY,
REPLACE(d.ENDDATEKEY,'.','') ENDDATEKEY,
d.VIRTUAL_OFFER_FLAG,
d.COUPON_OFFER_FLAG,
d.TIER,
d.INCN_LOCN_TYPE_ID,
d.LOCATIONID,
d.LOCATIONNAME,
d.CUSTOMER_SEGMENT,
d.MIN_PURCH,
d.DISC_PRICE,
d.CENTS_OFF_AMT,
d.PERCENT_OFF_AMT,
d.REG_PRICE_OVR,
d.SELL_UOM_DESC,
d.SELL_DISC_PRICE,
d.SELL_CENTS_OFF_AMT,
d.SELL_REG_PRICE_OVR,
d.FIRST_BUY,
d.SECOND_BUY,
d.THIRD_BUY,
d.FOURTH_BUY,
d.FIFTH_BUY,
d.FREE_GET,
d.AWARD_POINTS,
d.REDEEM_POINTS,
d.SPEND_AMT,
d.MQD_LIMIT,
d.LQD_LIMIT,
d.LEAD_LOCATIONID,
d.PROMODESCRIPTION,
d.THRESHOLD
,case when v. Region is null then 'Not Available' ELSE v. Region END AS Flyer 
,case when v.OFFER_ID is null then 'Not Available' when page_number =1 then 'Front' when page_number =total_pages then 'Back' else 'Inside' end as Page
,case when v.OFFER_ID is null then 'Not Available'  else page_number ||'-'|| SECTION_NUMBER end  as page_section
,case when v.OFFER_ID is null then 'Not Available' else v.PUB_ENTITY_NAME ENd AS Publication_name
, i.retailitemid 
,case when v.OFFER_ID is null then '0' else PAD_SPOT_NUMBER end
,case when v.OFFER_ID is null then '0' else page_number end
,case when v.OFFER_ID is null then 'Not Available' else ad_vehicle_name end
,case when v.OFFER_ID is null then '000' else region_cd end
,case when ad_vehicle_name='Flyer' then 'Flyer' 
 when ad_vehicle_name='Web' then 'Web' 
 when v.Region is null then 'Instore'
 ELSE 'Other' END AS promo_driver,
 CASE WHEN OFFER_ID='1' THEN 'OFF PROMO' ELSE 'ON PROMO' END AS STATUS
FROM SAVEON_OFFER_ITEM_STG i 
INNER JOIN SAVEON_OFFER_DTL_STG d ON i.OfferID = d.OfferID
LEFT JOIN saveon_flyervehicle_mstr v  on v.OFFER_ID=d.OFFERID and v.RETAIL_ITEM_CD=i.RETAILITEMID and v.OFFER_ID=i.OFFERID

   ;
 commit; 
 
drop table saveon_flyervehicle_mstr;

 insert into saveon_of_stg select * from saveon_of_stg_mstr where offerid not in (select offerid from saveon_of_stg) ;
 commit;
-- where d.offerid=347784940 

delete from saveon_of_stg where enddatekey<'20160201';
commit;


delete FROM saveon_of_stg
WHERE TRIM(flyer)  LIKE 'Other' and rowid NOT IN ( SELECT MAX(ROWID) FROM saveon_of_stg
                     GROUP BY offerid,locationid,retailitemid,REGION_CD )
;
commit;
delete FROM saveon_of_stg
WHERE rowid NOT IN ( SELECT MAX(ROWID) FROM saveon_of_stg
                     GROUP BY offerid,locationid,retailitemid,REGION_CD )
;
commit;

insert into saveon_of_stg_mstr select * from saveon_of_stg where offerid not in (select offerid from saveon_of_stg_mstr) ;
 commit;
