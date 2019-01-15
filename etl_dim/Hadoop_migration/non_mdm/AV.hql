
use wh_postx_saveon_p1;

SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
delete from SAVEON_AV_STG
where AP_VENDOR_ID in (select AP_VENDOR_ID from SAVEON_AV_SRC_STG);

insert into SAVEON_AV_STG select * from SAVEON_AV_SRC_STG;

