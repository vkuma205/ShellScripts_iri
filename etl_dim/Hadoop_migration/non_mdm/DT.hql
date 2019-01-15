
use wh_postx_saveon_p1;

SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

delete from SAVEON_DT_STG
where DEPARTMENT_ID in (select DEPARTMENT_ID from SAVEON_DT_SRC_STG);

insert into SAVEON_DT_STG select * from SAVEON_DT_SRC_STG;

