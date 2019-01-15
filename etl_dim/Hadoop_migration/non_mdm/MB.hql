
use wh_postx_saveon_p1;

SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

delete from SAVEON_MB_STG
where retailitemid in (select retailitemid from SAVEON_MB_SRC_STG);

insert into SAVEON_MB_STG select * from SAVEON_MB_SRC_STG;

