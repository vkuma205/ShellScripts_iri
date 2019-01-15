
use wh_postx_saveon_p1;

SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

delete from SAVEON_SI_STG
where SX in (select SX from SAVEON_SI_SRC_STG);

insert into SAVEON_SI_STG select * from SAVEON_SI_SRC_STG;
