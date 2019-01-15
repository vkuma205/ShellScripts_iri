SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table if not exists wh_postx_saveon_p1.saveon_cn_stg as select * from wh_postx_saveon_p1.saveon_cn_src_stg where 1=0;
insert overwrite table wh_postx_saveon_p1.saveon_cn_stg
select * from wh_postx_saveon_p1.saveon_cn_src_stg
UNION ALL
SELECT distinct cx,'0','ON CARD' from wh_postx_saveon_p1.saveon_cn_all_keys;

create table saveon_cn_stg_new as
select cx,
hhn,
(case when cx like '-SAVEON%' OR CX <='0' then 'OFF CARD' else 'ON CARD' end) as status
from wh_postx_saveon_p1.saveon_cn_stg;
DROP TABLE wh_postx_saveon_p1.saveon_cn_stg;
ALTER TABLE wh_postx_saveon_p1.saveon_cn_stg_new RENAME TO wh_postx_saveon_p1.saveon_cn_stg;
