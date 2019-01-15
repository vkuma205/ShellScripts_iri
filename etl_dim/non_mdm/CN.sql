insert into saveon_cn_stg

select distinct cx,'0','ON CARD' from saveon_cn_All_keys
where cx not in (
select cx from saveon_cn_stg);

commit;

update saveon_cn_stg set status = case when cx like '-SAVEON%' OR CX <='0' then 'OFF CARD' else 'ON CARD' end ;
commit;
