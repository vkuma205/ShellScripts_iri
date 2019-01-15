truncate table SAVEON_PL_STG;

insert into SAVEON_PL_STG p select PLANOID  ,FILENAME ,PLANODESC,WIDTH    ,HEIGHT   ,DEPTH    ,CLASS    ,DEPARTMENT, NULL, NULL, EFFECTIVE_DATE from SAVEON_PLANOGRAM_STG;
commit;

insert into SAVEON_PL_STG values ('1','Not Available','Not Available','0','0','0','Not Available','Not Available','Not Available','NOT IN PLANOGRAM','NULL');
commit;

insert into saveon_pl_stg_bk(
select * from saveon_pl_stg where PLANOID not in (select PLANOID from saveon_pl_stg_bk));
commit;

insert into saveon_pl_stg(
select * from saveon_pl_stg_Bk where PLANOID not in (select PLANOID from saveon_pl_stg));
commit;

update SAVEON_PL_STG set filepath = 
'/externaldata01/prd/saveon/rtlr_data/raw/planograms/'||FILENAME||'_'||PLANOID||'.pdf' where planoid<>'1';
commit;

update SAVEON_PL_STG set STATUS=CASE WHEN PLANOID IN (1) THEN'NOT IN PLANOGRAM'
ELSE 'IN PLANOGRAM' END;
COMMIT;

update SAVEON_PL_STG set Department=case when is_number(SUBSTR(class,1,2)) =0 then Department else trim(SUBSTR(class,1,2)) end||'-'||Department ;
commit;

