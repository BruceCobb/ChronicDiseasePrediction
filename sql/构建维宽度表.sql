set hive.execution.engine=spark;
set mapreduce.job.queuename=root.BDP;

-- create table spider.t05_manbing_table_01 as
insert overwrite table spider.t05_manbing_table_01
select erp_code,merchd_encd,count(merchd_encd) as cnt from
(select tmp.erp_code, sdt.merchd_encd from  pdm.t05_sell_dtl_tab sdt
inner join (select mit.erp_code,sft.sell_form_num from pdm.t02_mem_info_tab mit inner join  pdm.t05_sell_form_tab sft on mit.erp_code = sft.erp_code where mit.data_type = 'open_zipper' and sft.etl_dt = '20180717') tmp 
on sdt.sell_form_num = tmp.sell_form_num where sdt.etl_dt = '20180717') t0 group by t0.erp_code, t0.merchd_encd order by erp_code,cnt desc;

-- create table spider.t05_manbing_table_02 as
insert overwrite table spider.t05_manbing_table_02
select t1.*,t2.comn_fst_nm,t3.kind,t3.functional_management1 as functional_management
from spider.t05_manbing_table_01 t1
inner join (select distinct merchd_encd,comn_fst_nm from pdm.t04_merchd_basic_info_tab where data_type = 'open_zipper') t2 on t1.merchd_encd = t2.merchd_encd
inner join spider.gj_price_315_info_new01 t3 on t2.comn_fst_nm = t3.drug_name
;

-- create table spider.t05_manbing_table_03 as
insert overwrite table spider.t05_manbing_table_03
select erp_code,merchd_encd,cnt,comn_fst_nm,kind,functional_management,
row_number()OVER(partition by erp_code order by cnt desc,merchd_encd) as d_num
from spider.t05_manbing_table_02
order by erp_code;

-- 平均值3.950094903808552，标准差5.778887177324679
-- select avg(d_num),stddev(d_num) 
-- from (select erp_code,max(d_num) as d_num from spider.t05_manbing_table_03 group by erp_code) t;

-- create table spider.t05_manbing_table_04 as
insert overwrite table spider.t05_manbing_table_04
select erp_code
,MAX(case when d_num = 1 then kind else '' end ) d_num_1
,MAX(case when d_num = 2 then kind else '' end ) d_num_2
,MAX(case when d_num = 3 then kind else '' end ) d_num_3
,MAX(case when d_num = 4 then kind else '' end ) d_num_4
,MAX(case when d_num = 5 then kind else '' end ) d_num_5
,MAX(case when d_num = 6 then kind else '' end ) d_num_6
,MAX(case when d_num = 7 then kind else '' end ) d_num_7
,MAX(case when d_num = 8 then kind else '' end ) d_num_8
,MAX(case when d_num = 9 then kind else '' end ) d_num_9
,MAX(case when d_num = 10 then kind else '' end ) d_num_10
,MAX(case when d_num = 11 then kind else '' end ) d_num_11
,MAX(case when d_num = 12 then kind else '' end ) d_num_12
,MAX(case when d_num = 13 then kind else '' end ) d_num_13
,MAX(case when d_num = 14 then kind else '' end ) d_num_14
,MAX(case when d_num = 15 then kind else '' end ) d_num_15
,MAX(case when d_num = 16 then kind else '' end ) d_num_16
,MAX(case when d_num = 1 then functional_management else '' end ) d_func_1
,MAX(case when d_num = 2 then functional_management else '' end ) d_func_2
,MAX(case when d_num = 3 then functional_management else '' end ) d_func_3
,MAX(case when d_num = 4 then functional_management else '' end ) d_func_4
,MAX(case when d_num = 5 then functional_management else '' end ) d_func_5
,MAX(case when d_num = 6 then functional_management else '' end ) d_func_6
,MAX(case when d_num = 7 then functional_management else '' end ) d_func_7
,MAX(case when d_num = 8 then functional_management else '' end ) d_func_8
,MAX(case when d_num = 9 then functional_management else '' end ) d_func_9
,MAX(case when d_num = 10 then functional_management else '' end ) d_func_10
,MAX(case when d_num = 11 then functional_management else '' end ) d_func_11
,MAX(case when d_num = 12 then functional_management else '' end ) d_func_12
,MAX(case when d_num = 13 then functional_management else '' end ) d_func_13
,MAX(case when d_num = 14 then functional_management else '' end ) d_func_14
,MAX(case when d_num = 15 then functional_management else '' end ) d_func_15
,MAX(case when d_num = 16 then functional_management else '' end ) d_func_16
from spider.t05_manbing_table_03
group by erp_code;


 --create table spider.t05_manbing_table_05 as
insert overwrite table spider.t05_manbing_table_05
select erp_code
,case when d_num_1='高血压' or d_num_2='高血压' or d_num_3='高血压' or d_num_4='高血压' or d_num_5='高血压' or d_num_6='高血压' or d_num_7='高血压' or d_num_8='高血压' or d_num_9='高血压' or d_num_7='高血压' or d_num_11='高血压' or d_num_12='高血压' or d_num_13='高血压' or d_num_14='高血压' or d_num_15='高血压' or d_num_16='高血压' then 1 else 0 end drug_1
,case when d_num_1='糖尿病' or d_num_2='糖尿病' or d_num_3='糖尿病' or d_num_4='糖尿病' or d_num_5='糖尿病' or d_num_6='糖尿病' or d_num_7='糖尿病' or d_num_8='糖尿病' or d_num_9='糖尿病' or d_num_7='糖尿病' or d_num_11='糖尿病' or d_num_12='糖尿病' or d_num_13='糖尿病' or d_num_14='糖尿病' or d_num_15='糖尿病' or d_num_16='糖尿病' then 1 else 0 end drug_2
,case when d_num_1='咳喘' or d_num_2='咳喘' or d_num_3='咳喘' or d_num_4='咳喘' or d_num_5='咳喘' or d_num_6='咳喘' or d_num_7='咳喘' or d_num_8='咳喘' or d_num_9='咳喘' or d_num_7='咳喘' or d_num_11='咳喘' or d_num_12='咳喘' or d_num_13='咳喘' or d_num_14='咳喘' or d_num_15='咳喘' or d_num_16='咳喘' then 1 else 0 end drug_3
,case when d_num_1='肠胃' or d_num_2='肠胃' or d_num_3='肠胃' or d_num_4='肠胃' or d_num_5='肠胃' or d_num_6='肠胃' or d_num_7='肠胃' or d_num_8='肠胃' or d_num_9='肠胃' or d_num_7='肠胃' or d_num_11='肠胃' or d_num_12='肠胃' or d_num_13='肠胃' or d_num_14='肠胃' or d_num_15='肠胃' or d_num_16='肠胃' then 1 else 0 end drug_4
,case when d_num_1='肝胆' or d_num_2='肝胆' or d_num_3='肝胆' or d_num_4='肝胆' or d_num_5='肝胆' or d_num_6='肝胆' or d_num_7='肝胆' or d_num_8='肝胆' or d_num_9='肝胆' or d_num_7='肝胆' or d_num_11='肝胆' or d_num_12='肝胆' or d_num_13='肝胆' or d_num_14='肝胆' or d_num_15='肝胆' or d_num_16='肝胆' then 1 else 0 end drug_5
,d_func_1
,d_func_2
,d_func_3
,d_func_4
,d_func_5
,d_func_6
,d_func_7
,d_func_8
,d_func_9
,d_func_10
,d_func_11
,d_func_12
,d_func_13
,d_func_14
,d_func_15
,d_func_16
from spider.t05_manbing_table_04;

--只保留有5类疾病标签其中一种或多种的会员
create table spider.t05_manbing_table_06 as
select * from spider.t05_manbing_table_05 where drug_1<>0 or drug_2<>0 or drug_3<>0 or drug_4<>0 or drug_5<>0;

set hive.execution.engine=mr;
