set hive.execution.engine=spark;
set mapreduce.job.queuename=root.BDP;

-- 查询315表中的药品数量
-- SELECT count(*) FROM gj_price_315_info;

-- 查询每种药品类型的数量
-- SELECT gj.drug_name, count(*) FROM gj_price_315_info as gj group by gj.drug_name;

-- 查询爬虫药品表和商品信息详情表中的数据进行匹配
--SELECT count(distinct gj.drug_name ) FROM spider.gj_price_315_info gj inner join pdm.t04_merchd_basic_info_tab ma where gj.drug_name = ma.comn_fst_nm ;

--select '咳喘', count(1) from  pdm.t04_merchd_attr_info_tab where merchd_pat_func like '%咳喘%' union all
--select '糖尿病', count(1) from  pdm.t04_merchd_attr_info_tab where merchd_pat_func like '%糖尿病%'  union all
--select '肝胆', count(1) from  pdm.t04_merchd_attr_info_tab where merchd_pat_func like '%肝胆%'  union all
--select '肠胃', count(1) from  pdm.t04_merchd_attr_info_tab where merchd_pat_func like '%肠胃%'  union all
--select '高血压', count(1) from  pdm.t04_merchd_attr_info_tab where merchd_pat_func like '%高血压%';

--查看表的创建信息
--show create table pdm.t04_merchd_attr_info_tab;
--show create table pdm.t04_merchd_cate_tab;
--show create table pdm.t04_merchd_basic_info_tab;
--show create table spider.gj_price_315_info;

--SELECT count(*) FROM spider.gj_price_315_info;

--SELECT count(*) FROM spider.gj_price_315_info where kind is NULL union all

--SELECT count(*) FROM spider.gj_price_315_info where drug_name is NULL union all

--SELECT count(*) FROM spider.gj_price_315_info where indications_illness is NULL union all

--SELECT count(*) FROM spider.gj_price_315_info where functional_management is NULL;

--SELECT count(distinct kind)  FROM spider.gj_price_315_info;

--SELECT count(distinct drug_name)  FROM spider.gj_price_315_info ;

--SELECT count(distinct indications_illness)  FROM spider.gj_price_315_info;

--数据探查-2-0

--SELECT count(prod_big_cls_encd) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(merchd_midcla_encd) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(merchd_sml_cls_encd) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(merchd_sub_cls_encd) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(gua_qua_term) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(hash_value) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(eff_date) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(exp_date) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(data_type) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(erp_corp) FROM pdm.t04_merchd_basic_info_tab;

--2-1


--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where corp_encd == 'null' or corp_encd = 'NULL' or corp_encd is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where merchd_encd == 'null' or merchd_encd = 'NULL' or merchd_encd is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where cwl_merchd_encd == 'null' or cwl_merchd_encd = 'NULL' or cwl_merchd_encd is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where mecd == 'null' or mecd = 'NULL' or mecd is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where comn_fst_nm == 'null' or comn_fst_nm = 'NULL' or comn_fst_nm is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where en_fst_nm == 'null' or en_fst_nm = 'NULL' or en_fst_nm is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where merchd_spc == 'null' or merchd_spc = 'NULL' or merchd_spc is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where std_corp == 'null' or std_corp = 'NULL' or std_corp is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where merchd_dos_for == 'null' or merchd_dos_for = 'NULL' or merchd_dos_for is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where med_ins_ind == 'null' or med_ins_ind = 'NULL' or med_ins_ind is NULL union all

--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where med_ins_encd == 'null' or med_ins_encd = 'NULL' or med_ins_encd is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where med_ins_nm == 'null' or med_ins_nm = 'NULL' or med_ins_nm is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where salitm_tax_rate == 'null' or salitm_tax_rate = 'NULL' or salitm_tax_rate is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where stat == 'null' or stat = 'NULL' or stat is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where opr_mode == 'null' or opr_mode = 'NULL' or opr_mode is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where intl_bar_cd == 'null' or intl_bar_cd = 'NULL' or intl_bar_cd is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where apprv_num == 'null' or apprv_num = 'NULL' or apprv_num is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where rgst_cert_num == 'null' or rgst_cert_num = 'NULL' or rgst_cert_num is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where merchd_sht_nm == 'null' or merchd_sht_nm = 'NULL' or merchd_sht_nm is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where qlty_std == 'null' or qlty_std = 'NULL' or qlty_std is NULL union all


--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where fin_encd == 'null' or fin_encd = 'NULL' or fin_encd is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where prdc_mfr_encd == 'null' or prdc_mfr_encd = 'NULL' or prdc_mfr_encd is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where prod_big_cls_encd == 'null' or prod_big_cls_encd = 'NULL' or prod_big_cls_encd is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where merchd_midcla_encd == 'null' or merchd_midcla_encd = 'NULL' or merchd_midcla_encd is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where merchd_sml_cls_encd == 'null' or merchd_sml_cls_encd = 'NULL' or merchd_sml_cls_encd is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where merchd_sub_cls_encd == 'null' or merchd_sub_cls_encd = 'NULL' or merchd_sub_cls_encd is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where gua_qua_term == 'null' or gua_qua_term = 'NULL' or gua_qua_term is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where hash_value == 'null' or hash_value = 'NULL' or hash_value is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where eff_date == 'null' or eff_date = 'NULL' or eff_date is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_basic_info_tab where exp_date == 'null' or exp_date = 'NULL' or exp_date is NULL;

--2-2

--SELECT count(distinct corp_encd) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct merchd_encd) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct cwl_merchd_encd) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct mecd) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct comn_fst_nm) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct en_fst_nm) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct merchd_spc) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct std_corp) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct merchd_dos_for) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct med_ins_ind) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct med_ins_encd) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct med_ins_nm) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct salitm_tax_rate) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct stat) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct opr_mode) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct intl_bar_cd) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct apprv_num) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct rgst_cert_num) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct merchd_sht_nm) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct qlty_std) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct fin_encd) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct prdc_mfr_encd) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct prod_big_cls_encd) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct merchd_midcla_encd) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct merchd_sml_cls_encd) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct merchd_sub_cls_encd) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct gua_qua_term ) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct hash_value) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT count(distinct eff_date) FROM pdm.t04_merchd_basic_info_tab union all
--SELECT * FROM pdm.t04_merchd_attr_info_tab limit 100;

--3-0

--SELECT count(corp_encd) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(merchd_encd) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(cwl_merchd_encd) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(bar_cd) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(bar_cd_2) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(sell_point) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(pcs_mem) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(majr_compnt) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(merchd_traff_cond) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(merchd_pat_func) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(kep_cond) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(merchd_boxga) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(merchd_pkg_attr) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(hash_value) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(eff_date) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(exp_date) FROM pdm.t04_merchd_attr_info_tab;

--3-1



--SELECT count(*) FROM pdm.t04_merchd_attr_info_tab where corp_encd == 'null' or corp_encd = 'NULL' or corp_encd is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_attr_info_tab where merchd_encd == 'null' or merchd_encd = 'NULL' or merchd_encd is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_attr_info_tab where cwl_merchd_encd == 'null' or cwl_merchd_encd = 'NULL' or cwl_merchd_encd is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_attr_info_tab where bar_cd == 'null' or bar_cd = 'NULL' or bar_cd is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_attr_info_tab where bar_cd_2 == 'null' or bar_cd_2 = 'NULL' or bar_cd_2 is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_attr_info_tab where sell_point == 'null' or sell_point = 'NULL' or sell_point is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_attr_info_tab where pcs_mem == 'null' or pcs_mem = 'NULL' or pcs_mem is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_attr_info_tab where majr_compnt == 'null' or majr_compnt = 'NULL' or majr_compnt is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_attr_info_tab where merchd_traff_cond == 'null' or merchd_traff_cond = 'NULL' or merchd_traff_cond is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_attr_info_tab where merchd_pat_func == 'null' or merchd_pat_func = 'NULL' or merchd_pat_func is NULL union all

--SELECT count(*) FROM pdm.t04_merchd_attr_info_tab where kep_cond == 'null' or kep_cond = 'NULL' or kep_cond is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_attr_info_tab where merchd_boxga == 'null' or merchd_boxga = 'NULL' or merchd_boxga is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_attr_info_tab where merchd_pkg_attr == 'null' or merchd_pkg_attr = 'NULL' or merchd_pkg_attr is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_attr_info_tab where hash_value == 'null' or hash_value = 'NULL' or hash_value is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_attr_info_tab where eff_date == 'null' or eff_date = 'NULL' or eff_date is NULL union all
--SELECT count(*) FROM pdm.t04_merchd_attr_info_tab where exp_date == 'null' or exp_date = 'NULL' or exp_date is NULL ;

--3-2

--SELECT count(distinct corp_encd) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(distinct merchd_encd) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(distinct cwl_merchd_encd) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(distinct bar_cd) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(distinct bar_cd_2) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(distinct sell_point) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(distinct pcs_mem) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(distinct majr_compnt) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(distinct merchd_traff_cond) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(distinct merchd_pat_func) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(distinct kep_cond) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(distinct merchd_boxga) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(distinct merchd_pkg_attr) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(distinct hash_value) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(distinct eff_date) FROM pdm.t04_merchd_attr_info_tab union all
--SELECT count(distinct exp_date) FROM pdm.t04_merchd_attr_info_tab ;

--开始数据探查
--1、匹配药品通用名称

--SELECT * FROM pdm.t04_merchd_basic_info_tab as m, spider.gj_price_315_info as g where m.comn_fst_nm = g.drug_name limit 10;

--SELECT distinct g.kind FROM  spider.gj_price_315_info as g;

--构建维度宽表

select erp_code from pdm.t02_mem_info_tab limit 10;

set hive.execution.engine=mr;