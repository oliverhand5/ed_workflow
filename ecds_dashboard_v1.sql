






SELECT *
INTO OUTFILE 'D:/reports/ed/ed_dashboard_1.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n'
FROM (
SELECT 
	-- 'data_type',
	'referral_source', 'referral_mode', 'outcome', 'acuity', 'pseudo_nhs_no', 'age_band', 'gender', 'ethnicity', 'imd_group', 'lsoa', 'practice', 'pcn', 'site', 'provider', 'in_out_county', 'carehome', 'is_day_night', 'is_weekday', 'dow', 'hour', 'week', 'is_winter'
	, 'diag_grp_1', 'diag_grp_2', 'diag_grp_3', 'diag_category', 'county', 'act', 'pop'

UNION ALL

SELECT 
ifnull(sou.sno_source_short,'Not Coded') as 'Referral'
,ifnull(mo.sno_mode_short,'Not Coded') AS 'Arrival_Mode'
,ifnull(dis.sno_discharge_short,'Not Coded') as 'Outcome'
,ifnull(acu.sno_acuity_short,'Not Coded') as 'acuity_group'
,ifnull(ecds.pseudo_nhs_no,'')
,ifnull(age.pc_age_range,'Not Coded') as 'Age Band'
,case when ecds.gender IS NULL then 'Not Coded'
	when ecds.gender = '1' then 'M'
	when ecds.gender = '2' then 'F'
	ELSE 'O' END AS 'Gender'
,eth.ethnicity_description
,ifnull(dep.sno_deprivation_decile,'Not Coded') as 'Imd_decile'
,ecds.lsoa
,p.practice_name
,case 
	when p.practice_ccg_code = '18c' then p.pcn_name_short
	ELSE 'Other PCN' END AS 'pcn'
,t2.Organisation_Name AS 'site'
,t.Organisation_Name as 'provider'
,case when ecds.provider_code IN ('rlq00','rwp00','r1a00') then 'In County' ELSE 'Out of County' END AS 'in_out_county'
,case when ecds.carehome_code IS NULL then '' ELSE 'From Care Home' END AS 'CareHome'
-- ,ecds.arrival_date_time
,case when left(time(ecds.arrival_date_time),2) BETWEEN 8 AND 17 then 'Day (8>6)' ELSE 'Night' END AS 'is_daynight'
,case when WEEKDAY(ecds.arrival_date_time) BETWEEN 0 AND 4 then 'Weekday' ELSE 'Weekend' END AS 'is_weekday'
,case when WEEKDAY(ecds.arrival_date_time) = 0 then 'Mon'
	when WEEKDAY(ecds.arrival_date_time) = 1 then 'Tue'
	when WEEKDAY(ecds.arrival_date_time) = 2 then 'Wed'
	when WEEKDAY(ecds.arrival_date_time) = 3 then 'Thu'
	when WEEKDAY(ecds.arrival_date_time) = 4 then 'Fri'
	when WEEKDAY(ecds.arrival_date_time) = 5 then 'Sat'
	when WEEKDAY(ecds.arrival_date_time) = 6 then 'Sun'
	ELSE 'Not Coded' END AS 'dow'
,HOUR(ecds.arrival_date_time) AS 'hour_of_day'
,c.date_week_start_mon AS 'week_start'
,case when MONTH(ecds.arrival_date_time) IN ('11','12','1','2','3') then 'Winter (Nov-Mar)' ELSE 'Other' END AS 'is_winter'
,ifnull(ecds.diag_group_1,'Not Coded')
,ifnull(ecds.diag_group_2,'Not Coded')
,ifnull(ecds.diag_group_3,'Not Coded')
-- ARI diagnosis - using NHSE reference tables.  See annex for Snomed codes.
,case 
	when ecds.diag_group_1 is NULL then 'Not Coded'
	WHEN (ecds.diag_group_1 = 'Infectious disease' AND ecds.diag_group_2 = 'Respiratory') OR (ecds.diag_group_3 IN ('Bronchiolitis','Croup','Viral wheeze')) THEN 'ARI'
	WHEN (ecds.diag_group_3 = 'Acute COVID-19 [ND]') THEN 'ARI'
	WHEN (ecds.diag_group_1 = 'Medical specialties' AND ecds.diag_group_2 like 'Respiratory%') THEN 'ARI' 
	WHEN ecds.diag_group_3 IN ('stroke','Transient ischaemic attack') then 'Stroke/TIA'
	WHEN ecds.diag_group_3 IN ('Hip (NoF)') then 'Fractured NOF'
	WHEN ecds.diag_group_3 IN ('Dental abscess','Dental caries','Fracture of tooth (complex)','Fracture of tooth (simple)','Loose tooth / teeth','Mouth ulcer','Tooth / teeth removed from socket') then 'Dental'
	else 'Other' END AS 'Category'
,ifnull(prac.county,'Non HWICS') AS 'County'
,COUNT(*) AS 'Act'
,ifnull(sum(pop.TOTAL_PATIENTS),'') AS 'pop'

FROM 50_weekly_data.tb_weekly_ecds ecds
LEFT JOIN REFERENCE.tb_map_gp_practice_to_pcn_to_ccg p ON p.practice_code = ecds.gmp_practice_code
LEFT JOIN REFERENCE.tb_lkp_ods_trusts_and_sites_nat_mini t ON t.Organisation_Code = ecds.site_code
LEFT JOIN REFERENCE.tb_lkp_ods_trusts_and_sites_nat_mini t2 ON t2.Organisation_Code = ecds.provider_code
LEFT JOIN calendar.tb_cal_day_to_week_start c ON c.date = date(ecds.arrival_date_time)
LEFT JOIN 56_workforce.tb_wf_practice_patients pop ON pop.PRAC_CODE = ecds.gmp_practice_code AND pop.rep_month = (SELECT MAX(rep_month) FROM 56_workforce.tb_wf_practice_patients)
LEFT JOIN REFERENCE.tb_lkp_dd_ethnicity eth ON eth.ethnicity_code = ecds.ethnicity
LEFT JOIN REFERENCE.tb_sno_ed_discharge dis ON dis.sno_code = ecds.disposal_destination_code
LEFT JOIN REFERENCE.tb_sno_ed_mode mo ON mo.sno_code = ecds.arrival_mode_code
LEFT JOIN REFERENCE.tb_sno_ed_source sou ON sou.sno_code = ecds.referral_source_code
LEFT JOIN REFERENCE.tb_sno_ed_acuity acu ON acu.sno_code = ecds.acuity
LEFT JOIN REFERENCE.tb_sno_ed_deprivation dep ON dep.sno_code = ecds.imd_group
LEFT JOIN REFERENCE.tb_local_age_reference age ON age.age_at_activity_date = ecds.age
LEFT JOIN REFERENCE.tb_local_pcn_hwccg_short_names prac ON prac.practice_code = ecds.gmp_practice_code

WHERE 1=1
AND ecds.arrival_date_time > '2021-03-31'
-- Handle incomplete weeks, trim to the previous full week.
AND DATE(ecds.arrival_date_time) <=	(SELECT CASE WHEN WEEKDAY(c.date) = 6 THEN c.date ELSE c.date_week_end_sun end FROM calendar.tb_cal_day_to_week_start c WHERE c.date = (SELECT date(MAX(a.arrival_date_time)) FROM 50_weekly_data.tb_weekly_ecds a))

GROUP BY
ifnull(sou.sno_source_short,'Not Coded')
,ifnull(mo.sno_mode_short,'Not Coded')
,ifnull(dis.sno_discharge_short,'Not Coded')
,ifnull(acu.sno_acuity_short,'Not Coded')
,ifnull(ecds.pseudo_nhs_no,'')
,ifnull(age.pc_age_range,'Not Coded')
,case when ecds.gender IS NULL then 'Not Coded'
	when ecds.gender = '1' then 'M'
	when ecds.gender = '2' then 'F'
	ELSE 'O' END 
,eth.ethnicity_description
,ifnull(dep.sno_deprivation_decile,'Not Coded')
,ecds.lsoa
,p.practice_name
,case 
	when p.practice_ccg_code = '18c' then p.pcn_name_short
	ELSE 'Other PCN' END
,t2.Organisation_Name
,t.Organisation_Name
,case when ecds.provider_code IN ('rlq00','rwp00','r1a00') then 'In County' ELSE 'Out of County' END
,case when ecds.carehome_code IS NULL then '' ELSE 'From Care Home' END 
-- ,ecds.arrival_date_time
,case when left(time(ecds.arrival_date_time),2) BETWEEN 8 AND 17 then 'Day (8>6)' ELSE 'Night' END
,case when WEEKDAY(ecds.arrival_date_time) BETWEEN 0 AND 4 then 'Weekday' ELSE 'Weekend' END
,case when WEEKDAY(ecds.arrival_date_time) = 0 then 'Mon'
	when WEEKDAY(ecds.arrival_date_time) = 1 then 'Tue'
	when WEEKDAY(ecds.arrival_date_time) = 2 then 'Wed'
	when WEEKDAY(ecds.arrival_date_time) = 3 then 'Thu'
	when WEEKDAY(ecds.arrival_date_time) = 4 then 'Fri'
	when WEEKDAY(ecds.arrival_date_time) = 5 then 'Sat'
	when WEEKDAY(ecds.arrival_date_time) = 6 then 'Sun'
	ELSE 'Not Coded' END
,HOUR(ecds.arrival_date_time)
,c.date_week_start_mon
,case when MONTH(ecds.arrival_date_time) IN ('11','12','1','2','3') then 'Winter (Nov-Mar)' ELSE 'Other' END
,ifnull(ecds.diag_group_1,'Not Coded')
,ifnull(ecds.diag_group_2,'Not Coded')
,ifnull(ecds.diag_group_3,'Not Coded')
-- ARI diagnosis - using NHSE reference tables.  See annex for Snomed codes.
,case 
	when ecds.diag_group_1 is NULL then 'Not Coded'
	WHEN (ecds.diag_group_1 = 'Infectious disease' AND ecds.diag_group_2 = 'Respiratory') OR (ecds.diag_group_3 IN ('Bronchiolitis','Croup','Viral wheeze')) THEN 'ARI'
	WHEN (ecds.diag_group_3 = 'Acute COVID-19 [ND]') THEN 'ARI'
	WHEN (ecds.diag_group_1 = 'Medical specialties' AND ecds.diag_group_2 like 'Respiratory%') THEN 'ARI' 
	WHEN ecds.diag_group_3 IN ('stroke','Transient ischaemic attack') then 'Stroke/TIA'
	WHEN ecds.diag_group_3 IN ('Hip (NoF)') then 'Fractured NOF'
	WHEN ecds.diag_group_3 IN ('Dental abscess','Dental caries','Fracture of tooth (complex)','Fracture of tooth (simple)','Loose tooth / teeth','Mouth ulcer','Tooth / teeth removed from socket') then 'Dental'
	else 'Other' end
,ifnull(prac.county,'Non HWICS')

) exp;



-- ----------------------------------------------------------------------------------------------------------------------------------------------
-- SPC Dashboard


TRUNCATE TABLE 48_r_sql.tb_spc_in_multi;
INSERT INTO 48_r_sql.tb_spc_in_multi (
	SELECT 
	NULL
	,case 
		when ed.diag_group_1 is NULL then 'Not Coded'
		WHEN (ed.diag_group_1 = 'Infectious disease' AND ed.diag_group_2 = 'Respiratory') OR (ed.diag_group_3 IN ('Bronchiolitis','Croup','Viral wheeze')) THEN 'ARI'
		WHEN (ed.diag_group_3 = 'Acute COVID-19 [ND]') THEN 'ARI'
		WHEN (ed.diag_group_1 = 'Medical specialties' AND ed.diag_group_2 like 'Respiratory%') THEN 'ARI' 
		WHEN ed.diag_group_3 IN ('stroke','Transient ischaemic attack') then 'Stroke/TIA'
		WHEN ed.diag_group_3 IN ('Hip (NoF)') then 'Fractured NOF'
		WHEN ed.diag_group_3 IN ('Dental abscess','Dental caries','Fracture of tooth (complex)','Fracture of tooth (simple)','Loose tooth / teeth','Mouth ulcer','Tooth / teeth removed from socket') then 'Dental'
		else 'Other' end
	,NULL
	,NULL
	,NULL
	,NULL
	,c.date_week_start_mon
	,NULL
	,COUNT(*)
	,case 
		when 1=1 then 'up'
		ELSE 'down' END AS 'direction'
	,NULL
	,NULL	
	
	FROM 50_weekly_data.tb_weekly_ecds ed
		LEFT JOIN calendar.tb_cal_day_to_week_start c ON c.date = date(ed.arrival_date_time)
	WHERE 1=1
	AND ed.arrival_date_time > '2021-03-31'
	-- Handle incomplete weeks, trim to the previous full week.
	AND DATE(ed.arrival_date_time) <=	(SELECT CASE WHEN WEEKDAY(c.date) = 6 THEN c.date ELSE c.date_week_end_sun end FROM calendar.tb_cal_day_to_week_start c WHERE c.date = (SELECT date(MAX(a.arrival_date_time)) FROM 50_weekly_data.tb_weekly_ecds a))
	
	GROUP BY 
	case 
		when ed.diag_group_1 is NULL then 'Not Coded'
		WHEN (ed.diag_group_1 = 'Infectious disease' AND ed.diag_group_2 = 'Respiratory') OR (ed.diag_group_3 IN ('Bronchiolitis','Croup','Viral wheeze')) THEN 'ARI'
		WHEN (ed.diag_group_3 = 'Acute COVID-19 [ND]') THEN 'ARI'
		WHEN (ed.diag_group_1 = 'Medical specialties' AND ed.diag_group_2 like 'Respiratory%') THEN 'ARI' 
		WHEN ed.diag_group_3 IN ('stroke','Transient ischaemic attack') then 'Stroke/TIA'
		WHEN ed.diag_group_3 IN ('Hip (NoF)') then 'Fractured NOF'
		WHEN ed.diag_group_3 IN ('Dental abscess','Dental caries','Fracture of tooth (complex)','Fracture of tooth (simple)','Loose tooth / teeth','Mouth ulcer','Tooth / teeth removed from socket') then 'Dental'
		else 'Other' end
	,c.date_week_start_mon
);


CALL 48_r_sql.sp_start_process(0, 0);

-- SELECT * FROM 48_r_sql.tb_spc_output o;


-- ----------------------------------------------------------------------------------------------------------------------------------------------
-- Export SPC


SELECT *
INTO OUTFILE 'D:/reports/ed/ed_dashboard_1_spc.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n'
FROM (
SELECT 
	'Area', 'Organisation', 'Line', 'Period', 'Target', 'Value', 'Common Cause', 'Special Above', 'Special Below', 'Mean', 'Ucl', 'lcl', 'Flag_performance', 'Flag_assurance'

UNION ALL

SELECT 
o.field_1 AS 'Area'
,ifnull(o.field_2,'') AS 'Organisation'
,ifnull(o.field_3,'') AS 'Line'
,ifnull(o.`month`,'') AS 'Period'
,ifnull(o.target,'')
,ifnull(o.value,'')
,case when flag = 0 then o.value ELSE '' END AS 'Common Cause'
,case when (orange_is = 'up' and flag = 1) OR (orange_is = 'down' and flag = -1) then o.value ELSE '' END AS 'Special Above'
,case when (orange_is = 'up' and flag = -1) OR (orange_is = 'down' and flag = 1) then o.value ELSE '' END AS 'Special Below'
,ifnull(o.mean,'')
,ifnull(o.ucl,'')
,ifnull(o.lcl,'')
,ifnull(o.flag_performance,'')
,ifnull(o.flag_assurance,'')
FROM 48_r_sql.tb_spc_output o
) exp2;

