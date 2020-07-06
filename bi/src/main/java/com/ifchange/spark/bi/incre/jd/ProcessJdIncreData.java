package com.ifchange.spark.bi.incre.jd;

import com.ifchange.spark.bi.incre.tob.ProcessTobResumeAndAlgoIncreData;
import com.ifchange.spark.mysql.Mysql;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 处理jd每日增量数据
 * positions
 * positions_algorithms
 * positions_extras
 */
public class ProcessJdIncreData {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessTobResumeAndAlgoIncreData.class);

    private static final Logger POSITIONS_LOG = LoggerFactory.getLogger("POSITIONS_LOGGER");

    private static final Logger POSITIONS_ALGORITHMS_LOG = LoggerFactory.getLogger("POSITIONS_ALGORITHMS_LOGGER");

    private static final Logger POSITIONS_EXTRAS_LOG = LoggerFactory.getLogger("POSITIONS_EXTRAS_LOGGER");

    private static final String HOST_1 = "192.168.8.86";

    private static final String HOST_2 = "192.168.8.88";

    private static final String USERNAME = "databus_user";

    private static final String PASSWORD = "Y,qBpk5vT@zVOwsz";

    private static final int PORT = 3307;

    private static final String DB_NAME_1 = "position_0";

    private static final String DB_NAME_2 = "position_1";

    private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


    public static void main(String[] args) throws Exception {
        int num = Integer.parseInt(args[0]);

        LOG.info("init mysql");
        Mysql mysql1 = new Mysql(USERNAME, PASSWORD, DB_NAME_1, HOST_1, PORT);
        Mysql mysql2 = new Mysql(USERNAME, PASSWORD, DB_NAME_2, HOST_2, PORT);
        long t1 = System.currentTimeMillis();
        Calendar instance = Calendar.getInstance();
        instance.add(Calendar.DATE, -num);
        Date time = instance.getTime();
        LocalDate date = time.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        int year = date.getYear();
        int monthValue = date.getMonthValue();
        int dayOfMonth = date.getDayOfMonth();
        LocalDateTime start = LocalDateTime.of(year, monthValue, dayOfMonth, 0, 0, 1);
        LocalDateTime end = LocalDateTime.of(year, monthValue, dayOfMonth, 23, 59, 59);
        String startTime = DTF.format(start);
        String endTime = DTF.format(end);
        LOG.info("start:{}", startTime);
        LOG.info("end:{}", endTime);

        List<String> idList = new ArrayList<>();
        for (int j = 0; j < 24; j++) {
            idList.clear();
            String db = "position_" + j;
            //positions
            String sql = "select * from `" + db + "`.positions where updated_at>='" + startTime + "' and updated_at<='" + endTime + "'";
            LOG.info(sql);
            List<Map<String, String>> list = j % 2 == 0 ? mysql1.getPositionsList(sql) : mysql2.getPositionsList(sql);
            if (null != list && list.size() > 0) {
                for (Map<String, String> map : list) {
                    String id = map.get("id");
                    String corporationId = map.get("corporation_id");
                    String corporationName = map.get("corporation_name");
                    String name = map.get("name");
                    String cityIds = map.get("city_ids");
                    String architectureName = map.get("architecture_name");
                    String salaryBegin = map.get("salary_begin");
                    String salaryEnd = map.get("salary_end");
                    String dailySalaryBegin = map.get("daily_salary_begin");
                    String dailySalaryEnd = map.get("daily_salary_end");
                    String annualSalaryBegin = map.get("annual_salary_begin");
                    String annualSalaryEnd = map.get("annual_salary_end");
                    String experienceBegin = map.get("experience_begin");
                    String experienceEnd = map.get("experience_end");
                    String degreeId = map.get("degree_id");
                    String degreeIsUp = map.get("degree_is_up");
                    String userId = map.get("user_id");
                    String topId = map.get("top_id");
                    String status = map.get("status");
                    String isShow = map.get("is_show");
                    String isSource = map.get("is_source");
                    String isShort = map.get("is_short");
                    String isAiSite = map.get("is_ai_site");
                    String isDeleted = map.get("is_deleted");
                    String createdAt = map.get("created_at");
                    String updatedAt = map.get("updated_at");
                    String refreshedAt = map.get("refreshed_at");
                    String editedAt = map.get("edited_at");
                    String grabDate = map.get("grab_date");
                    String lastViewTime = map.get("last_view_time");
                    POSITIONS_LOG.info("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                        id, corporationId, corporationName, name, cityIds, architectureName, salaryBegin, salaryEnd, dailySalaryBegin, dailySalaryEnd,
                        annualSalaryBegin, annualSalaryEnd, experienceBegin, experienceEnd, degreeId, degreeIsUp, userId, topId, status, isShow,
                        isSource, isShort, isAiSite, isDeleted, createdAt, updatedAt, refreshedAt, editedAt, grabDate, lastViewTime);
                    idList.add(id);
                }
            }


            for (String id : idList) {
                //positions_algorithms
                String algoSql = "select * from `" + db + "`.positions_algorithms where id=" + id;
                LOG.info(algoSql);
                Map<String, String> map = j % 2 == 0 ? mysql1.getPositionsAlgorithms(algoSql)
                    : mysql2.getPositionsAlgorithms(algoSql);
                if (null != map && map.size() > 0) {
                    String algoId = map.get("id");
                    String jdFunctions = map.get("jd_functions");
                    String jdSchools = map.get("jd_schools");
                    String jdCorporations = map.get("jd_corporations");
                    String jdOriginalCorporations = map.get("jd_original_corporations");
                    String jdFeatures = map.get("jd_features");
                    String jdTags = map.get("jd_tags");
                    String jdTrades = map.get("jd_trades");
                    String jdTitles = map.get("jd_titles");
                    String jdEntities = map.get("jd_entities");
                    String jdAddress = map.get("jd_address");
                    String jdOther = map.get("jd_other");
                    String jdRealCorporations = map.get("jd_real_corporations");
                    String jdComment = map.get("jd_comment");
                    String jdNerSkill = map.get("jd_ner_skill");
                    String humanTags = map.get("human_tags");
                    String isDeleted = map.get("is_deleted");
                    String createdAt = map.get("created_at");
                    String updatedAt = map.get("updated_at");
                    POSITIONS_ALGORITHMS_LOG.info("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                        algoId, jdFunctions, jdSchools, jdCorporations, jdOriginalCorporations, jdFeatures, jdTags, jdTrades, jdTitles, jdEntities,
                        jdAddress, jdOther, jdRealCorporations, jdComment, jdNerSkill, humanTags, isDeleted, createdAt, updatedAt);
                }


                //positions_extras
                String extraSql = "select id," +
                    "REPLACE(REPLACE(REPLACE(email, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as email," +
                    "strategy_type," +
                    "position_type," +
                    "shop_id," +
                    "relation_id," +
                    "REPLACE(REPLACE(REPLACE(category, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as category," +
                    "hunter_industry," +
                    "hunter_suspended," +
                    "hunter_protection_period," +
                    "hunter_pay_end," +
                    "hunter_pay_begin," +
                    "hunter_salary_end," +
                    "hunter_salary_begin," +
                    "category_id," +
                    "REPLACE(REPLACE(REPLACE(real_corporation_name, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as real_corporation_name," +
                    "REPLACE(REPLACE(REPLACE(address, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as address," +
                    "REPLACE(REPLACE(REPLACE(salary, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as salary," +
                    "REPLACE(REPLACE(REPLACE(source_ids, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as source_ids," +
                    "REPLACE(REPLACE(REPLACE(project_ids, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as project_ids," +
                    "REPLACE(REPLACE(REPLACE(languages, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as languages," +
                    "is_oversea," +
                    "recruit_type," +
                    "nature," +
                    "is_inside," +
                    "is_secret," +
                    "number," +
                    "gender," +
                    "REPLACE(REPLACE(REPLACE(profession, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as profession," +
                    "is_pa_researched," +
                    "recommand_resume_count," +
                    "REPLACE(REPLACE(REPLACE(referral_reward, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as referral_reward," +
                    "REPLACE(REPLACE(REPLACE(occupation_commercial_activitie, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as occupation_commercial_activitie," +
                    "REPLACE(REPLACE(REPLACE(company_commercial_activitie, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as company_commercial_activitie," +
                    "is_manager," +
                    "subordinate," +
                    "manager_years," +
                    "REPLACE(REPLACE(REPLACE(tags, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as tags," +
                    "REPLACE(REPLACE(REPLACE(description, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as description," +
                    "REPLACE(REPLACE(REPLACE(requirement, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as requirement," +
                    "REPLACE(REPLACE(REPLACE(department_desc, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as department_desc," +
                    "REPLACE(REPLACE(REPLACE(additional_desc, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as additional_desc," +
                    "REPLACE(REPLACE(REPLACE(processing_rate, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as processing_rate," +
                    "show_count," +
                    "last_source," +
                    "REPLACE(REPLACE(REPLACE(last_source_id, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as last_source_id," +
                    "REPLACE(REPLACE(REPLACE(refreshed_info, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as refreshed_info," +
                    "REPLACE(REPLACE(REPLACE(user_ids, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as user_ids," +
                    "REPLACE(REPLACE(REPLACE(organization, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as organization," +
                    "REPLACE(REPLACE(REPLACE(hp_research, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as hp_research," +
                    "special_period," +
                    "REPLACE(REPLACE(REPLACE(pa_info, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as pa_info," +
                    "REPLACE(REPLACE(REPLACE(custom_data, CHAR(10), '\\\\\\\\n'), CHAR(13), '\\\\\\\\n'),'\\\\t','\\\\\\\\t') as custom_data," +
                    "is_urgent," +
                    "pa_urgent_level," +
                    "is_headhunter," +
                    "is_headhunter_trade," +
                    "is_interpolate," +
                    "is_hot," +
                    "is_deleted," +
                    "created_at," +
                    "updated_at," +
                    "pa_refreshed_at from `" + db + "`.positions_extras where id=" + id;
                Map<String, String> extraMap = j % 2 == 0 ? mysql1.getPositionsExtras(extraSql) : mysql2.getPositionsExtras(extraSql);
                if (null != extraMap && extraMap.size() > 0) {
                    String extraId = extraMap.get("id");
                    String email = extraMap.get("email");
                    String strategyType = extraMap.get("strategy_type");
                    String positionType = extraMap.get("position_type");
                    String shopId = extraMap.get("shop_id");
                    String relationId = extraMap.get("relation_id");
                    String category = extraMap.get("category");
                    String hunterIndustry = extraMap.get("hunter_industry");
                    String hunterSuspended = extraMap.get("hunter_suspended");
                    String hunterProtectionPeriod = extraMap.get("hunter_protection_period");
                    String hunterPayEnd = extraMap.get("hunter_pay_end");
                    String hunterPayBegin = extraMap.get("hunter_pay_begin");
                    String hunterSalaryEnd = extraMap.get("hunter_salary_end");
                    String hunterSalaryBegin = extraMap.get("hunter_salary_begin");
                    String categoryId = extraMap.get("category_id");
                    String realCorporationName = extraMap.get("real_corporation_name");
                    String address = extraMap.get("address");
                    String salary = extraMap.get("salary");
                    String sourceIds = extraMap.get("source_ids");
                    String projectIds = extraMap.get("project_ids");
                    String languages = extraMap.get("languages");
                    String isOversea = extraMap.get("is_oversea");
                    String recruitType = extraMap.get("recruit_type");
                    String nature = extraMap.get("nature");
                    String isInside = extraMap.get("is_inside");
                    String isSecret = extraMap.get("is_secret");
                    String number = extraMap.get("number");
                    String gender = extraMap.get("gender");
                    String profession = extraMap.get("profession");
                    String isPaResearched = extraMap.get("is_pa_researched");
                    String recommandResumeCount = extraMap.get("recommand_resume_count");
                    String referralReward = extraMap.get("referral_reward");
                    String occupationCommercialActivitie = extraMap.get("occupation_commercial_activitie");
                    String companyCommercialActivitie = extraMap.get("company_commercial_activitie");
                    String isManager = extraMap.get("is_manager");
                    String subordinate = extraMap.get("subordinate");
                    String managerYears = extraMap.get("manager_years");
                    String tags = extraMap.get("tags");
                    String description = extraMap.get("description");
                    String requirement = extraMap.get("requirement");
                    String departmentDesc = extraMap.get("department_desc");
                    String additionalDesc = extraMap.get("additional_desc");
                    String processingRate = extraMap.get("processing_rate");
                    String showCount = extraMap.get("show_count");
                    String lastSource = extraMap.get("last_source");
                    String lastSourceId = extraMap.get("last_source_id");
                    String refreshedInfo = extraMap.get("refreshed_info");
                    String userIds = extraMap.get("user_ids");
                    String organization = extraMap.get("organization");
                    String hpResearch = extraMap.get("hp_research");
                    String specialPeriod = extraMap.get("special_period");
                    String paInfo = extraMap.get("pa_info");
                    String customData = extraMap.get("custom_data");
                    String isUrgent = extraMap.get("is_urgent");
                    String paUrgentLevel = extraMap.get("pa_urgent_level");
                    String isHeadhunter = extraMap.get("is_headhunter");
                    String isHeadhunterTrade = extraMap.get("is_headhunter_trade");
                    String isInterpolate = extraMap.get("is_interpolate");
                    String isHot = extraMap.get("is_hot");
                    String isDeleted = extraMap.get("is_deleted");
                    String createdAt = extraMap.get("created_at");
                    String updatedAt = extraMap.get("updated_at");
                    String paRefreshedAt = extraMap.get("pa_refreshed_at");
                    POSITIONS_EXTRAS_LOG.info("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t" +
                            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t" +
                            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t" +
                            "{}\t{}\t{}",
                        extraId, email, strategyType, positionType, shopId, relationId, category, hunterIndustry, hunterSuspended, hunterProtectionPeriod,
                        hunterPayEnd, hunterPayBegin, hunterSalaryEnd, hunterSalaryBegin, categoryId, realCorporationName, address, salary, sourceIds, projectIds,
                        languages, isOversea, recruitType, nature, isInside, isSecret, number, gender, profession, isPaResearched,
                        recommandResumeCount, referralReward, occupationCommercialActivitie, companyCommercialActivitie, isManager, subordinate, managerYears, tags, description, requirement,
                        departmentDesc, additionalDesc, processingRate, showCount, lastSource, lastSourceId, refreshedInfo, userIds, organization, hpResearch,
                        specialPeriod, paInfo, customData, isUrgent, paUrgentLevel, isHeadhunter, isHeadhunterTrade, isInterpolate, isHot, isDeleted,
                        createdAt, updatedAt, paRefreshedAt);
                }
            }
        }
        mysql1.close();
        mysql2.close();
        long t2 = System.currentTimeMillis();
        LOG.info("process position incre use time:{}", (t2 - t1));

    }
}
