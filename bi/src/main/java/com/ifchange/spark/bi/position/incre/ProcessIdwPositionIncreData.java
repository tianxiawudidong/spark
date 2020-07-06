package com.ifchange.spark.bi.position.incre;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.bi.bean.cv.IndustryMapping;
import com.ifchange.spark.bi.bean.position.*;
import com.ifchange.spark.bi.bean.position.incre.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;


/**
 * position 增量数据 idw层数据处理
 * idw_positions.positions 依赖 ods_positions.positions ods_positions.positions_extras ods_positions.positions_algorithms
 * idw_positions.positions_function
 * idw_positions.positions_industry
 * idw_positions.positions_refreshed_info
 * idw_positions.positions_work_city
 */
public class ProcessIdwPositionIncreData {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwPositionIncreData.class);

    private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String positionIncrePath = args[2];
        String positionExtraIncrePath = args[3];
        String positionAlgorithmsIncrePath = args[4];
        String industryPath = args[5];
        String day = args[6];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        Calendar instance = Calendar.getInstance();
        int num = 0;
        try {
            num = Integer.parseInt(day);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        instance.add(Calendar.DATE, -num);
        Date time = instance.getTime();
        final String date = DTF.format(time.toInstant().atZone(ZoneId.systemDefault()).toLocalDate());

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        //加载industry_mapping /user/hive/warehouse/edw_dim_dimension.db/industry_mapping/part-00000-c0e3973b-671c-4549-a23d-6289f643bc6d-c000.txt
        Dataset<IndustryMapping> industryMappingDs = sparkSession.read().textFile(industryPath)
            .map((MapFunction<String, IndustryMapping>) s -> {
                IndustryMapping industryMapping = new IndustryMapping();
                String[] split = s.split("\t");
                int industryId = Integer.parseInt(split[0]);
                int pindustryId = Integer.parseInt(split[1]);
                int depth = Integer.parseInt(split[2]);
                industryMapping.setId(industryId);
                industryMapping.setPindustry_id(pindustryId);
                industryMapping.setDepth(depth);
                return industryMapping;
            }, Encoders.bean(IndustryMapping.class));

        Dataset<Positions> positionDs = sparkSession.read().textFile(positionIncrePath)
            .filter((FilterFunction<String>) s -> {
                boolean flag = false;
                if (StringUtils.isNoneBlank(s)) {
                    String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                    if (split.length == 30) {
                        String isDeleted = split[23];
                        if (StringUtils.equals("N", isDeleted)) {
                            flag = true;
                        } else {
                            LOG.info("position is_deleted:{} not equal N", isDeleted);
                        }
                    } else {
                        LOG.error("position size is wrong,{}", split.length);
                    }
                } else {
                    LOG.error("position data is empty,{}", s);
                }
                return flag;
            }).map((MapFunction<String, Positions>) s -> {
                Positions positions = new Positions();
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                String id = split[0];
                LOG.info("position id:{}", id);
                try {
                    positions.setId(Long.parseLong(id.trim()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                String corporationId = split[1];
                try {
                    positions.setCorporationId(Integer.parseInt(corporationId));
                } catch (Exception e) {
                    positions.setCorporationId(0);
                }
                String corporationName = split[2];
                positions.setCorporationName(corporationName);
                String name = split[3];
                positions.setName(name);
                String cityIds = split[4];
                positions.setCityIds(cityIds);
                String architectureName = split[5];
                positions.setArchitectureName(architectureName);
                String salaryBegin = split[6];
                try {
                    positions.setSalaryBegin(Integer.parseInt(salaryBegin));
                } catch (Exception e) {
                    positions.setSalaryBegin(0);
                }
                String salaryEnd = split[7];
                try {
                    positions.setSalaryEnd(Integer.parseInt(salaryEnd));
                } catch (Exception e) {
                    positions.setSalaryEnd(0);
                }
                String dailySalaryBegin = split[8];
                try {
                    positions.setDailySalaryBegin(Integer.parseInt(dailySalaryBegin));
                } catch (Exception e) {
                    positions.setDailySalaryBegin(0);
                }
                String dailySalaryEnd = split[9];
                try {
                    positions.setDailySalaryEnd(Integer.parseInt(dailySalaryEnd));
                } catch (Exception e) {
                    positions.setDailySalaryEnd(0);
                }
                String annualSalaryBegin = split[10];
                try {
                    positions.setAnnualSalaryBegin(Integer.parseInt(annualSalaryBegin));
                } catch (Exception e) {
                    positions.setAnnualSalaryBegin(0);
                }
                String annualSalaryEnd = split[11];
                try {
                    positions.setAnnualSalaryEnd(Integer.parseInt(annualSalaryEnd));
                } catch (Exception e) {
                    positions.setAnnualSalaryEnd(0);
                }
                String experienceBegin = split[12];
                try {
                    positions.setExperienceBegin(Integer.parseInt(experienceBegin));
                } catch (Exception e) {
                    positions.setExperienceBegin(0);
                }
                String experienceEnd = split[13];
                try {
                    positions.setExperienceEnd(Integer.parseInt(experienceEnd));
                } catch (Exception e) {
                    positions.setExperienceEnd(0);
                }
                String degreeId = split[14];
                try {
                    positions.setDegreeId(Integer.parseInt(degreeId));
                } catch (Exception e) {
                    positions.setDegreeId(0);
                }
                String degreeIsUp = split[15];
                try {
                    positions.setDegreeIsUp(Integer.parseInt(degreeIsUp));
                } catch (Exception e) {
                    positions.setDegreeIsUp(0);
                }
                String userId = split[16];
                try {
                    positions.setUserId(Integer.parseInt(userId));
                } catch (Exception e) {
                    positions.setUserId(0);
                }
                String topId = split[17];
                try {
                    positions.setTopId(Integer.parseInt(topId));
                } catch (Exception e) {
                    positions.setTopId(0);
                }
                String status = split[18];
                try {
                    positions.setStatus(Integer.parseInt(status));
                } catch (Exception e) {
                    positions.setStatus(0);
                }
                String isShow = split[19];
                positions.setIsShow(isShow);
                String isSource = split[20];
                positions.setIsSource(isSource);
                String isShort = split[21];
                try {
                    positions.setIsShort(Integer.parseInt(isShort));
                } catch (Exception e) {
                    positions.setIsShort(0);
                }
                String isAiSite = split[22];
                try {
                    positions.setIsAiSite(Integer.parseInt(isAiSite));
                } catch (Exception e) {
                    positions.setIsAiSite(0);
                }
                String isDeleted = split[23];
                positions.setIsDeleted(isDeleted);
                String createdAt = split[24];
                positions.setCreatedAt(createdAt);
                String updatedAt = split[25];
                positions.setUpdatedAt(updatedAt);
                String refreshedAt = split[26];
                positions.setRefreshedAt(refreshedAt);
                String editedAt = split[27];
                positions.setEditedAt(editedAt);
                String grabDate = split[28];
                positions.setGrabDate(grabDate);
                String lastViewTime = split[29];
                positions.setLastViewTime(lastViewTime);
                return positions;
            }, Encoders.bean(Positions.class));

        Dataset<PositionsExtras> positionExtraDs = sparkSession.read().textFile(positionExtraIncrePath)
            .filter((FilterFunction<String>) s -> {
                boolean flag = false;
                if (StringUtils.isNoneBlank(s)) {
                    String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                    if (split.length == 63) {
                        String isDeleted = split[59];
                        if (StringUtils.equals("N", isDeleted)) {
                            flag = true;
                        } else {
                            LOG.info("position_extra is_deleted:{} not equal N", isDeleted);
                        }
                    } else {
                        LOG.error("position_extra size is wrong,{}", split.length);
                    }
                } else {
                    LOG.error("position_extra data is empty,{}", s);
                }
                return flag;
            }).map((MapFunction<String, PositionsExtras>) s -> {
                PositionsExtras positionExtras = new PositionsExtras();
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                String id = split[0];
                LOG.info("position_extras id:{}", id);
                try {
                    positionExtras.setId(Long.parseLong(id.trim()));
                } catch (NumberFormatException e) {
                    positionExtras.setId(0);
                }
                String email = split[1];
                positionExtras.setEmail(email);
//                strategy_type                   | tinyint(3) unsigned |
//                    position_type                   | tinyint(3) unsigned |
                String strategyType = split[2];
                try {
                    positionExtras.setStrategyType(Integer.parseInt(strategyType));
                } catch (NumberFormatException e) {
                    positionExtras.setStrategyType(0);
                }
                String positionType = split[3];
                try {
                    positionExtras.setPositionType(Integer.parseInt(positionType));
                } catch (NumberFormatException e) {
                    positionExtras.setPositionType(0);
                }

                String shopId = split[4];
                try {
                    positionExtras.setShopId(Integer.parseInt(shopId));
                } catch (NumberFormatException e) {
                    positionExtras.setShopId(0);
                }
                String relationId = split[5];
                try {
                    positionExtras.setRelationId(Long.parseLong(relationId));
                } catch (NumberFormatException e) {
                    positionExtras.setRelationId(0);
                }
                String category = split[6];
                positionExtras.setCategory(category);
                String hunterIndustry = split[7];
                try {
                    positionExtras.setHunterIndustry(Integer.parseInt(hunterIndustry));
                } catch (NumberFormatException e) {
                    positionExtras.setHunterIndustry(0);
                }
                String hunterSuspended = split[8];
                try {
                    positionExtras.setHunterSuspended(Integer.parseInt(hunterSuspended));
                } catch (NumberFormatException e) {
                    positionExtras.setHunterSuspended(0);
                }
                String hunterProtectionPeriod = split[9];
                try {
                    positionExtras.setHunterProtectionPeriod(Integer.parseInt(hunterProtectionPeriod));
                } catch (NumberFormatException e) {
                    positionExtras.setHunterProtectionPeriod(0);
                }
                String hunterPayEnd = split[10];
                try {
                    positionExtras.setHunterPayEnd(Integer.parseInt(hunterPayEnd));
                } catch (NumberFormatException e) {
                    positionExtras.setHunterPayEnd(0);
                }
                String hunterPayBegin = split[11];
                try {
                    positionExtras.setHunterPayBegin(Integer.parseInt(hunterPayBegin));
                } catch (NumberFormatException e) {
                    positionExtras.setHunterPayBegin(0);
                }
                String hunterSalaryEnd = split[12];
                try {
                    positionExtras.setHunterSalaryEnd(Integer.parseInt(hunterSalaryEnd));
                } catch (NumberFormatException e) {
                    positionExtras.setHunterSalaryEnd(0);
                }
                String hunterSalaryBegin = split[13];
                try {
                    positionExtras.setHunterSalaryBegin(Integer.parseInt(hunterSalaryBegin));
                } catch (NumberFormatException e) {
                    positionExtras.setHunterSalaryBegin(0);
                }
                String categoryId = split[14];
                try {
                    positionExtras.setCategoryId(Integer.parseInt(categoryId));
                } catch (NumberFormatException e) {
                    positionExtras.setCategoryId(0);
                }
                String realCorporationName = split[15];
                positionExtras.setRealCorporationName(realCorporationName);
                String address = split[16];
                positionExtras.setAddress(address);
                String salary = split[17];
                positionExtras.setSalary(salary);
                String sourceIds = split[18];
                positionExtras.setSourceIds(sourceIds);
                String projectIds = split[19];
                positionExtras.setProjectIds(projectIds);
                String languages = split[20];
                positionExtras.setLanguages(languages);
                String isOversea = split[21];
                positionExtras.setIsOversea(isOversea);
                String recruitType = split[22];
                try {
                    positionExtras.setRecruitType(Integer.parseInt(recruitType));
                } catch (NumberFormatException e) {
                    positionExtras.setRecruitType(0);
                }
                String nature = split[23];
                try {
                    positionExtras.setNature(Integer.parseInt(nature));
                } catch (NumberFormatException e) {
                    positionExtras.setNature(0);
                }
                String isInside = split[24];
                try {
                    positionExtras.setIsInside(Integer.parseInt(isInside));
                } catch (NumberFormatException e) {
                    positionExtras.setIsInside(0);
                }
                String isSecret = split[25];
                try {
                    positionExtras.setIsSecret(Integer.parseInt(isSecret));
                } catch (NumberFormatException e) {
                    positionExtras.setIsSecret(0);
                }
                String number = split[26];
                try {
                    positionExtras.setNumber(Integer.parseInt(number));
                } catch (NumberFormatException e) {
                    positionExtras.setNumber(0);
                }
                String gender = split[27];
                try {
                    positionExtras.setGender(Integer.parseInt(gender));
                } catch (NumberFormatException e) {
                    positionExtras.setGender(0);
                }
                String profession = split[28];
                positionExtras.setProfession(profession);
                String isPaResearched = split[29];
                try {
                    positionExtras.setIsPaResearched(Integer.parseInt(isPaResearched));
                } catch (NumberFormatException e) {
                    positionExtras.setIsPaResearched(0);
                }
                String recommandResumeCount = split[30];
                try {
                    positionExtras.setRecommandResumeCount(Integer.parseInt(recommandResumeCount));
                } catch (NumberFormatException e) {
                    positionExtras.setRecommandResumeCount(0);
                }
                String referralReward = split[31];
                positionExtras.setReferralReward(referralReward);
                String occupationCommercialActivitie = split[32];
                positionExtras.setOccupationCommercialActivitie(occupationCommercialActivitie);
                String companyCommercialActivitie = split[33];
                positionExtras.setCompanyCommercialActivitie(companyCommercialActivitie);
                String isManager = split[34];
                positionExtras.setIsManager(isManager);
                String subordinate = split[35];
                try {
                    positionExtras.setSubordinate(Integer.parseInt(subordinate));
                } catch (NumberFormatException e) {
                    positionExtras.setSubordinate(0);
                }
                String managerYears = split[36];
                try {
                    positionExtras.setManagerYears(Integer.parseInt(managerYears));
                } catch (NumberFormatException e) {
                    positionExtras.setManagerYears(0);
                }
                String tags = split[37];
                positionExtras.setTags(tags);
                String description = split[38];
                positionExtras.setDescription(description);
                String requirement = split[39];
                positionExtras.setRequirement(requirement);
                String departmentDesc = split[40];
                positionExtras.setDepartmentDesc(departmentDesc);
                String additionalDesc = split[41];
                positionExtras.setAdditionalDesc(additionalDesc);
                String processingRate = split[42];
                positionExtras.setProcessingRate(processingRate);
                String showCount = split[43];
                try {
                    positionExtras.setShowCount(Integer.parseInt(showCount));
                } catch (NumberFormatException e) {
                    positionExtras.setShowCount(0);
                }
                String lastSource = split[44];
                try {
                    positionExtras.setLastSource(Integer.parseInt(lastSource));
                } catch (NumberFormatException e) {
                    positionExtras.setLastSource(0);
                }
                String lastSourceId = split[45];
                positionExtras.setLastSourceId(lastSourceId);
                String refreshedInfo = split[46];
                positionExtras.setRefreshedInfo(refreshedInfo);
                String userIds = split[47];
                positionExtras.setUserIds(userIds);
                String organization = split[48];
                positionExtras.setOrganization(organization);
                String hpResearch = split[49];
                positionExtras.setHpResearch(hpResearch);
                String specialPeriod = split[50];
                try {
                    positionExtras.setSpecialPeriod(Integer.parseInt(specialPeriod));
                } catch (NumberFormatException e) {
                    positionExtras.setSpecialPeriod(0);
                }
                String paInfo = split[51];
                positionExtras.setPaInfo(paInfo);
                String customData = split[52];
                positionExtras.setCustomData(customData);
                String isUrgent = split[53];
                try {
                    positionExtras.setIsUrgent(Integer.parseInt(isUrgent));
                } catch (NumberFormatException e) {
                    positionExtras.setIsUrgent(0);
                }
                String paUrgentLevel = split[54];
                try {
                    positionExtras.setPaUrgentLevel(Integer.parseInt(paUrgentLevel));
                } catch (NumberFormatException e) {
                    positionExtras.setPaUrgentLevel(0);
                }
                String isHeadhunter = split[55];
                try {
                    positionExtras.setIsHeadhunter(Integer.parseInt(isHeadhunter));
                } catch (NumberFormatException e) {
                    positionExtras.setIsHeadhunter(0);
                }
                String isHeadhunterTrade = split[56];
                try {
                    positionExtras.setIsHeadhunterTrade(Integer.parseInt(isHeadhunterTrade));
                } catch (NumberFormatException e) {
                    positionExtras.setIsHeadhunterTrade(0);
                }
                String isInterpolate = split[57];
                try {
                    positionExtras.setIsInterpolate(Integer.parseInt(isInterpolate));
                } catch (NumberFormatException e) {
                    positionExtras.setIsInterpolate(0);
                }
                String isHot = split[58];
                try {
                    positionExtras.setIsHot(Integer.parseInt(isHot));
                } catch (NumberFormatException e) {
                    positionExtras.setIsHot(0);
                }
                String isDeleted = split[59];
                positionExtras.setIsDeleted(isDeleted);
                String createdAt = split[60];
                positionExtras.setCreatedAt(createdAt);
                String updatedAt = split[61];
                positionExtras.setUpdatedAt(updatedAt);
                String paRefreshedAt = split[62];
                positionExtras.setPaRefreshedAt(paRefreshedAt);
                return positionExtras;
            }, Encoders.bean(PositionsExtras.class));

        Dataset<PositionsAlgorithms> positionAlgorithmsDs = sparkSession.read()
            .textFile(positionAlgorithmsIncrePath)
            .filter((FilterFunction<String>) s -> {
                boolean flag = false;
                if (StringUtils.isNoneBlank(s)) {
                    String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                    if (split.length == 19) {
                        String isDeleted = split[16];
                        if (StringUtils.equals("N", isDeleted)) {
                            flag = true;
                        } else {
                            LOG.info("position_algorithms is_deleted:{} not equal N", isDeleted);
                        }
                    } else {
                        LOG.error("position_algorithms size is wrong,{}", split.length);
                    }
                } else {
                    LOG.error("position_algorithms data is empty,{}", s);
                }
                return flag;
            }).map((MapFunction<String, PositionsAlgorithms>) s -> {
                PositionsAlgorithms positionAlgorithms = new PositionsAlgorithms();
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                String id = split[0];
                LOG.info("position_algorithms id:{}", id);
                try {
                    positionAlgorithms.setId(Long.parseLong(id.trim()));
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                    positionAlgorithms.setId(0);
                }
                String jdFunctions = split[1];
                positionAlgorithms.setJdFunctions(jdFunctions);
                String jdSchools = split[2];
                positionAlgorithms.setJdSchools(jdSchools);
                String jdCorporations = split[3];
                positionAlgorithms.setJdCorporations(jdCorporations);
                String jdOriginalCorporations = split[4];
                positionAlgorithms.setJdOriginalCorporations(jdOriginalCorporations);
                String jdFeatures = split[5];
                positionAlgorithms.setJdFeatures(jdFeatures);
                String jdTags = split[6];
                positionAlgorithms.setJdTags(jdTags);
                String jdTrades = split[7];
                positionAlgorithms.setJdTrades(jdTrades);
                String jdTitles = split[8];
                positionAlgorithms.setJdTitles(jdTitles);
                String jdEntities = split[9];
                positionAlgorithms.setJdEntities(jdEntities);
                String jdAddress = split[10];
                positionAlgorithms.setJdAddress(jdAddress);
                String jdOther = split[11];
                positionAlgorithms.setJdOther(jdOther);
                String jdRealCorporations = split[12];
                positionAlgorithms.setJdRealCorporations(jdRealCorporations);
                String jdComment = split[13];
                positionAlgorithms.setJdComment(jdComment);
                String jdNerSkill = split[14];
                positionAlgorithms.setJdNerSkill(jdNerSkill);
                String humanTags = split[15];
                positionAlgorithms.setHumanTags(humanTags);
                String isDeleted = split[16];
                positionAlgorithms.setIsDeleted(isDeleted);
                String createdAt = split[17];
                positionAlgorithms.setCreatedAt(createdAt);
                String updatedAt = split[18];
                positionAlgorithms.setUpdatedAt(updatedAt);
                return positionAlgorithms;
            }, Encoders.bean(PositionsAlgorithms.class));


        //positions left join positions_extras
        //positions left join positions_algorithms
        // * idw_positions.positions
        // * idw_positions.positions_function jdTags
        // * idw_positions.positions_industry
        // * idw_positions.positions_refreshed_info
        // * idw_positions.positions_work_city

        //joinType - Type of join to perform. Default inner. Must be one of: inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, left_anti
        Dataset<IdwPositionsDataIncre> idwPositionsDataDs = positionDs
            .join(positionExtraDs, positionDs.col("id").equalTo(positionExtraDs.col("id")), "left")
            .join(positionAlgorithmsDs, positionDs.col("id").equalTo(positionAlgorithmsDs.col("id")), "left")
            .select(positionDs.col("id"),
                positionDs.col("corporationId"),
                positionDs.col("salaryBegin"),
                positionDs.col("salaryEnd"),
                positionDs.col("experienceBegin"),
                positionDs.col("experienceEnd"),
                positionDs.col("degreeId"),
                positionDs.col("userId"),
                positionDs.col("status"),
                positionExtraDs.col("number"),
                positionExtraDs.col("gender"),
                positionAlgorithmsDs.col("jdTags"),
                positionAlgorithmsDs.col("jdOriginalCorporations"),
                positionAlgorithmsDs.col("jdRealCorporations"),
                positionDs.col("createdAt"),
                positionDs.col("updatedAt"),
                positionDs.col("refreshedAt"),
                positionDs.col("editedAt"),
                positionExtraDs.col("refreshedInfo"),
                positionDs.col("cityIds"),
                positionDs.col("name"))
            .map((MapFunction<Row, IdwPositionsDataIncre>) row -> {
                IdwPositionsDataIncre idwPositionsDataIncre = new IdwPositionsDataIncre();

                /**
                 * idw_positions_incre
                 */
                IdwPositionsIncre idwPositions = new IdwPositionsIncre();
                idwPositions.setDay(date);
                long positionId = row.getLong(0);
                idwPositions.setPosition_id(positionId);
                int corporationId = null != row.get(1) ? row.getInt(1) : 0;
                LOG.info("id:{},corporationId:{}", positionId, corporationId);
                idwPositions.setCompany_id(corporationId);
                idwPositions.setSalary_begin(row.getInt(2));
                idwPositions.setSalary_end(row.getInt(3));
                idwPositions.setExperience_begin(row.getInt(4));
                idwPositions.setExperience_end(row.getInt(5));
                idwPositions.setDegree_id(row.getInt(6));
                idwPositions.setUser_id(row.getInt(7));
                idwPositions.setStatus(row.getInt(8));
                idwPositions.setNumber(null != row.get(9) ? row.getInt(9) : 0);
                idwPositions.setGender(null != row.get(10) ? row.getInt(10) : 0);
                String jdTags = null != row.get(11) ? row.getString(11) : "";
                String jdOriginalCorporations = null != row.get(12) ? row.getString(12) : "";
                String jdRealCorporations = null != row.get(13) ? row.getString(13) : "";
                LOG.info("jd_tags:{}", jdTags);
                if (StringUtils.isNoneBlank(jdTags)) {
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(jdTags);
                        if (null != jsonObject) {
                            JSONArray refZhiji = jsonObject.getJSONArray("ref_zhiji");
                            if (null != refZhiji && refZhiji.size() > 0) {
                                JSONObject refZhijiJSONObject = refZhiji.getJSONObject(0);
                                if (null != refZhijiJSONObject) {
                                    int bundle = refZhijiJSONObject.getInteger("bundle");
                                    idwPositions.setLevel(bundle);
                                }

                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                //case when jd_real_corporation_id =0 then jd_original_corporation_id    ELSE 0 end  company_id,
                int realCompanyId = 0;
                int originalCompanyId = 0;
                if (StringUtils.isNoneBlank(jdRealCorporations)) {
                    try {
                        JSONArray array = JSONArray.parseArray(jdRealCorporations);
                        if (null != array && array.size() > 0) {
                            JSONObject json = array.getJSONObject(0);
                            JSONObject companyInfo = json.getJSONObject("company_info");
                            realCompanyId = null != companyInfo.get("internal_id") ? companyInfo.getInteger("internal_id") : 0;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                idwPositions.setReal_company_id(realCompanyId);
                if (StringUtils.isNoneBlank(jdOriginalCorporations)) {
                    try {
                        JSONArray array = JSONArray.parseArray(jdOriginalCorporations);
                        if (null != array && array.size() > 0) {
                            JSONObject json = array.getJSONObject(0);
                            JSONObject companyInfo = json.getJSONObject("company_info");
                            originalCompanyId = null != companyInfo.get("internal_id") ? companyInfo.getInteger("internal_id") : 0;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                idwPositions.setOrigin_company_id(originalCompanyId);
//                int companyId = realCompanyId == 0 ? originalCompanyId : 0;
//                idwPositions.setCompany_id(companyId);
                idwPositions.setCreated_at(row.getString(14));
                idwPositions.setUpdated_at(row.getString(15));
                idwPositions.setRefreshed_at(row.getString(16));
                idwPositions.setEdited_at(row.getString(17));
                idwPositions.setPosition_name(row.getString(20));
                idwPositionsDataIncre.setIdwPositionsIncre(idwPositions);


                /**
                 * idw_positions.positions_function_incre
                 */
                List<IdwPositionsFunctionsIncre> idwPositionsFunctionsList = new ArrayList<>();
                if (StringUtils.isNoneBlank(jdTags)) {
                    JSONObject json = JSONObject.parseObject(jdTags);
                    if (null != json) {
                        JSONObject refZhinengMultiJson = json.getJSONObject("ref_zhineng_multi");
                        if (null != refZhinengMultiJson) {
                            //category 二级职能
                            String category = refZhinengMultiJson.getString("category");
                            if (StringUtils.isNoneBlank(category)) {
                                IdwPositionsFunctionsIncre idwPositionsFunctions = new IdwPositionsFunctionsIncre();
                                idwPositionsFunctions.setDay(date);
                                idwPositionsFunctions.setPosition_id(positionId);
                                String[] categorySplit = StringUtils.splitByWholeSeparatorPreserveAllTokens(category, ":");
                                if (categorySplit.length == 2) {
                                    long functionId = Long.parseLong(categorySplit[0]);
                                    double rank = Double.parseDouble(categorySplit[1]);
                                    idwPositionsFunctions.setFunction_id(functionId);
                                    idwPositionsFunctions.setRank(rank);
                                    idwPositionsFunctions.setDepth(2);
                                    idwPositionsFunctionsList.add(idwPositionsFunctions);
                                }
                            }
                            //must 三级职能
                            JSONArray mustArray = refZhinengMultiJson.getJSONArray("must");
                            if (null != mustArray && mustArray.size() > 0) {
                                JSONObject mustJson = mustArray.getJSONObject(0);
                                IdwPositionsFunctionsIncre idwPositionsFunctions = new IdwPositionsFunctionsIncre();
                                long functionId = Long.parseLong(String.valueOf(mustJson.get("function_id")));
                                double rank = Double.parseDouble(mustJson.getString("rank"));
                                idwPositionsFunctions.setDay(date);
                                idwPositionsFunctions.setFunction_id(functionId);
                                idwPositionsFunctions.setRank(rank);
                                idwPositionsFunctions.setDepth(3);
                                idwPositionsFunctions.setPosition_id(positionId);
                                idwPositionsFunctionsList.add(idwPositionsFunctions);
                            }

                            //should 四级职能
                            JSONArray shouldArray = refZhinengMultiJson.getJSONArray("should");
                            if (null != shouldArray && shouldArray.size() > 0) {
                                JSONObject shouldJson = shouldArray.getJSONObject(0);
                                IdwPositionsFunctionsIncre idwPositionsFunctions = new IdwPositionsFunctionsIncre();
                                long functionId = Long.parseLong(String.valueOf(shouldJson.get("function_id")));
                                double rank = Double.parseDouble(shouldJson.getString("rank"));
                                idwPositionsFunctions.setDay(date);
                                idwPositionsFunctions.setFunction_id(functionId);
                                idwPositionsFunctions.setRank(rank);
                                idwPositionsFunctions.setDepth(4);
                                idwPositionsFunctions.setPosition_id(positionId);
                                idwPositionsFunctionsList.add(idwPositionsFunctions);
                            }
                        }
                    }
                }
                idwPositionsDataIncre.setIdwPositionsFunctionsIncreList(idwPositionsFunctionsList);

                /**
                 * idw_positions.positions_industry_temp_incre
                 */
                List<IdwPositionsIndustryTempIncre> idwPositionsIndustryTempList = new ArrayList<>();
                List<Long> jdRealTradeIds = new ArrayList<>();
                if (StringUtils.isNoneBlank(jdRealCorporations)) {
                    try {
                        JSONArray array = JSONArray.parseArray(jdRealCorporations);
                        if (null != array && array.size() > 0) {
                            JSONObject json = array.getJSONObject(0);
                            JSONArray secondTradeList = json.getJSONArray("second_trade_list");
                            JSONArray firstTradeList = json.getJSONArray("first_trade_list");
                            if (!secondTradeList.isEmpty()) {
                                for (int i = 0; i < secondTradeList.size(); i++) {
                                    long value = secondTradeList.getInteger(i).longValue();
                                    jdRealTradeIds.add(value);
                                }
                            } else {
                                if (null != firstTradeList && firstTradeList.size() > 0) {
                                    for (int i = 0; i < firstTradeList.size(); i++) {
                                        long value = firstTradeList.getInteger(i).longValue();
                                        jdRealTradeIds.add(value);
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                List<Long> jdOriginalTradeIds = new ArrayList<>();
                if (StringUtils.isNoneBlank(jdOriginalCorporations)) {
                    try {
                        JSONArray array = JSONArray.parseArray(jdOriginalCorporations);
                        if (null != array && array.size() > 0) {
                            JSONObject json = array.getJSONObject(0);
                            JSONArray secondTradeList = json.getJSONArray("second_trade_list");
                            JSONArray firstTradeList = json.getJSONArray("first_trade_list");
                            if (!secondTradeList.isEmpty()) {
                                for (int i = 0; i < secondTradeList.size(); i++) {
                                    long value = secondTradeList.getInteger(i).longValue();
                                    jdOriginalTradeIds.add(value);
                                }
                            } else {
                                if (null != firstTradeList && firstTradeList.size() > 0) {
                                    for (int i = 0; i < firstTradeList.size(); i++) {
                                        long value = firstTradeList.getInteger(i).longValue();
                                        jdOriginalTradeIds.add(value);
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                //null!=jd_real_trade_ids?jd_real_trade_ids:jd_original_trade_ids
                List<Long> tradeIds = !jdRealTradeIds.isEmpty() ? jdRealTradeIds : jdOriginalTradeIds;
                if (tradeIds.size() > 0) {
                    for (long tradeId : tradeIds) {
                        IdwPositionsIndustryTempIncre idwPositionsIndustryTemp = new IdwPositionsIndustryTempIncre();
                        idwPositionsIndustryTemp.setDay(date);
                        idwPositionsIndustryTemp.setPosition_id(positionId);
                        idwPositionsIndustryTemp.setIndustry_id(tradeId);
                        idwPositionsIndustryTempList.add(idwPositionsIndustryTemp);
                    }
                }
                idwPositionsDataIncre.setIdwPositionsIndustryTempIncreList(idwPositionsIndustryTempList);

                /**
                 * idw_positions.positions_refreshed_info_incre
                 */
                List<IdwPositionsRefreshedInfoIncre> idwPositionsRefreshedInfoList = new ArrayList<>();
                String refreshedInfo = row.getString(18);
                if (StringUtils.isNoneBlank(refreshedInfo)) {
                    try {
                        JSONArray array = JSONArray.parseArray(refreshedInfo);
                        if (null != array && array.size() > 0) {
                            for (int i = 0; i < array.size(); i++) {
                                IdwPositionsRefreshedInfoIncre idwPositionsRefreshedInfo = new IdwPositionsRefreshedInfoIncre();
                                String value = array.getString(i);
                                idwPositionsRefreshedInfo.setDay(date);
                                idwPositionsRefreshedInfo.setPosition_id(positionId);
                                idwPositionsRefreshedInfo.setRefreshed_info(value);
                                idwPositionsRefreshedInfoList.add(idwPositionsRefreshedInfo);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                idwPositionsDataIncre.setIdwPositionsRefreshedInfoIncreList(idwPositionsRefreshedInfoList);

                /**
                 * idw_positions.positions_work_city_incre
                 */
                IdwPositionsWorkCityIncre idwPositionsWorkCity = new IdwPositionsWorkCityIncre();
                String cityIds = row.getString(19);
                if (StringUtils.isNoneBlank(cityIds)) {
                    String cityId = "";
                    JSONObject cityJson = null;
                    try {
                        cityJson = JSONObject.parseObject(cityIds);
                    } catch (Exception e) {
                        try {
                            JSONArray array = JSONArray.parseArray(cityIds);
                            if (null != array && array.size() > 0) {
                                cityJson = array.getJSONObject(0);
                            }
                        } catch (Exception e1) {
                            e1.printStackTrace();
                        }
                    }
                    if (null != cityJson && cityJson.size() > 0) {
                        for (Map.Entry<String, Object> entry : cityJson.entrySet()) {
                            cityId = entry.getKey();
                        }
                    }
                    if (StringUtils.isNoneBlank(cityId) && StringUtils.isNumeric(cityId)) {
                        idwPositionsWorkCity.setDay(date);
                        idwPositionsWorkCity.setPosition_id(positionId);
                        idwPositionsWorkCity.setCity_id(cityId);
                    }
                }
                idwPositionsDataIncre.setIdwPositionsWorkCityIncre(idwPositionsWorkCity);

                return idwPositionsDataIncre;
            }, Encoders.bean(IdwPositionsDataIncre.class));

        idwPositionsDataDs.persist();

        //idw_positions.positions_incre
        Dataset<IdwPositionsIncre> idwPositionsDs = idwPositionsDataDs.map((MapFunction<IdwPositionsDataIncre, IdwPositionsIncre>) IdwPositionsDataIncre::getIdwPositionsIncre, Encoders.bean(IdwPositionsIncre.class));

        //idw_positions.positions_functions_incre
        Dataset<IdwPositionsFunctionsIncre> idwPositionsFunctionsDs = idwPositionsDataDs.flatMap((FlatMapFunction<IdwPositionsDataIncre, IdwPositionsFunctionsIncre>) idwPositionsData -> {
            List<IdwPositionsFunctionsIncre> idwPositionsFunctionsList = idwPositionsData.getIdwPositionsFunctionsIncreList();
            return idwPositionsFunctionsList.iterator();
        }, Encoders.bean(IdwPositionsFunctionsIncre.class));

        //idw_positions.positions_industry_incre
        Dataset<IdwPositionsIndustryTempIncre> idwPositionsIndustryTempDs = idwPositionsDataDs.flatMap((FlatMapFunction<IdwPositionsDataIncre, IdwPositionsIndustryTempIncre>) idwPositionsData -> {
            List<IdwPositionsIndustryTempIncre> idwPositionsIndustryTempList = idwPositionsData.getIdwPositionsIndustryTempIncreList();
            return idwPositionsIndustryTempList.iterator();
        }, Encoders.bean(IdwPositionsIndustryTempIncre.class));


        //idw_positions.positions_refreshed_info_incre
        Dataset<IdwPositionsRefreshedInfoIncre> idwPositionsRefreshedInfoDs = idwPositionsDataDs.flatMap((FlatMapFunction<IdwPositionsDataIncre, IdwPositionsRefreshedInfoIncre>) idwPositionsData -> {
            List<IdwPositionsRefreshedInfoIncre> idwPositionsRefreshedInfoList = idwPositionsData.getIdwPositionsRefreshedInfoIncreList();
            return idwPositionsRefreshedInfoList.iterator();
        }, Encoders.bean(IdwPositionsRefreshedInfoIncre.class));

        //idw_positions.positions_work_city_incre
        Dataset<IdwPositionsWorkCityIncre> idwPositionsWorkCityDs = idwPositionsDataDs.map((MapFunction<IdwPositionsDataIncre, IdwPositionsWorkCityIncre>) IdwPositionsDataIncre::getIdwPositionsWorkCityIncre, Encoders.bean(IdwPositionsWorkCityIncre.class));

        //save data into hive
        idwPositionsDs.write()
            .mode(SaveMode.Append)
            .partitionBy("day")
            .saveAsTable("idw_positions.positions_incre");

        idwPositionsFunctionsDs.write()
            .mode(SaveMode.Append)
            .partitionBy("day")
            .saveAsTable("idw_positions.positions_function_incre");

        idwPositionsIndustryTempDs
            .join(industryMappingDs, idwPositionsIndustryTempDs.col("industry_id").equalTo(industryMappingDs.col("id")), "left")
            .select(idwPositionsIndustryTempDs.col("position_id"),
                industryMappingDs.col("pindustry_id"),
                industryMappingDs.col("depth"),
                idwPositionsIndustryTempDs.col("day"))
            .distinct()
            .filter("pindustry_id is not null")
            .write().mode(SaveMode.Append)
            .partitionBy("day")
            .saveAsTable("idw_positions.positions_industry_incre");

        idwPositionsRefreshedInfoDs.write()
            .mode(SaveMode.Append)
            .partitionBy("day")
            .saveAsTable("idw_positions.positions_refreshed_info_incre");

        idwPositionsWorkCityDs.select("position_id", "city_id", "day")
            .distinct()
            .write()
            .mode(SaveMode.Append)
            .partitionBy("day")
            .saveAsTable("idw_positions.positions_work_city_incre");

        idwPositionsDataDs.unpersist();

    }
}
