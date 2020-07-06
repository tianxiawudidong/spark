package com.ifchange.spark.bi.position;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.bi.bean.cv.IndustryMapping;
import com.ifchange.spark.bi.bean.position.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * 基于meta 元数据  处理idw_positions 相关数据
 * position idw层数据处理
 * idw_positions.positions 依赖 ods_positions.positions ods_positions.positions_extras ods_positions.positions_algorithms
 * idw_positions.positions_function
 * idw_positions.positions_industry
 * idw_positions.positions_refreshed_info
 * idw_positions.positions_work_city
 * <p>
 * <p>
 * 先读positions meta
 * positionExtra meata
 * positionAlgorithms meta
 */
public class ProcessIdwPositionDataV2 {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwPositionData.class);

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String positionPath = args[2];
        String positionMetaPath = args[3];
        String positionExtraPath = args[4];
        String positionExtraMetaPath = args[5];
        String positionAlgorithmsPath = args[6];
        String positionAlgorithmsMetaPath = args[7];
        String industryPath = args[8];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
        SparkContext sc = sparkSession.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);
        List<String> positionsMetaList = sparkSession.read().textFile(positionMetaPath).collectAsList();
        List<String> positionsExtraMetaList = sparkSession.read().textFile(positionExtraMetaPath).collectAsList();
        List<String> positionsAlgorithmsMetaList = sparkSession.read().textFile(positionAlgorithmsMetaPath).collectAsList();

        //读取元数据 并广播
        Broadcast<List<String>> positionMetaList = jsc.broadcast(positionsMetaList);
        Broadcast<List<String>> positionExtraMetaList = jsc.broadcast(positionsExtraMetaList);
        Broadcast<List<String>> positionAlgorithmsMetaList = jsc.broadcast(positionsAlgorithmsMetaList);

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

        Dataset<Positions> positionDs = sparkSession.read().textFile(positionPath)
            .filter((FilterFunction<String>) s -> {
                boolean flag = false;
                //只过滤出is_deleted为N的数据
                List<String> metaList = positionMetaList.value();
                String meta = metaList.get(0);
                JSONArray positionMetaArray = JSONArray.parseArray(meta);
                int index = 0;
                for (int i = 0; i < positionMetaArray.size(); i++) {
                    JSONObject jsonObject = positionMetaArray.getJSONObject(i);
                    String name = jsonObject.getString("name");
                    if (StringUtils.equals(name, "is_deleted")) {
                        index = i;
                        break;
                    }
                }
                if (StringUtils.isNoneBlank(s)) {
                    String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                    String isDeleted = split[index];
                    LOG.info("positions is_deleted:{} index:{}", isDeleted, index);
                    if (StringUtils.equals("N", isDeleted)) {
                        flag = true;
                    } else {
                        LOG.info("position is_deleted:{} not equal N", isDeleted);
                    }
                } else {
                    LOG.error("position data is empty,{}", s);
                }
                return flag;
            }).map((MapFunction<String, Positions>) s -> {
                Positions positions = new Positions();
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                List<String> metaList = positionMetaList.value();
                String meta = metaList.get(0);
                JSONArray positionMetaArray = JSONArray.parseArray(meta);
                for (int i = 0; i < positionMetaArray.size(); i++) {
                    JSONObject jsonObject = positionMetaArray.getJSONObject(i);
                    String name = jsonObject.getString("name");
                    String value = split[i];
                    switch (name) {
                        case "id":
                            try {
                                positions.setId(Long.parseLong(value.trim()));
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            break;
                        case "corporation_id":
                            try {
                                positions.setCorporationId(Integer.parseInt(value));
                            } catch (Exception e) {
                                positions.setCorporationId(0);
                            }
                            break;
                        case "corporation_name":
                            positions.setCorporationName(value);
                            break;
                        case "name":
                            positions.setName(value);
                            break;
                        case "city_ids":
                            positions.setCityIds(value);
                            break;
                        case "architecture_name":
                            positions.setArchitectureName(value);
                            break;
                        case "salary_begin":
                            try {
                                positions.setSalaryBegin(Integer.parseInt(value));
                            } catch (Exception e) {
                                positions.setSalaryBegin(0);
                            }
                            break;
                        case "salary_end":
                            try {
                                positions.setSalaryEnd(Integer.parseInt(value));
                            } catch (Exception e) {
                                positions.setSalaryEnd(0);
                            }
                            break;
                        case "daily_salary_begin":
                            try {
                                positions.setDailySalaryBegin(Integer.parseInt(value));
                            } catch (Exception e) {
                                positions.setDailySalaryBegin(0);
                            }
                            break;
                        case "daily_salary_end":
                            try {
                                positions.setDailySalaryEnd(Integer.parseInt(value));
                            } catch (Exception e) {
                                positions.setDailySalaryEnd(0);
                            }
                            break;
                        case "annual_salary_begin":
                            try {
                                positions.setAnnualSalaryBegin(Integer.parseInt(value));
                            } catch (Exception e) {
                                positions.setAnnualSalaryBegin(0);
                            }
                            break;
                        case "annual_salary_end":
                            try {
                                positions.setAnnualSalaryEnd(Integer.parseInt(value));
                            } catch (Exception e) {
                                positions.setAnnualSalaryEnd(0);
                            }
                            break;
                        case "experience_begin":
                            try {
                                positions.setExperienceBegin(Integer.parseInt(value));
                            } catch (Exception e) {
                                positions.setExperienceBegin(0);
                            }
                            break;
                        case "experience_end":
                            try {
                                positions.setExperienceEnd(Integer.parseInt(value));
                            } catch (Exception e) {
                                positions.setExperienceEnd(0);
                            }
                            break;
                        case "degree_id":
                            try {
                                positions.setDegreeId(Integer.parseInt(value));
                            } catch (Exception e) {
                                positions.setDegreeId(0);
                            }
                            break;
                        case "degree_is_up":
                            try {
                                positions.setDegreeIsUp(Integer.parseInt(value));
                            } catch (Exception e) {
                                positions.setDegreeIsUp(0);
                            }
                            break;
                        case "user_id":
                            try {
                                positions.setUserId(Integer.parseInt(value));
                            } catch (Exception e) {
                                positions.setUserId(0);
                            }
                            break;
                        case "top_id":
                            try {
                                positions.setTopId(Integer.parseInt(value));
                            } catch (Exception e) {
                                positions.setTopId(0);
                            }
                            break;
                        case "status":
                            try {
                                positions.setStatus(Integer.parseInt(value));
                            } catch (Exception e) {
                                positions.setStatus(0);
                            }
                            break;
                        case "is_show":
                            positions.setIsShow(value);
                            break;
                        case "is_source":
                            positions.setIsSource(value);
                            break;
                        case "is_short":
                            try {
                                positions.setIsShort(Integer.parseInt(value));
                            } catch (Exception e) {
                                positions.setIsShort(0);
                            }
                            break;
                        case "is_ai_site":
                            try {
                                positions.setIsAiSite(Integer.parseInt(value));
                            } catch (Exception e) {
                                positions.setIsAiSite(0);
                            }
                            break;
                        case "is_deleted":
                            positions.setIsDeleted(value);
                            break;
                        case "created_at":
                            positions.setCreatedAt(value);
                            break;
                        case "updated_at":
                            positions.setUpdatedAt(value);
                            break;
                        case "refreshed_at":
                            positions.setRefreshedAt(value);
                            break;
                        case "edited_at":
                            positions.setEditedAt(value);
                            break;
                        case "grab_date":
                            positions.setGrabDate(value);
                            break;
                        case "last_view_time":
                            positions.setLastViewTime(value);
                            break;
                        default:
                            break;
                    }
                }
                return positions;
            }, Encoders.bean(Positions.class));

        /*
         * positions_extra
         */
        Dataset<PositionsExtras> positionExtraDs = sparkSession.read().textFile(positionExtraPath)
            .filter((FilterFunction<String>) s -> {
                boolean flag = false;
                List<String> metaList = positionExtraMetaList.value();
                String meta = metaList.get(0);
                JSONArray positionMetaArray = JSONArray.parseArray(meta);
                int index = 0;
                for (int i = 0; i < positionMetaArray.size(); i++) {
                    JSONObject jsonObject = positionMetaArray.getJSONObject(i);
                    String name = jsonObject.getString("name");
                    if (StringUtils.equals(name, "is_deleted")) {
                        index = i;
                        break;
                    }
                }
                if (StringUtils.isNoneBlank(s)) {
                    String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                    String isDeleted = split[index];
                    LOG.info("positions_extra is_deleted:{} index:{}", isDeleted, index);
                    if (StringUtils.equals("N", isDeleted)) {
                        flag = true;
                    } else {
                        LOG.info("position_extra is_deleted:{} not equal N", isDeleted);
                    }
                } else {
                    LOG.error("position_extra data is empty,{}", s);
                }
                return flag;
            }).map((MapFunction<String, PositionsExtras>) s -> {
                PositionsExtras positionExtras = new PositionsExtras();
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                List<String> metaList = positionExtraMetaList.value();
                String meta = metaList.get(0);
                JSONArray positionExtraMetaArray = JSONArray.parseArray(meta);
                for (int i = 0; i < positionExtraMetaArray.size(); i++) {
                    JSONObject jsonObject = positionExtraMetaArray.getJSONObject(i);
                    String name = jsonObject.getString("name");
                    String value = split[i];
                    switch (name) {
                        case "id":
                            try {
                                positionExtras.setId(Long.parseLong(value.trim()));
                            } catch (NumberFormatException e) {
                                positionExtras.setId(0);
                            }
                            break;
                        case "email":
                            positionExtras.setEmail(value);
                            break;
                        case "strategy_type":
                            try {
                                positionExtras.setStrategyType(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setStrategyType(0);
                            }
                            break;
                        case "position_type":
                            try {
                                positionExtras.setPositionType(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setPositionType(0);
                            }
                            break;
                        case "shop_id":
                            try {
                                positionExtras.setShopId(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setShopId(0);
                            }
                            break;
                        case "relation_id":
                            try {
                                positionExtras.setRelationId(Long.parseLong(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setRelationId(0);
                            }
                            break;
                        case "category":
                            positionExtras.setCategory(value);
                            break;
                        case "hunter_industry":
                            try {
                                positionExtras.setHunterIndustry(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setHunterIndustry(0);
                            }
                            break;
                        case "hunter_suspended":
                            try {
                                positionExtras.setHunterSuspended(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setHunterSuspended(0);
                            }
                            break;
                        case "hunter_protection_period":
                            try {
                                positionExtras.setHunterProtectionPeriod(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setHunterProtectionPeriod(0);
                            }
                            break;
                        case "hunter_pay_end":
                            try {
                                positionExtras.setHunterPayEnd(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setHunterPayEnd(0);
                            }
                            break;
                        case "hunter_pay_begin":
                            try {
                                positionExtras.setHunterPayBegin(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setHunterPayBegin(0);
                            }
                            break;
                        case "hunter_salary_end":
                            try {
                                positionExtras.setHunterSalaryEnd(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setHunterSalaryEnd(0);
                            }
                            break;
                        case "hunter_salary_begin":
                            try {
                                positionExtras.setHunterSalaryBegin(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setHunterSalaryBegin(0);
                            }
                            break;
                        case "category_id":
                            try {
                                positionExtras.setCategoryId(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setCategoryId(0);
                            }
                            break;
                        case "real_corporation_name":
                            positionExtras.setRealCorporationName(value);
                            break;
                        case "address":
                            positionExtras.setAddress(value);
                            break;
                        case "salary":
                            positionExtras.setSalary(value);
                            break;
                        case "source_ids":
                            positionExtras.setSourceIds(value);
                            break;
                        case "project_ids":
                            positionExtras.setProjectIds(value);
                            break;
                        case "languages":
                            positionExtras.setLanguages(value);
                            break;
                        case "is_oversea":
                            positionExtras.setIsOversea(value);
                            break;
                        case "recruit_type":
                            try {
                                positionExtras.setRecruitType(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setRecruitType(0);
                            }
                            break;
                        case "nature":
                            try {
                                positionExtras.setNature(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setNature(0);
                            }
                            break;
                        case "is_inside":
                            try {
                                positionExtras.setIsInside(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setIsInside(0);
                            }
                            break;
                        case "is_secret":
                            try {
                                positionExtras.setIsSecret(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setIsSecret(0);
                            }
                            break;
                        case "number":
                            try {
                                positionExtras.setNumber(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setNumber(0);
                            }
                            break;
                        case "gender":
                            try {
                                positionExtras.setGender(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setGender(0);
                            }
                            break;
                        case "profession":
                            positionExtras.setProfession(value);
                            break;
                        case "is_pa_researched":
                            try {
                                positionExtras.setIsPaResearched(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setIsPaResearched(0);
                            }
                            break;
                        case "recommand_resume_count":
                            try {
                                positionExtras.setRecommandResumeCount(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setRecommandResumeCount(0);
                            }
                            break;
                        case "referral_reward":
                            positionExtras.setReferralReward(value);
                            break;
                        case "occupation_commercial_activitie":
                            positionExtras.setOccupationCommercialActivitie(value);
                            break;
                        case "company_commercial_activitie":
                            positionExtras.setCompanyCommercialActivitie(value);
                            break;
                        case "is_manager":
                            positionExtras.setIsManager(value);
                            break;
                        case "subordinate":
                            try {
                                positionExtras.setSubordinate(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setSubordinate(0);
                            }
                            break;
                        case "manager_years":
                            try {
                                positionExtras.setManagerYears(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setManagerYears(0);
                            }
                            break;
                        case "tags":
                            positionExtras.setTags(value);
                            break;
                        case "description":
                            positionExtras.setDescription(value);
                            break;
                        case "requirement":
                            positionExtras.setRequirement(value);
                            break;
                        case "department_desc":
                            positionExtras.setDepartmentDesc(value);
                            break;
                        case "additional_desc":
                            positionExtras.setAdditionalDesc(value);
                            break;
                        case "processing_rate":
                            positionExtras.setProcessingRate(value);
                            break;
                        case "show_count":
                            try {
                                positionExtras.setShowCount(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setShowCount(0);
                            }
                            break;
                        case "last_source":
                            try {
                                positionExtras.setLastSource(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setLastSource(0);
                            }
                            break;
                        case "last_source_id":
                            positionExtras.setLastSourceId(value);
                            break;
                        case "refreshed_info":
                            positionExtras.setRefreshedInfo(value);
                            break;
                        case "user_ids":
                            positionExtras.setUserIds(value);
                            break;
                        case "organization":
                            positionExtras.setOrganization(value);
                            break;
                        case "hp_research":
                            positionExtras.setHpResearch(value);
                            break;
                        case "special_period":
                            try {
                                positionExtras.setSpecialPeriod(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setSpecialPeriod(0);
                            }
                            break;
                        case "pa_info":
                            positionExtras.setPaInfo(value);
                            break;
                        case "custom_data":
                            positionExtras.setCustomData(value);
                            break;
                        case "is_urgent":
                            try {
                                positionExtras.setIsUrgent(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setIsUrgent(0);
                            }
                            break;
                        case "pa_urgent_level":
                            try {
                                positionExtras.setPaUrgentLevel(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setPaUrgentLevel(0);
                            }
                            break;
                        case "is_headhunter":
                            try {
                                positionExtras.setIsHeadhunter(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setIsHeadhunter(0);
                            }
                            break;
                        case "is_headhunter_trade":
                            try {
                                positionExtras.setIsHeadhunterTrade(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setIsHeadhunterTrade(0);
                            }
                            break;
                        case "is_interpolate":
                            try {
                                positionExtras.setIsInterpolate(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setIsInterpolate(0);
                            }
                            break;
                        case "is_hot":
                            try {
                                positionExtras.setIsHot(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                positionExtras.setIsHot(0);
                            }
                            break;
                        case "is_deleted":
                            positionExtras.setIsDeleted(value);
                            break;
                        case "created_at":
                            positionExtras.setCreatedAt(value);
                            break;
                        case "updated_at":
                            positionExtras.setUpdatedAt(value);
                            break;
                        case "pa_refreshed_at":
                            positionExtras.setPaRefreshedAt(value);
                            break;
                        default:
                            break;
                    }
                }
                return positionExtras;
            }, Encoders.bean(PositionsExtras.class));

        /*
         * position_algorithms
         */
        Dataset<PositionsAlgorithms> positionAlgorithmsDs = sparkSession.read()
            .textFile(positionAlgorithmsPath)
            .filter((FilterFunction<String>) s -> {
                boolean flag = false;
                List<String> metaList = positionAlgorithmsMetaList.value();
                String meta = metaList.get(0);
                JSONArray positionMetaArray = JSONArray.parseArray(meta);
                int index = 0;
                for (int i = 0; i < positionMetaArray.size(); i++) {
                    JSONObject jsonObject = positionMetaArray.getJSONObject(i);
                    String name = jsonObject.getString("name");
                    if (StringUtils.equals(name, "is_deleted")) {
                        index = i;
                        break;
                    }
                }
                if (StringUtils.isNoneBlank(s)) {
                    String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                    String isDeleted = split[index];
                    LOG.info("positions_algorithms is_deleted:{} index:{}", isDeleted, index);
                    if (StringUtils.equals("N", isDeleted)) {
                        flag = true;
                    } else {
                        LOG.info("position_algorithms is_deleted:{} not equal N", isDeleted);
                    }
                } else {
                    LOG.error("position_algorithms data is empty,{}", s);
                }
                return flag;
            }).map((MapFunction<String, PositionsAlgorithms>) s -> {
                PositionsAlgorithms positionAlgorithms = new PositionsAlgorithms();
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                List<String> metaList = positionAlgorithmsMetaList.value();
                String meta = metaList.get(0);
                JSONArray positionExtraMetaArray = JSONArray.parseArray(meta);
                for (int i = 0; i < positionExtraMetaArray.size(); i++) {
                    JSONObject jsonObject = positionExtraMetaArray.getJSONObject(i);
                    String name = jsonObject.getString("name");
                    String value = split[i];
                    switch (name) {
                        case "id":
                            try {
                                positionAlgorithms.setId(Long.parseLong(value.trim()));
                            } catch (NumberFormatException e) {
                                e.printStackTrace();
                                positionAlgorithms.setId(0);
                            }
                            break;
                        case "jd_functions":
                            positionAlgorithms.setJdFunctions(value);
                            break;
                        case "jd_schools":
                            positionAlgorithms.setJdSchools(value);
                            break;
                        case "jd_corporations":
                            positionAlgorithms.setJdCorporations(value);
                            break;
                        case "jd_original_corporations":
                            positionAlgorithms.setJdOriginalCorporations(value);
                            break;
                        case "jd_features":
                            positionAlgorithms.setJdFeatures(value);
                            break;
                        case "jd_tags":
                            positionAlgorithms.setJdTags(value);
                            break;
                        case "jd_trades":
                            positionAlgorithms.setJdTrades(value);
                            break;
                        case "jd_titles":
                            positionAlgorithms.setJdTitles(value);
                            break;
                        case "jd_entities":
                            positionAlgorithms.setJdEntities(value);
                            break;
                        case "jd_address":
                            positionAlgorithms.setJdAddress(value);
                            break;
                        case "jd_other":
                            positionAlgorithms.setJdOther(value);
                            break;
                        case "jd_real_corporations":
                            positionAlgorithms.setJdRealCorporations(value);
                            break;
                        case "jd_comment":
                            positionAlgorithms.setJdComment(value);
                            break;
                        case "jd_ner_skill":
                            positionAlgorithms.setJdNerSkill(value);
                            break;
                        case "human_tags":
                            positionAlgorithms.setHumanTags(value);
                            break;
                        case "is_deleted":
                            positionAlgorithms.setIsDeleted(value);
                            break;
                        case "created_at":
                            positionAlgorithms.setCreatedAt(value);
                            break;
                        case "updated_at":
                            positionAlgorithms.setUpdatedAt(value);
                            break;
                        default:
                            break;
                    }
                }
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
        Dataset<IdwPositionsData> idwPositionsDataDs = positionDs
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
            .map((MapFunction<Row, IdwPositionsData>) row -> {
                IdwPositionsData idwPositionsData = new IdwPositionsData();

                /*
                 * idw_positions
                 */
                IdwPositions idwPositions = new IdwPositions();
                long positionId = row.getLong(0);
                idwPositions.setPosition_id(positionId);
                int corporationId = null != row.get(1) ? row.getInt(1) : 0;
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
                idwPositionsData.setIdwPositions(idwPositions);


                /*
                 * idw_positions.positions_function
                 */
                List<IdwPositionsFunctions> idwPositionsFunctionsList = new ArrayList<>();
                if (StringUtils.isNoneBlank(jdTags)) {
                    JSONObject json = JSONObject.parseObject(jdTags);
                    if (null != json) {
                        JSONObject refZhinengMultiJson = json.getJSONObject("ref_zhineng_multi");
                        if (null != refZhinengMultiJson) {
                            //category 二级职能
                            String category = refZhinengMultiJson.getString("category");
                            if (StringUtils.isNoneBlank(category)) {
                                IdwPositionsFunctions idwPositionsFunctions = new IdwPositionsFunctions();
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
                                IdwPositionsFunctions idwPositionsFunctions = new IdwPositionsFunctions();
                                long functionId = Long.parseLong(String.valueOf(mustJson.get("function_id")));
                                double rank = Double.parseDouble(mustJson.getString("rank"));
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
                                IdwPositionsFunctions idwPositionsFunctions = new IdwPositionsFunctions();
                                long functionId = Long.parseLong(String.valueOf(shouldJson.get("function_id")));
                                double rank = Double.parseDouble(shouldJson.getString("rank"));
                                idwPositionsFunctions.setFunction_id(functionId);
                                idwPositionsFunctions.setRank(rank);
                                idwPositionsFunctions.setDepth(4);
                                idwPositionsFunctions.setPosition_id(positionId);
                                idwPositionsFunctionsList.add(idwPositionsFunctions);
                            }
                        }
                    }
                }
                idwPositionsData.setIdwPositionsFunctionsList(idwPositionsFunctionsList);

                /*
                 * idw_positions.positions_industry_temp
                 */
                List<IdwPositionsIndustryTemp> idwPositionsIndustryTempList = new ArrayList<>();
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
                        IdwPositionsIndustryTemp idwPositionsIndustryTemp = new IdwPositionsIndustryTemp();
                        idwPositionsIndustryTemp.setPosition_id(positionId);
                        idwPositionsIndustryTemp.setIndustry_id(tradeId);
                        idwPositionsIndustryTempList.add(idwPositionsIndustryTemp);
                    }
                }
                idwPositionsData.setIdwPositionsIndustryTempList(idwPositionsIndustryTempList);

                /*
                 * idw_positions.positions_refreshed_info
                 */
                List<IdwPositionsRefreshedInfo> idwPositionsRefreshedInfoList = new ArrayList<>();
                String refreshedInfo = row.getString(18);
                if (StringUtils.isNoneBlank(refreshedInfo)) {
                    try {
                        JSONArray array = JSONArray.parseArray(refreshedInfo);
                        if (null != array && array.size() > 0) {
                            for (int i = 0; i < array.size(); i++) {
                                IdwPositionsRefreshedInfo idwPositionsRefreshedInfo = new IdwPositionsRefreshedInfo();
                                String value = array.getString(i);
                                idwPositionsRefreshedInfo.setPosition_id(positionId);
                                idwPositionsRefreshedInfo.setRefreshed_info(value);
                                idwPositionsRefreshedInfoList.add(idwPositionsRefreshedInfo);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                idwPositionsData.setIdwPositionsRefreshedInfoList(idwPositionsRefreshedInfoList);

                /*
                 * idw_positions.positions_work_city
                 */
                IdwPositionsWorkCity idwPositionsWorkCity = new IdwPositionsWorkCity();
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
                        idwPositionsWorkCity.setPosition_id(positionId);
                        idwPositionsWorkCity.setCity_id(cityId);
                    }
                }
                idwPositionsData.setIdwPositionsWorkCity(idwPositionsWorkCity);

                return idwPositionsData;
            }, Encoders.bean(IdwPositionsData.class));

        idwPositionsDataDs.persist();

        //idw_positions.positions
        Dataset<IdwPositions> idwPositionsDs = idwPositionsDataDs.map((MapFunction<IdwPositionsData, IdwPositions>) IdwPositionsData::getIdwPositions, Encoders.bean(IdwPositions.class));

        //idw_positions.positions_functions
        Dataset<IdwPositionsFunctions> idwPositionsFunctionsDs = idwPositionsDataDs.flatMap((FlatMapFunction<IdwPositionsData, IdwPositionsFunctions>) idwPositionsData -> {
            List<IdwPositionsFunctions> idwPositionsFunctionsList = idwPositionsData.getIdwPositionsFunctionsList();
            return idwPositionsFunctionsList.iterator();
        }, Encoders.bean(IdwPositionsFunctions.class));

        //idw_positions.positions_industry
        Dataset<IdwPositionsIndustryTemp> idwPositionsIndustryTempDs = idwPositionsDataDs.flatMap((FlatMapFunction<IdwPositionsData, IdwPositionsIndustryTemp>) idwPositionsData -> {
            List<IdwPositionsIndustryTemp> idwPositionsIndustryTempList = idwPositionsData.getIdwPositionsIndustryTempList();
            return idwPositionsIndustryTempList.iterator();
        }, Encoders.bean(IdwPositionsIndustryTemp.class));


        //idw_positions.positions_refreshed_info
        Dataset<IdwPositionsRefreshedInfo> idwPositionsRefreshedInfoDs = idwPositionsDataDs.flatMap((FlatMapFunction<IdwPositionsData, IdwPositionsRefreshedInfo>) idwPositionsData -> {
            List<IdwPositionsRefreshedInfo> idwPositionsRefreshedInfoList = idwPositionsData.getIdwPositionsRefreshedInfoList();
            return idwPositionsRefreshedInfoList.iterator();
        }, Encoders.bean(IdwPositionsRefreshedInfo.class));

        //idw_positions.positions_work_city
        Dataset<IdwPositionsWorkCity> idwPositionsWorkCityDs = idwPositionsDataDs.map((MapFunction<IdwPositionsData, IdwPositionsWorkCity>) IdwPositionsData::getIdwPositionsWorkCity, Encoders.bean(IdwPositionsWorkCity.class));

        //save data into hive
        idwPositionsDs.write().mode(SaveMode.Overwrite).saveAsTable("idw_positions.positions");

        idwPositionsFunctionsDs.write().mode(SaveMode.Overwrite).saveAsTable("idw_positions.positions_function");

        idwPositionsIndustryTempDs
            .join(industryMappingDs, idwPositionsIndustryTempDs.col("industry_id").equalTo(industryMappingDs.col("id")), "left")
            .select(idwPositionsIndustryTempDs.col("position_id"),
                industryMappingDs.col("pindustry_id"),
                industryMappingDs.col("depth"))
            .distinct()
            .filter("pindustry_id is not null")
            .write().mode(SaveMode.Overwrite).saveAsTable("idw_positions.positions_industry");

        idwPositionsRefreshedInfoDs.write().mode(SaveMode.Overwrite).saveAsTable("idw_positions.positions_refreshed_info");

        idwPositionsWorkCityDs.select("position_id", "city_id")
            .distinct()
            .write()
            .mode(SaveMode.Overwrite).saveAsTable("idw_positions.positions_work_city");

        idwPositionsDataDs.unpersist();

    }
}
