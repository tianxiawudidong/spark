package com.ifchange.spark.bi.position;

import com.alibaba.fastjson.JSONArray;
import com.ifchange.spark.bi.bean.position.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * position idw层数据处理
 * idw_positions.positions_refreshed_info
 */
public class ProcessIdwPositionPositionsRefreshedInfo {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwPositionPositionsRefreshedInfo.class);

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String positionPath = args[2];
        String positionExtraPath = args[3];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        Dataset<Positions> positionDs = sparkSession.read().textFile(positionPath)
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

        Dataset<PositionsExtras> positionExtraDs = sparkSession.read().textFile(positionExtraPath)
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
                String strategyType=split[2];
                try {
                    positionExtras.setStrategyType(Integer.parseInt(strategyType));
                } catch (NumberFormatException e) {
                    positionExtras.setStrategyType(0);
                }
                String positionType=split[3];
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


        //positions left join positions_extras
        // * idw_positions.positions_refreshed_info

        //joinType - Type of join to perform. Default inner. Must be one of: inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, left_anti
        Dataset<IdwPositionsRefreshedInfo> idwPositionsRefreshedInfoDataset = positionDs
            .join(positionExtraDs, positionDs.col("id").equalTo(positionExtraDs.col("id")), "left")
            .select(positionDs.col("id"),
                positionExtraDs.col("refreshedInfo"))
            .flatMap((FlatMapFunction<Row, IdwPositionsRefreshedInfo>) row -> {
                List<IdwPositionsRefreshedInfo> idwPositionsRefreshedInfoList = new ArrayList<>();
                long positionId = row.getLong(0);
                String refreshedInfo = row.getString(1);
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
                return idwPositionsRefreshedInfoList.iterator();
            },Encoders.bean(IdwPositionsRefreshedInfo.class));

        idwPositionsRefreshedInfoDataset.write().mode(SaveMode.Overwrite).saveAsTable("idw_positions.positions_refreshed_info");

    }

}
