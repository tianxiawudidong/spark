package com.ifchange.spark.bi.position;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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
 * idw_positions.positions_function
 */
public class ProcessIdwPositionPositionsFunction {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwPositionPositionsFunction.class);

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String positionPath = args[2];
        String positionAlgorithmsPath = args[3];

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


        Dataset<PositionsAlgorithms> positionAlgorithmsDs = sparkSession.read()
            .textFile(positionAlgorithmsPath)
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
                String jdNerSkill=split[14];
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


        //positions left join positions_algorithms
        // * idw_positions.positions_function jdTags

        //joinType - Type of join to perform. Default inner. Must be one of: inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, left_anti
        Dataset<IdwPositionsFunctions> idwPositionsFunctionsDataset = positionDs
            .join(positionAlgorithmsDs, positionDs.col("id").equalTo(positionAlgorithmsDs.col("id")), "left")
            .select(positionDs.col("id"),
                positionAlgorithmsDs.col("jdTags"))
            .flatMap((FlatMapFunction<Row, IdwPositionsFunctions>) row -> {
                List<IdwPositionsFunctions> list = new ArrayList<>();
                long positionId = row.getLong(0);
                String jdTags = null != row.get(1) ? row.getString(1) : "";
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
                                    list.add(idwPositionsFunctions);
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
                                list.add(idwPositionsFunctions);
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
                                list.add(idwPositionsFunctions);
                            }
                        }
                    }
                }
                return list.iterator();
            }, Encoders.bean(IdwPositionsFunctions.class));


        //save data into hive
        idwPositionsFunctionsDataset.write().mode(SaveMode.Overwrite).saveAsTable("idw_positions.positions_function");


    }
}
