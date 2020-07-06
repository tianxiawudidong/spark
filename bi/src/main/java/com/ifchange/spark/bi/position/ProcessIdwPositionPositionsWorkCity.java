package com.ifchange.spark.bi.position;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.bi.bean.position.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


/**
 * position idw层数据处理
 * idw_positions.positions_work_city
 */
public class ProcessIdwPositionPositionsWorkCity {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwPositionPositionsWorkCity.class);

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String positionPath = args[2];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();


        Dataset<IdwPositionsWorkCity> idwPositionsWorkCityDataset = sparkSession.read().textFile(positionPath)
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
            }, Encoders.bean(Positions.class))
            .select("id", "cityIds")
            .map((MapFunction<Row, IdwPositionsWorkCity>) row -> {
                IdwPositionsWorkCity idwPositionsWorkCity = new IdwPositionsWorkCity();
                long positionId = row.getLong(0);
                String cityIds = row.getString(1);
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
                return idwPositionsWorkCity;
            }, Encoders.bean(IdwPositionsWorkCity.class));


        idwPositionsWorkCityDataset.select("position_id", "city_id")
            .distinct()
            .write()
            .mode(SaveMode.Overwrite).saveAsTable("idw_positions.positions_work_city");


    }
}
