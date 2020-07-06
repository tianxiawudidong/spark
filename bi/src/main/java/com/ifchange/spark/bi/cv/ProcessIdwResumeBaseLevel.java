package com.ifchange.spark.bi.cv;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.bi.bean.cv.Algorithms;
import com.ifchange.spark.bi.bean.cv.BaseLevel;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * idw_resume 数据处理
 * base_level
 */
public class ProcessIdwResumeBaseLevel {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwResumeBaseLevel.class);

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String algoPath = args[2];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        Dataset<BaseLevel> baseLevelDataset = sparkSession.read()
            .textFile(algoPath)
            .filter((FilterFunction<String>) s -> {
                boolean flag = false;
                if (StringUtils.isNoneBlank(s)) {
                    String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                    if (split.length == 4) {
                        String id = split[0];
                        if (StringUtils.isNoneBlank(id) && StringUtils.isNumeric(id)) {
                            flag = true;
                        }
                    }
                }
                return flag;
            }).map((MapFunction<String, Algorithms>) s -> {
                Algorithms algorithms = new Algorithms();
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                try {
                    Long id = Long.parseLong(split[0]);
                    algorithms.setId(id);
                    String json = split[1];
                    if (StringUtils.isNoneBlank(json)) {
                        JSONObject jsonObject = JSONObject.parseObject(json);
                        String cvTitle = jsonObject.getString("cv_title");
                        algorithms.setCvTitle(cvTitle);
                    }
                } catch (Exception e) {
                    LOG.error("parse algorithms data error:{}", e.getMessage());
                    e.printStackTrace();
                }

                String updatedAt = split[2];
                String createdAt = split[3];
                algorithms.setCreatedAt(createdAt);
                algorithms.setUpdatedAt(updatedAt);
                return algorithms;
            }, Encoders.bean(Algorithms.class))
            .flatMap((FlatMapFunction<Algorithms, BaseLevel>) algorithms -> {
                List<BaseLevel> list = new ArrayList<>();
                Long id = algorithms.getId();
                String cvTitle = algorithms.getCvTitle();
                String updatedAt = algorithms.getUpdatedAt();
                if (StringUtils.isNoneBlank(cvTitle) && !StringUtils.equals("\"\"", cvTitle)) {
                    try {
                        JSONObject cvTitleJson = JSONObject.parseObject(cvTitle);
                        if (null != cvTitleJson && cvTitleJson.size() > 0) {
                            for (Map.Entry<String, Object> entry : cvTitleJson.entrySet()) {
                                BaseLevel baseLevel = new BaseLevel();
                                baseLevel.setResume_id(id);
                                baseLevel.setUpdated_at(updatedAt);
                                String wid = entry.getKey();
                                baseLevel.setWid(wid);
                                Object value1 = entry.getValue();
                                if (value1 instanceof Map) {
                                    Map<String, Object> value = (Map<String, Object>) value1;
                                    int level = 0;
                                    try {
                                        level = null != value.get("level")
                                            ? Integer.parseInt(String.valueOf(value.get("level"))) : 0;
                                    } catch (Exception e) {
                                        LOG.error("id:{} cv_title level parse Integer error", id);
                                    }
                                    baseLevel.setLevel(level);
                                }
                                list.add(baseLevel);
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("id:{} algorithms parse json error:{}", id, e.getMessage());
                        e.printStackTrace();
                    }
                }
                return list.iterator();
            }, Encoders.bean(BaseLevel.class))
            .filter((FilterFunction<BaseLevel>) Objects::nonNull);

        baseLevelDataset.write().mode(SaveMode.Overwrite).saveAsTable("idw_resume.base_level");

    }

}
