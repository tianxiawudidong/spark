package com.ifchange.spark.bi.cv;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.bi.bean.cv.Algorithms;
import com.ifchange.spark.bi.bean.cv.BaseFunction;
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
 * base_function
 */
public class ProcessIdwResumeBaseFunction {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwResumeBaseFunction.class);

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String algoPath = args[2];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        Dataset<BaseFunction> baseFunctionDataset = sparkSession.read()
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
                        String cvTag = jsonObject.getString("cv_tag");
                        algorithms.setCvTag(cvTag);
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
            .flatMap((FlatMapFunction<Algorithms, BaseFunction>) algorithms -> {
                List<BaseFunction> baseFunctions = new ArrayList<>();
                long id = algorithms.getId();
                String cvTag = algorithms.getCvTag();
                String updatedAt = algorithms.getUpdatedAt();
                if (StringUtils.isNoneBlank(cvTag) && !StringUtils.equals("\"\"", cvTag)) {
                    try {
                        JSONObject cvTagJson = JSONObject.parseObject(cvTag);
                        if (null != cvTagJson && cvTagJson.size() > 0) {
                            for (Map.Entry<String, Object> entry : cvTagJson.entrySet()) {
                                String wid = entry.getKey();
                                Object value1 = entry.getValue();
                                if (value1 instanceof Map) {
                                    Map<String, Object> value = (Map<String, Object>) value1;
                                    //二级职能
                                    String category = null != value.get("category") ? String.valueOf(value.get("category")) : "";
                                    if (StringUtils.isNoneBlank(category)) {
                                        String[] split = category.split(":");
                                        if (split.length == 2) {
                                            //function_id
                                            Integer functionId = 0;
                                            try {
                                                functionId = Integer.parseInt(split[0]);
                                            } catch (Exception e) {
                                                LOG.error("id:{} cv_tag.category:{} parse int error", id, category);
                                            }
                                            BaseFunction baseFunction = new BaseFunction();
                                            baseFunction.setUpdated_at(updatedAt);
                                            baseFunction.setResume_id(id);
                                            baseFunction.setWid(wid);
                                            baseFunction.setFunction_id(functionId);
                                            //value
                                            Double values = Double.parseDouble(split[1]);
                                            baseFunction.setValue(values);
                                            baseFunctions.add(baseFunction);
                                        }
                                    }

                                    //三级职能
                                    Object must1 = value.get("must");
                                    List<String> mustList = (must1 instanceof List) ? (List<String>) must1 : null;
                                    if (null != mustList && mustList.size() > 0) {
                                        String must = mustList.get(0);
                                        if (StringUtils.isNoneBlank(must)) {
                                            String[] split1 = must.split(":");
                                            if (split1.length == 2) {
                                                BaseFunction baseFunction = new BaseFunction();
                                                baseFunction.setUpdated_at(updatedAt);
                                                baseFunction.setResume_id(id);
                                                baseFunction.setWid(wid);
                                                //function_id
                                                Integer functionId3 = 0;
                                                try {
                                                    functionId3 = Integer.parseInt(split1[0]);
                                                } catch (NumberFormatException e) {
                                                    LOG.error("id:{} cv_tag.must:{} parse int error", id, must1);
                                                }
                                                baseFunction.setFunction_id(functionId3);
                                                //value
                                                Double values3 = Double.parseDouble(split1[1]);
                                                baseFunction.setValue(values3);
                                                baseFunctions.add(baseFunction);
                                            }
                                        }
                                    }

                                    //四级职能
                                    Object should1 = value.get("should");
                                    List<String> shouldList = (should1 instanceof List) ? (List<String>) should1 : null;
                                    if (null != shouldList && shouldList.size() > 0) {
                                        String should = shouldList.get(0);
                                        if (StringUtils.isNoneBlank(should)) {
                                            String[] split2 = should.split(":");
                                            if (split2.length == 2) {
                                                BaseFunction baseFunction = new BaseFunction();
                                                baseFunction.setUpdated_at(updatedAt);
                                                baseFunction.setResume_id(id);
                                                baseFunction.setWid(wid);
                                                //function_id
                                                Integer functionId4 = 0;
                                                try {
                                                    functionId4 = Integer.parseInt(split2[0]);
                                                } catch (NumberFormatException e) {
                                                    LOG.error("id:{} cv_tag.should:{} parse int error", id, should1);
                                                }
                                                baseFunction.setFunction_id(functionId4);
                                                //value
                                                Double values4 = Double.parseDouble(split2[1]);
                                                baseFunction.setValue(values4);
                                                baseFunctions.add(baseFunction);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("id:{} algorithms parse json error:{}", id, e.getMessage());
                        e.printStackTrace();
                    }
                }
                return baseFunctions.iterator();
            }, Encoders.bean(BaseFunction.class))
            .filter((FilterFunction<BaseFunction>) Objects::nonNull);

        baseFunctionDataset.write().mode(SaveMode.Overwrite).saveAsTable("idw_resume.base_function");

    }

}
