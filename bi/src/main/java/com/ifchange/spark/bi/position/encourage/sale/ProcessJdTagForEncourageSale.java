package com.ifchange.spark.bi.position.encourage.sale;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.bi.bean.position.IdwThirdsiteDataPositionsFunctions;
import com.ifchange.spark.bi.http.JdTagHttp;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 处理励销职位- 职能
 */
public class ProcessJdTagForEncourageSale {


    private static final Logger LOG = LoggerFactory.getLogger(ProcessJdTagForEncourageSale.class);

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String positionPath = args[2];
        int partition = Integer.parseInt(args[3]);

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
        LongAccumulator totalAcc = sparkSession.sparkContext().longAccumulator("position_map_total_acc");
        LongAccumulator errorAcc = sparkSession.sparkContext().longAccumulator("position_map_error_acc");
        LongAccumulator rightAcc = sparkSession.sparkContext().longAccumulator("position_map_right_acc");

        Dataset<IdwThirdsiteDataPositionsFunctions> dataset = sparkSession.read()
            .textFile(positionPath)
            .repartition(partition)
            .filter((FilterFunction<String>) s -> {
                totalAcc.add(1L);
                boolean flag = false;
                if (StringUtils.isNoneBlank(s)) {
                    String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                    if (split.length == 29) {
                        String isDeleted = split[17];
                        if (StringUtils.equals("0", isDeleted)) {
                            flag = true;
                            rightAcc.add(1L);
                        } else {
                            errorAcc.add(1L);
                        }
                    } else {
                        errorAcc.add(1L);
                        LOG.error("position_sources size is wrong,{}", split.length);
                    }
                } else {
                    errorAcc.add(1L);
                    LOG.error("position_sources data is empty,{}", s);
                }
                return flag;
            }).mapPartitions((MapPartitionsFunction<String, IdwThirdsiteDataPositionsFunctions>) iterator -> {
                JdTagHttp.init(2);

                List<IdwThirdsiteDataPositionsFunctions> list = new ArrayList<>();
                while (iterator.hasNext()) {

                    String line = iterator.next();
                    String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, "\t");
                    String description = split[24];
                    String name = split[4];
                    String id = split[0];
                    String positionId = split[14];
                    if(StringUtils.isNoneBlank(description)){
                        JdTagHttp jdTagHttp = new JdTagHttp(id, "", name, description);
                        String result = jdTagHttp.start(jdTagHttp);
                        LOG.info("{}", result);
                        //解析
                        if (StringUtils.isNoneBlank(result)) {
                            JSONObject jsonObject = JSONObject.parseObject(result);
                            if (null != jsonObject && jsonObject.size() > 0) {
                                //2级职能
                                JSONObject refZhinengMulti = jsonObject.getJSONObject("ref_zhineng_multi");
                                if (null != refZhinengMulti && refZhinengMulti.size() > 0) {
                                    String category = refZhinengMulti.getString("category");
                                    if (StringUtils.isNoneBlank(category)) {
                                        IdwThirdsiteDataPositionsFunctions positionsFunctions = new IdwThirdsiteDataPositionsFunctions();
                                        String[] split1 = StringUtils.splitByWholeSeparator(category, ":");
                                        String functionIdStr = split1[0];
                                        String rankStr = split1[1];
                                        long functionId = Long.parseLong(functionIdStr);
                                        double rank = Double.parseDouble(rankStr);
                                        positionsFunctions.setId(Long.parseLong(id));
                                        positionsFunctions.setPosition_id(Long.parseLong(positionId));
                                        positionsFunctions.setDepth(2);
                                        positionsFunctions.setFunction_id(functionId);
                                        positionsFunctions.setRank(rank);
                                        list.add(positionsFunctions);
                                    }
                                }
                                //3级职能
                                JSONArray mustArray = refZhinengMulti.getJSONArray("must");
                                if (null != mustArray && mustArray.size() > 0) {
                                    for (int i = 0; i < mustArray.size(); i++) {
                                        JSONObject mustJson = mustArray.getJSONObject(i);
                                        if(null !=mustJson && mustJson.size()>0){
                                            IdwThirdsiteDataPositionsFunctions positionsFunctions = new IdwThirdsiteDataPositionsFunctions();
                                            long functionId = mustJson.getLong("function_id");
                                            String rankStr = mustJson.getString("rank");
                                            double rank = Double.parseDouble(rankStr);
                                            positionsFunctions.setId(Long.parseLong(id));
                                            positionsFunctions.setPosition_id(Long.parseLong(positionId));
                                            positionsFunctions.setDepth(3);
                                            positionsFunctions.setFunction_id(functionId);
                                            positionsFunctions.setRank(rank);
                                            list.add(positionsFunctions);
                                        }
                                    }
                                }

                                //4级职能
                                JSONArray shouldArray = refZhinengMulti.getJSONArray("should");
                                if (null != shouldArray && shouldArray.size() > 0) {
                                    for (int i = 0; i < shouldArray.size(); i++) {
                                        JSONObject shouldJson = shouldArray.getJSONObject(i);
                                        if(null !=shouldJson && shouldJson.size()>0){
                                            IdwThirdsiteDataPositionsFunctions positionsFunctions = new IdwThirdsiteDataPositionsFunctions();
                                            long functionId = shouldJson.getLong("function_id");
                                            String rankStr = shouldJson.getString("rank");
                                            double rank = Double.parseDouble(rankStr);
                                            positionsFunctions.setId(Long.parseLong(id));
                                            positionsFunctions.setPosition_id(Long.parseLong(positionId));
                                            positionsFunctions.setDepth(4);
                                            positionsFunctions.setFunction_id(functionId);
                                            positionsFunctions.setRank(rank);
                                            list.add(positionsFunctions);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                return list.iterator();
            }, Encoders.bean(IdwThirdsiteDataPositionsFunctions.class));

        //save data into hive
        dataset.write().mode(SaveMode.Overwrite).saveAsTable("idw_positions_thirdsite_data.positions_function");


    }

}
