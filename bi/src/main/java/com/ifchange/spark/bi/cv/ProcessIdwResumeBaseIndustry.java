package com.ifchange.spark.bi.cv;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.bi.bean.cv.Algorithms;
import com.ifchange.spark.bi.bean.cv.BaseIndustryTemp;
import com.ifchange.spark.bi.bean.cv.IndustryMapping;
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
import java.util.Objects;

/**
 * idw_resume 数据处理
 * base_industry
 */
public class ProcessIdwResumeBaseIndustry {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwResumeBaseIndustry.class);

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String algoPath = args[2];
        String industryPath = args[3];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        //加载industry_mapping /user/hive/warehouse/edw_dim_dimension.db/industry_mapping/part-00000-c0e3973b-671c-4549-a23d-6289f643bc6d-c000.txt
        Dataset<IndustryMapping> industryMappingDs = sparkSession.read()
            .textFile(industryPath)
            .map((MapFunction<String, IndustryMapping>) s -> {
                IndustryMapping industryMapping = new IndustryMapping();
                String[] split = s.split("\t");
                Integer industryId = Integer.parseInt(split[0]);
                Integer pindustryId = Integer.parseInt(split[1]);
                Integer depth = Integer.parseInt(split[2]);
                industryMapping.setId(industryId);
                industryMapping.setPindustry_id(pindustryId);
                industryMapping.setDepth(depth);
                return industryMapping;
            }, Encoders.bean(IndustryMapping.class));

        Dataset<BaseIndustryTemp> baseIndustryTempDataset = sparkSession.read()
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
                        String cvTrade = jsonObject.getString("cv_trade");
                        algorithms.setCvTrade(cvTrade);
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
            .flatMap((FlatMapFunction<Algorithms, BaseIndustryTemp>) algorithms -> {
                List<BaseIndustryTemp> list = new ArrayList<>();
                Long id = algorithms.getId();
                String cvTrade = algorithms.getCvTrade();
                LOG.info("id:{},cv_trade:{}", id, cvTrade);
                String updatedAt = algorithms.getUpdatedAt();
                if (StringUtils.isNoneBlank(cvTrade)) {
                    try {
                        JSONArray array = JSONObject.parseArray(cvTrade);
                        if (null != array && array.size() > 0) {
                            for (int i = 0; i < array.size(); i++) {
                                JSONObject tradeJson = array.getJSONObject(i);
                                if (null != tradeJson && tradeJson.size() > 0) {
                                    String wid = tradeJson.getString("work_id");
                                    List<Integer> tradeList = null != tradeJson.get("second_trade_list") ?
                                        (List<Integer>) tradeJson.get("second_trade_list") :
                                        (List<Integer>) tradeJson.get("first_trade_list");
                                    if (null != tradeList && tradeList.size() > 0) {
                                        for (int industryId : tradeList) {
                                            BaseIndustryTemp baseIndustryTemp = new BaseIndustryTemp();
                                            baseIndustryTemp.setResume_id(id);
                                            baseIndustryTemp.setWid(wid);
                                            baseIndustryTemp.setUpdated_at(updatedAt);
                                            baseIndustryTemp.setIndustry_id(industryId);
                                            LOG.info("id:{}->wid:{}->industry_id:{}", id, wid, industryId);
                                            list.add(baseIndustryTemp);
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
                return list.iterator();
            }, Encoders.bean(BaseIndustryTemp.class))
            .filter((FilterFunction<BaseIndustryTemp>) Objects::nonNull)
            .repartition(200);


        baseIndustryTempDataset.join(industryMappingDs,
            baseIndustryTempDataset.col("industry_id").equalTo(industryMappingDs.col("id")), "left")
            .select(baseIndustryTempDataset.col("resume_id"),
                baseIndustryTempDataset.col("wid"),
                industryMappingDs.col("pindustry_id"),
                industryMappingDs.col("depth"),
                baseIndustryTempDataset.col("updated_at"))
            .write().mode(SaveMode.Overwrite).saveAsTable("idw_resume.base_industry");

    }

}
