package com.ifchange.spark.bi.cv;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.bi.bean.cv.Algorithms;
import com.ifchange.spark.bi.bean.cv.BaseCompany;
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
 * base_company
 */
public class ProcessIdwResumeBaseCompany {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwResumeBaseCompany.class);

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String algoPath = args[2];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();


        Dataset<BaseCompany> baseCompanyDataset = sparkSession.read()
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
            .flatMap((FlatMapFunction<Algorithms, BaseCompany>) algorithms -> {
                List<BaseCompany> baseCompanys = new ArrayList<>();
                Long id = algorithms.getId();
                String cvTrade = algorithms.getCvTrade();
                String updatedAt = algorithms.getUpdatedAt();
                if (StringUtils.isNoneBlank(cvTrade)) {
                    try {
                        JSONArray array = JSONObject.parseArray(cvTrade);
                        if (null != array && array.size() > 0) {
                            for (int i = 0; i < array.size(); i++) {
                                BaseCompany baseCompany = new BaseCompany();
                                baseCompany.setResume_id(id);
                                baseCompany.setUpdated_at(updatedAt);
                                JSONObject jsons = array.getJSONObject(i);
                                if (null != jsons && jsons.size() > 0) {
                                    String wid = jsons.getString("work_id");
                                    Integer companyId = null != jsons.get("company_id") ? jsons.getInteger("company_id") : 0;
                                    baseCompany.setWid(wid);
                                    baseCompany.setCompany_id(companyId);
                                    baseCompanys.add(baseCompany);
                                }
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("id:{} algorithms parse json error:{}", id, e.getMessage());
                        e.printStackTrace();
                    }
                }
                return baseCompanys.iterator();
            }, Encoders.bean(BaseCompany.class))
            .filter((FilterFunction<BaseCompany>) Objects::nonNull);


        //save data to hive
        baseCompanyDataset.write().mode(SaveMode.Overwrite).saveAsTable("idw_resume.base_company");


    }

}
