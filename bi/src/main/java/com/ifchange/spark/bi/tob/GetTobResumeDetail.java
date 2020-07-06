package com.ifchange.spark.bi.tob;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.util.MyString;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;

/**
 * tob简历情况汇总
 */
public class GetTobResumeDetail {

    private static final Logger LOG = LoggerFactory.getLogger(GetTobResumeDetail.class);

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        int partition = Integer.parseInt(args[2]);
        String tobResumeDetailPath = args[3];
        String savePath = args[4];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        LongAccumulator tobResumeNumberAcc = sparkSession.sparkContext().longAccumulator("tob_resume_number");

        Dataset<String> result = sparkSession.read()
            .textFile(tobResumeDetailPath)
            .repartition(partition)
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
            }).map((MapFunction<String, String>) s -> {
                String results = "";
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                String tobResumeId = split[0];
                String compressStr = split[1];

                if (StringUtils.isNoneBlank(compressStr)) {
                    String compress = "";
                    try {
                        compress = new String(MyString.gzipUncompress(Base64.getDecoder().decode(compressStr)));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (StringUtils.isNoneBlank(compress)) {
                        JSONObject jsonObject = JSONObject.parseObject(compress);
                        if (null != jsonObject && jsonObject.size() > 0) {
                            JSONObject basic = null;
                            JSONObject education = null;
                            JSONObject work = null;
                            JSONObject certificate = null;
                            JSONObject project = null;
                            JSONObject training = null;
                            JSONObject language = null;
                            JSONObject skill = null;
                            JSONObject contact = null;
                            try {
                                basic = jsonObject.getJSONObject("basic");
                                education = jsonObject.getJSONObject("education");
                                work = jsonObject.getJSONObject("work");
                                certificate = jsonObject.getJSONObject("certificate");
                                project = jsonObject.getJSONObject("project");
                                training = jsonObject.getJSONObject("training");
                                language = jsonObject.getJSONObject("language");
                                skill = jsonObject.getJSONObject("skill");
                                contact = jsonObject.getJSONObject("contact");
                            } catch (Exception e) {
                                LOG.error("id:{},msg:{}", tobResumeId, e.getMessage());
                            }
                            if (null != basic && basic.size() > 0 &&
                                null != education && education.size() > 0 &&
                                null != work && work.size() > 0 &&
                                null != certificate && certificate.size() > 0 &&
                                null != project && project.size() > 0 &&
                                null != training && training.size() > 0 &&
                                null != language && language.size() > 0 &&
                                null != skill && skill.size() > 0 &&
                                null != contact && contact.size() > 0) {
                                results = tobResumeId;
                                tobResumeNumberAcc.add(1L);
                                LOG.info("id:{} is detail", tobResumeId);
                            }
                        }
                    }
                }
                return results;
            }, Encoders.STRING()).filter((FilterFunction<String>) StringUtils::isNoneBlank);

        result.repartition(1).write().text(savePath);

    }

}
