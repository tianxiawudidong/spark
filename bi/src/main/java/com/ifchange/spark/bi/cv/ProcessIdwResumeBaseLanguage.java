package com.ifchange.spark.bi.cv;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.bi.bean.cv.*;
import com.ifchange.spark.util.MyString;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * idw_resume 数据处理
 * base_language
 */
public class ProcessIdwResumeBaseLanguage {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwResumeBaseLanguage.class);

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String extraPath = args[2];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        Dataset<ResumeExtra> resumeExtraDataset = sparkSession.read()
            .textFile(extraPath)
            .filter((FilterFunction<String>) s -> {
                boolean flag = false;
                if (StringUtils.isNoneBlank(s)) {
                    String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                    /**
                     * resume_extra \t 10
                     */
                    if (split.length == 10) {
                        String id = split[0];
                        if (StringUtils.isNoneBlank(id) && StringUtils.isNumeric(id)) {
                            flag = true;
                        }
                    }
                }
                return flag;
            }).map((MapFunction<String, ResumeExtra>) s -> {
                ResumeExtra resumeExtra = new ResumeExtra();
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                try {
                    Long id = Long.parseLong(split[0]);
                    resumeExtra.setId(id);
                } catch (Exception e) {
                    LOG.error("parse extra data error:{}", e.getMessage());
                    e.printStackTrace();
                }
                String compress = split[1];
                String updatedAt = split[8];
                String createdAt = split[9];
                resumeExtra.setCompress(compress);
                resumeExtra.setCreatedAt(createdAt);
                resumeExtra.setUpdatedAt(updatedAt);
                return resumeExtra;
            }, Encoders.bean(ResumeExtra.class));


        Dataset<BaseLanguage> baseLanguageDataset = resumeExtraDataset
            .flatMap((FlatMapFunction<ResumeExtra, BaseLanguage>) resumeExtra -> {
                List<BaseLanguage> baseLanguages = new ArrayList<>();
                Long resumeId = resumeExtra.getId();
                String compressStr = resumeExtra.getCompress();
                if (StringUtils.isNoneBlank(compressStr)) {
                    String compress = "";
                    try {
                        compress = MyString.unzipString(MyString.hexStringToBytes(compressStr));
                    } catch (Exception e) {
                        LOG.info("id:{} unzip compress error:{}", resumeId, e.getMessage());
                        e.printStackTrace();
                    }
                    JSONObject jsonObject = null;
                    if (StringUtils.isNoneBlank(compress)) {
                        try {
                            jsonObject = JSONObject.parseObject(compress);
                        } catch (Exception e) {
                            LOG.info("id:{} compress cannot parse to json,msg:{}", resumeId, e.getMessage());
                            e.printStackTrace();
                        }
                    }
                    if (null != jsonObject) {
                        Map<String, Object> languageJson = null;
                        Object languageObj = jsonObject.get("language");
                        if (languageObj instanceof Map) {
                            languageJson = (Map) languageObj;
                        }
                        if (null != languageJson && languageJson.size() > 0) {
                            //    private int ;
                            for (Map.Entry<String, Object> entry : languageJson.entrySet()) {
                                BaseLanguage baseLanguage = new BaseLanguage();
                                baseLanguage.setResume_id(resumeId);
                                String lid = entry.getKey();
                                baseLanguage.setLid(lid);
                                try {
                                    Map<String, Object> language = (Map<String, Object>) entry.getValue();
                                    if (null != language && language.size() > 0) {
                                        String certificate = null != language.get("certificate") ?
                                            String.valueOf(language.get("certificate")) : "";
                                        baseLanguage.setCertificate(certificate);
                                        String name = null != language.get("name") ?
                                            String.valueOf(language.get("name")) : "";
                                        baseLanguage.setName(name);
                                        String level = null != language.get("level") ?
                                            String.valueOf(language.get("level")) : "";
                                        baseLanguage.setLevel(level);
                                        String createdAt = null != language.get("created_at") ?
                                            String.valueOf(language.get("created_at")) : "";
                                        baseLanguage.setCreated_at(createdAt);
                                        String updatedAt = null != language.get("updated_at") ?
                                            String.valueOf(language.get("updated_at")) : "";
                                        baseLanguage.setUpdated_at(updatedAt);
                                        int sortId = 0;
                                        try {
                                            sortId = null != language.get("sort_id") ?
                                                Integer.parseInt(String.valueOf(language.get("sort_id"))) : 0;
                                        } catch (NumberFormatException e) {
                                            LOG.info("sort_id:{} cannot parse");
                                        }
                                        baseLanguage.setSort_id(sortId);
                                        baseLanguages.add(baseLanguage);
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }
                return baseLanguages.iterator();
            }, Encoders.bean(BaseLanguage.class))
            .filter((FilterFunction<BaseLanguage>) Objects::nonNull);

        //save data to hive
        baseLanguageDataset.write().mode(SaveMode.Overwrite).saveAsTable("idw_resume.base_language");
    }

}
