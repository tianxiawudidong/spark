package com.ifchange.spark.bi.cv;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.bi.bean.cv.BaseCertificate;
import com.ifchange.spark.bi.bean.cv.ResumeExtra;
import com.ifchange.spark.util.MyString;
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
 * base_certificate
 */
public class ProcessIdwResumeBaseCertificate {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwResumeBaseCertificate.class);

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


        Dataset<BaseCertificate> baseCertificateDataset = resumeExtraDataset
            .flatMap((FlatMapFunction<ResumeExtra, BaseCertificate>) resumeExtra -> {
                List<BaseCertificate> baseCertificates = new ArrayList<>();
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
                        Map<String, Object> certificateJson = null;
                        Object certificate = jsonObject.get("certificate");
                        if (certificate instanceof Map) {
                            certificateJson = (Map) certificate;
                        }
                        if (null != certificateJson && certificateJson.size() > 0) {
                            for (Map.Entry<String, Object> entry : certificateJson.entrySet()) {
                                BaseCertificate baseCertificate = new BaseCertificate();
                                baseCertificate.setResume_id(resumeId);
                                String id = entry.getKey();
                                baseCertificate.setId(id);
                                try {
                                    Map<String, Object> language = (Map<String, Object>) entry.getValue();
                                    if (null != language && language.size() > 0) {
                                        String name = null != language.get("name") ?
                                            String.valueOf(language.get("name")) : "";
                                        baseCertificate.setName(name);

                                        String startTime = null != language.get("start_time") ?
                                            String.valueOf(language.get("start_time")) : "";
                                        baseCertificate.setStart_time(startTime);

                                        String description = null != language.get("description") ?
                                            String.valueOf(language.get("description")) : "";
                                        baseCertificate.setDescription(description);

                                        String isDeleted = null != language.get("is_deleted") ?
                                            String.valueOf(language.get("is_deleted")) : "";
                                        baseCertificate.setIs_deleted(isDeleted);

                                        String updatedAt = null != language.get("updated_at") ?
                                            String.valueOf(language.get("updated_at")) : "";
                                        baseCertificate.setUpdated_at(updatedAt);

                                        String createdAt = null != language.get("created_at") ?
                                            String.valueOf(language.get("created_at")) : "";
                                        baseCertificate.setCreated_at(createdAt);

                                        int sortId = 0;
                                        try {
                                            sortId = null != language.get("sort_id") ?
                                                Integer.parseInt(String.valueOf(language.get("sort_id"))) : 0;
                                        } catch (NumberFormatException e) {
                                            LOG.info("sort_id:{} cannot parse");
                                        }
                                        baseCertificate.setSort_id(sortId);
                                        baseCertificates.add(baseCertificate);
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }
                return baseCertificates.iterator();
            }, Encoders.bean(BaseCertificate.class))
            .filter((FilterFunction<BaseCertificate>) Objects::nonNull);

        //save data to hive
        baseCertificateDataset.write().mode(SaveMode.Overwrite).saveAsTable("idw_resume.base_certificate");
    }

}
