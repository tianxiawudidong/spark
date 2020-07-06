package com.ifchange.spark.bi.tob;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.bi.bean.cv.ResumeExtra;
import com.ifchange.spark.bi.bean.tob.*;
import com.ifchange.spark.util.MyString;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;

/**
 * idw_tob_icdc_maps
 */
public class ProcessIdwTobIcdcMaps {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwTobIcdcMaps.class);

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String tobResumeDetailPath = args[2];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        Dataset<ResumeExtra> resumeExtraDs = sparkSession.read()
            .textFile(tobResumeDetailPath)
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
                String updatedAt = split[2];
                String createdAt = split[3];
                resumeExtra.setCompress(compress);
                resumeExtra.setCreatedAt(createdAt);
                resumeExtra.setUpdatedAt(updatedAt);
                return resumeExtra;
            }, Encoders.bean(ResumeExtra.class));


        Dataset<IdwTobIcdcMaps> tobIcdcMapsDs = resumeExtraDs
            .map((MapFunction<ResumeExtra, IdwTobIcdcMaps>) resumeExtra -> {
                IdwTobIcdcMaps idwTobIcdcMaps = new IdwTobIcdcMaps();
                long tobResumeId = resumeExtra.getId();
                idwTobIcdcMaps.setTob_resume_id(tobResumeId);
                String compressStr = resumeExtra.getCompress();

                if (StringUtils.isNoneBlank(compressStr)) {
                    String compress = new String(MyString.gzipUncompress(Base64.getDecoder().decode(compressStr)));
                    JSONObject jsonObject = null;
                    if (StringUtils.isNoneBlank(compress)) {
                        try {
                            jsonObject = JSONObject.parseObject(compress);
                        } catch (Exception e) {
                            LOG.info("id:{} compress cannot parse to json,msg:{}", tobResumeId, e.getMessage());
                        }
                    }
                    if (null != jsonObject) {
                        try {
                            JSONObject basic = jsonObject.getJSONObject("basic");
                            if (null != basic) {
                                String id = null != basic.get("id") ? basic.getString("id") : "0";
                                long icdcResumeId = Long.parseLong(id);
                                idwTobIcdcMaps.setIcdc_resume_id(icdcResumeId);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
                return idwTobIcdcMaps;
            }, Encoders.bean(IdwTobIcdcMaps.class));


        //save data to hive
        tobIcdcMapsDs.write().mode(SaveMode.Overwrite).saveAsTable("idw_tob_resume.tob_icdc_maps");


    }

}
