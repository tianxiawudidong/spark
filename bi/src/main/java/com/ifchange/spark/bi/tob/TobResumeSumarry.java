package com.ifchange.spark.bi.tob;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.bi.bean.tob.ResumeCounter;
import com.ifchange.spark.util.MyString;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * tob简历情况汇总
 * 1、简历总数
 * 2、简历格式非json的
 * 3、重复简历分布情况
 */
public class TobResumeSumarry {

    private static final Logger LOG = LoggerFactory.getLogger(TobResumeSumarry.class);

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        int partition = Integer.parseInt(args[2]);
        String path = args[3];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        LongAccumulator tobResumeTotalCountAcc = sparkSession.sparkContext().longAccumulator("tob_resume_total_count");
        LongAccumulator tobResumeNotJsonAcc = sparkSession.sparkContext().longAccumulator("tob_resume_not_json");

        Dataset<ResumeCounter> dataset = sparkSession.read().textFile(path)
            .repartition(partition)
            .filter((FilterFunction<String>) s -> StringUtils.isNoneBlank(s)
                && StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t").length == 4)
            .map((MapFunction<String, ResumeCounter>) s -> {
                ResumeCounter resumeCounter = new ResumeCounter();
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                tobResumeTotalCountAcc.add(1L);
                String tobResumeId = split[0];
                String content = split[1];
                String compress = new String(MyString.gzipUncompress(Base64.getDecoder().decode(content)));
                if (StringUtils.isNoneBlank(compress)) {
                    try {
                        JSONObject.parseObject(compress);
                    } catch (Exception e) {
                        tobResumeNotJsonAcc.add(1L);
                        LOG.error("id:{} content is not json", tobResumeId);
                    }
                }
                String md5 = MyString.getMD5(content);
                resumeCounter.setData(md5);
                resumeCounter.setId(tobResumeId);
                return resumeCounter;
            }, Encoders.bean(ResumeCounter.class));

        dataset.write().mode(SaveMode.Overwrite).saveAsTable("idw_tob_resume.tob_resume_sumarry");

    }

}
