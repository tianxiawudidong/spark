package com.ifchange.spark.algorithms.jc.ats.recruitstep.compact;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RecruitStepCompactDs {

    private static final Logger LOG = LoggerFactory.getLogger(RecruitStepCompactDs.class);

    public static void main(String[] args) {

        String master = args[0];
        String appName = args[1];
        String incrementPath = args[2];
        String historyPath = args[3];
        int partition = Integer.parseInt(args[4]);
        String savePath = args[5];
        final String type = args[6];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        SparkContext sc = sparkSession.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);
        LongAccumulator accumulator = sc.longAccumulator("duplicate_accumulator");


        Dataset<String> increKey = sparkSession.read()
            .textFile(incrementPath)
            .filter((FilterFunction<String>) s -> StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t").length == 9)
            .map((MapFunction<String, String>) s -> {
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                String uid = split[0];
                String icdcPositionId = StringUtils.equals(type, "tyx") ? split[1] : split[4];
                String tobResumeId = split[3];
                return uid + "-" + icdcPositionId + "-" + tobResumeId;
            }, Encoders.STRING());

        List<String> list = increKey.collectAsList();

        final Broadcast<List<String>> broadcast = jsc.broadcast(list);

        Dataset<String> increDs = sparkSession.read()
            .textFile(incrementPath)
            .filter((FilterFunction<String>) s -> StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t").length == 9);

        Dataset<String> historyDs = sparkSession.read()
            .textFile(historyPath)
            .filter((FilterFunction<String>) s -> StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t").length == 9)
            .map((MapFunction<String, String>) s -> {
                List<String> lists = broadcast.value();
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                //通用线
                //uid、icdc_position_id、resume_id、tob_resume_id、stage_type_id、stage_type_from、updated_at、cv_json（tob_resume_id）、jd_json
                String uid = split[0];
                String icdcPositionId = StringUtils.equals(type, "tyx") ? split[1] : split[4];
                String tobResumeId = split[3];
                String key = uid + "-" + icdcPositionId + "-" + tobResumeId;
                String result = "";
                if (!lists.contains(key)) {
                    result = s;
                } else {
                    LOG.info("key:{} has duplicate", key);
                    accumulator.add(1L);
                }
                return result;
            }, Encoders.STRING())
            .filter((FilterFunction<String>) StringUtils::isNoneBlank);

        Dataset<String> result = increDs.union(historyDs).coalesce(partition);

        result.write().text(savePath);
    }
}
