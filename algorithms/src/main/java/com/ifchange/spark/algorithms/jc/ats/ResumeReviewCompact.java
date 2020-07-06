package com.ifchange.spark.algorithms.jc.ats;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * resume_review 将历史和增量的每周进行合并(去重）
 */
public class ResumeReviewCompact {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResumeReviewCompact.class);

    public static void main(String[] args) {

        String master = args[0];
        String appName = args[1];
        String incrementPath = args[2];
        String historyPath = args[3];
        int partition = Integer.parseInt(args[4]);
        String savePath = args[5];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> increRdd = jsc.textFile(incrementPath).map(str -> {
            String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(str, "\t");
            if (split.length != 8) {
                LOGGER.info("value :{} length:{} is error", str, split.length);
                throw new Exception("split length is error,value:" + str);
            }
            String uid = split[0];
            String positionId = split[1];
            String resumeId = split[2];
            return uid + "\t" + positionId + "\t" + resumeId;
        });
        List<String> list = increRdd.collect();
        final Broadcast<List<String>> broadcast = jsc.broadcast(list);

        JavaRDD<String> rdd1 = jsc.textFile(historyPath).map(str -> {
            List<String> lists = broadcast.value();
            String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(str, "\t");
            if (split.length != 8) {
                LOGGER.info("value :{} length:{} is error", str, split.length);
                throw new Exception("split length is error,value:" + str);
            }
            String uid = split[0];
            String positionId = split[1];
            String resumeId = split[2];
            String status = split[3];
            String resumeStatus = split[4];
            String hrStatus = split[5];
            String cvJson = split[6];
            String jdJson = split[7];

            String key = uid + "\t" + positionId + "\t" + resumeId;
            String result = "";
            if (!lists.contains(key)) {
                result = uid + "\t" + positionId + "\t" + resumeId + "\t" + status + "\t" + resumeStatus + "\t"
                    + hrStatus + "\t" + cvJson + "\t" + jdJson;
            }
            return result;
        }).filter(StringUtils::isNoneBlank);

        JavaRDD<String> rdd2 = jsc.textFile(incrementPath).map(str -> {
            String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(str, "\t");
            if (split.length != 8) {
                LOGGER.info("value :{} length:{} is error", str, split.length);
                throw new Exception("split length is error,value:" + str);
            }
            String uid = split[0];
            String positionId = split[1];
            String resumeId = split[2];
            String status = split[3];
            String resumeStatus = split[4];
            String hrStatus = split[5];
            String cvJson = split[6];
            String jdJson = split[7];
            return uid + "\t" + positionId + "\t" + resumeId + "\t" + status + "\t" + resumeStatus + "\t"
                + hrStatus + "\t" + cvJson + "\t" + jdJson;
        });

        JavaRDD<String> result = rdd1.union(rdd2).coalesce(partition);
        result.saveAsTextFile(savePath);

    }
}
