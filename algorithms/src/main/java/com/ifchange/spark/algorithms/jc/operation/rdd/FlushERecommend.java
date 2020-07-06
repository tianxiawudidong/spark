package com.ifchange.spark.algorithms.jc.operation.rdd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.algorithms.CvAlgorithmsTagUtil;
import com.ifchange.spark.algorithms.JdAlgorithmsTagUtil;
import com.ifchange.spark.algorithms.http.*;
import com.ifchange.spark.mysql.Mysql;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * e_recommend 刷全库（非第一次）
 * 输入：     hdfs e_recommend_all_$date
 * 处理逻辑：读取出每一行的cvJson、jdJson，
 * 解析cvJson，根据cvJson中的compress，调相应的算法信息，将结果覆盖掉cvJson的原来的算法结果
 * jdJSon同理
 * 输出:     hdfs e_recommend_all_$date
 */
public class FlushERecommend {

    private static final Logger logger = LoggerFactory.getLogger(FlushERecommend.class);

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    private static final String callCvAlgorithmsStr = "cv_trade,cv_title,cv_tag,cv_entity,cv_education,cv_feature,cv_quality,cv_language,cv_degree,cv_resign,cv_workyear,cv_current_status,cv_certificate";

    private static final String USER_NAME = "biuser";

    private static final String PASS_WORD = "30iH541pSBCU";

    private static final String HOST = "192.168.8.46";

    private static final int PORT = 3307;

    private static final String DB_NAME = "bi_data";

    public static void main(String[] args) {

        if (args.length < 4) {
            logger.info("args length is not correct");
            System.exit(-1);
        }

        String appName = args[0];
        String master = args[1];
        int partition = Integer.parseInt(args[2]);
        String path = args[3];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> resultRdd = jsc.textFile(path, partition)
            .filter(StringUtils::isNoneBlank)
            .mapPartitions((FlatMapFunction<Iterator<String>, String>) stringIterator -> {
                List<String> list = new ArrayList<>();
                try {
                    CropTagHttp.init();
                    CvTagHttp.init();
                    CvEntityHttp.init();
                    CvFeatureHttp.init();
                    CvDegreeHttp.init();
                    CvWorkYearHttp.init();
                    CvEducationHttp.init();
                    CvTitleHttp.init();
                    CvCertificateHttp.init();
                    CvLanguageHttp.init();
                    CvResignHttp.init();
                    CvQualityHttp.init();
                    //jd
                    JdTagHttp.init();
                    TobInternalAccountHttp.init();
                    JdTradesHttp.init();
                    JdSchoolHttp.init();
                    JdCorporationsHttp.init();
                    JdEntityHttp.init();
                    JdFeatureHttp.init();
                    JdOriginalCorporationsHttp.init();
                    JdRealCorporationsHttp.init();
                } catch (Exception e) {
                    logger.info("init gearMan pool error:{}", e.getMessage());
                }

                Mysql mysql = new Mysql(USER_NAME, PASS_WORD, DB_NAME, HOST, PORT);

                while (stringIterator.hasNext()) {
                    StringBuilder sb = new StringBuilder();
                    String str = stringIterator.next();
                    String[] split = str.split("\t");
                    int length = split.length;
                    for (int i = 0; i < length - 2; i++) {
                        sb.append(split[i]);
                        sb.append("\t");
                    }

                    String cid = split[6];
                    String jid = split[5];
                    String cvJsonStr = split[length - 2];
                    String jdJsonStr = split[length - 1];
                    //解析cvJSON
                    if (StringUtils.isNoneBlank(cvJsonStr)) {
                        JSONObject cvJson = JSONObject.parseObject(cvJsonStr);
                        JSONObject jsonObject = cvJson.getJSONObject(cid);
                        JSONObject algorithm = jsonObject.getJSONObject("algorithm");
                        //获取cv_resign 需要的history
                        String history = "";
                        String cvResign = algorithm.getString("cv_resign");
                        if (StringUtils.isNotBlank(cvResign) && cvResign.trim().length() > 0) {
                            try {
                                JSONObject resignJson = JSONObject.parseObject(cvResign);
                                if (null != resignJson) {
                                    history = resignJson.getString("history");
                                }
                            } catch (Exception e) {
                                logger.info("{} cannot parse to json", cvResign);
                            }
                        }
                        //删掉cvJSon原来的算法信息
                        if (algorithm.size() > 0) {
                            for (Map.Entry<String, Object> entry : algorithm.entrySet()) {
                                String key = entry.getKey();
                                if (key.startsWith("cv_") && !key.equals("cv_source")) {
                                    algorithm.remove(key);
                                }
                            }
                        }
                        jsonObject.put("algorithm", algorithm);
                        Map<String, Object> behavior = new HashMap<>();
                        int timesDeliver = 0;
                        int timesUpdate = 0;
                        String sql = "  select * from `bi_data`.bi_update_deliver_num where resume_id = " + cid;
                        logger.info(sql);
                        List<Map<String, Object>> biDatas = mysql.executeQuery(sql);
                        if (null != biDatas && biDatas.size() > 0) {
                            Map<String, Object> map = biDatas.get(0);
                            timesDeliver = null != map.get("days7_deliver_num") ? (int) map.get("days7_deliver_num") : 0;
                            timesUpdate = null != map.get("days7_update_num") ? (int) map.get("days7_update_num") : 0;
                        }
                        behavior.put("times_deliver", timesDeliver);
                        behavior.put("times_update", timesUpdate);
                        Map<String, String> result = CvAlgorithmsTagUtil.algorithmsTagForCv(cid, jsonObject);
                        algorithm.putAll(result);
                        jsonObject.put("algorithm", algorithm);
                        cvJson.put(cid, jsonObject);
                        sb.append(cvJson);
                    }
                    sb.append("\t");

                    if (StringUtils.isNoneBlank(jdJsonStr)) {
                        JSONObject jdJson = JSONObject.parseObject(jdJsonStr);
                        JSONObject jsonObject = jdJson.getJSONObject(jid);
                        try {
                            JSONObject json = JdAlgorithmsTagUtil.flushAllJdAlgoForJc(jsonObject);
                            jdJson.put(jid, json);
                        } catch (Exception e) {
                            logger.error(e.getMessage());
                        }
                        sb.append(jdJson);
                    }
                    list.add(sb.toString());
                }
                return list.iterator();
            });

        resultRdd.saveAsTextFile("/basic_data/tob/operation/e_recommend_all_" + sdf.format(new Date()));

    }
}
