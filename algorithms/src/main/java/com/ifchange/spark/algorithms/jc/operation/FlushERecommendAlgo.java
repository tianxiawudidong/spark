package com.ifchange.spark.algorithms.jc.operation;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.algorithms.CvAlgorithmsTagUtil;
import com.ifchange.spark.algorithms.JdAlgorithmsTagUtil;
import com.ifchange.spark.algorithms.http.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * updated_at 19/9/12
 * /basic_data/tob/operation/e_recommend_history/init/
 * cvJson jdJson 重刷
 */
public class FlushERecommendAlgo {

    private static final Logger LOG = LoggerFactory.getLogger(FlushERecommendAlgo.class);

    public static void main(String[] args) {

        if (args.length < 5) {
            LOG.error("args length is not correct");
            System.exit(-1);
        }

        String appName = args[0];
        String master = args[1];
        int partition = Integer.parseInt(args[2]);
        String path = args[3];
        String savePath = args[4];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        LongAccumulator accumulator = spark.sparkContext().longAccumulator("e_recommend_acc");

        Dataset<String> result = spark.read()
            .textFile(path)
            .repartition(partition)
            .filter((FilterFunction<String>) StringUtils::isNoneBlank)
            .mapPartitions((MapPartitionsFunction<String, String>) stringIterator -> {
                List<String> list = new ArrayList<>();

                CropTagHttp.init();
                CvTagHttp.init();
//                CvEntityHttp.init();
                CvFeatureHttp.init();
                CvDegreeHttp.init();
                CvWorkYearHttp.init();
                CvEducationHttp.init();
                CvTitleHttp.init();
                CvCertificateHttp.init();
                CvLanguageHttp.init();
//                CvQualityHttp.init();
                CvCurrentStatus.init();
                CvSkillHttp.init();
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
                JdSkillHttp.init();

                while (stringIterator.hasNext()) {
                    StringBuilder sb = new StringBuilder();
                    String str = stringIterator.next();
                    if (StringUtils.isNoneBlank(str)) {
                        String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(str, "\t");
                        int length = split.length;
                        if (length == 19) {
                            accumulator.add(1L);
                            String jid = split[5];
                            String cid = split[6];
                            LOG.info("cid:{},jid:{}", cid, jid);
                            String format = String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
                                split[0], split[1], split[2], split[3], split[4], split[5], split[6], split[7], split[8],
                                split[9], split[10], split[11], split[12], split[13], split[14], split[15], split[16]
                            );
                            sb.append(format);
                            sb.append("\t");
                            String cvJsonStr = split[17];
                            if (StringUtils.isNoneBlank(cvJsonStr)) {
                                JSONObject jsonObject = JSONObject.parseObject(cvJsonStr);
                                if (null != jsonObject && jsonObject.size() > 0) {
                                    try {
                                        JSONObject cvJson = jsonObject.getJSONObject(cid);
                                        if (null != cvJson && cvJson.size() > 0) {
                                            JSONObject algorithm = cvJson.getJSONObject("algorithm");
                                            Map<String, String> map = CvAlgorithmsTagUtil.algorithmsTagForCvOnYueTa(cid, cvJson);
                                            if (null != map && map.size() > 0) {
                                                for (Map.Entry<String, String> entry : map.entrySet()) {
                                                    algorithm.put(entry.getKey(), entry.getValue());
                                                }
                                            }
                                            cvJson.put("algorithm", algorithm);
                                            jsonObject.put(cid, cvJson);
                                            sb.append(jsonObject);
                                        }
                                    } catch (Exception e) {
                                        LOG.info("{} parse json error",cid);
                                        sb.append(cvJsonStr);
                                    }
                                }
                            } else {
                                sb.append(cvJsonStr);
                            }
                            sb.append("\t");


                            String jdJsonStr = split[18];
                            if (!"0".equals(jid)) {
                                if (StringUtils.isNoneBlank(jdJsonStr)) {
                                    JSONObject jsonObject = JSONObject.parseObject(jdJsonStr);
                                    if (null != jsonObject && jsonObject.size() > 0) {
                                        JSONObject jdJson = jsonObject.getJSONObject(jid);
                                        try {
                                            JSONObject json = JdAlgorithmsTagUtil.flushAllJdAlgoForJc(jdJson);
                                            jsonObject.put(jid, json);
                                        } catch (Exception e) {
                                            LOG.error(e.getMessage());
                                        }
                                        sb.append(jsonObject);
                                    } else {
                                        sb.append(jdJsonStr);
                                    }
                                } else {
                                    sb.append(jdJsonStr);
                                }
                            } else {
                                sb.append(jdJsonStr);
                            }
                        } else {
                            LOG.error("data size:{} is not correct", length);
                            sb.append(str);
                        }
                    }
                    list.add(sb.toString());
                }
                return list.iterator();
            }, Encoders.STRING());

        Long value = accumulator.value();
        LOG.info("process e_recommend number:{}", value);
        result.repartition(partition)
            .write()
            .text(savePath);
    }

}
