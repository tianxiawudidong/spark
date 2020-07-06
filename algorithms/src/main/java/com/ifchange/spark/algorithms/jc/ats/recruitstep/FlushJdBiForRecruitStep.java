package com.ifchange.spark.algorithms.jc.ats.recruitstep;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.algorithms.JdAlgorithmsTagUtil;
import com.ifchange.spark.algorithms.http.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ats 行为数据刷 jd_bi
 * <p>
 * uid、icdc_position_id、resume_id、tob_resume_id、stage_type_id、stage_type_from、updated_at、cv_json（tob_resume_id）、jd_json
 * <p>
 * update at 2019/12/24
 */
public class FlushJdBiForRecruitStep {

    private static final Logger LOG = LoggerFactory.getLogger(FlushJdBiForRecruitStep.class);

    public static void main(String[] args) {

        if (args.length < 5) {
            LOG.info("args length is not correct");
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
        LongAccumulator accumulator = spark.sparkContext().longAccumulator("recruit_step_number");


        Dataset<String> result = spark.read()
            .textFile(path)
            .repartition(partition)
            .mapPartitions((MapPartitionsFunction<String, String>) iterator -> {
                List<String> list = new ArrayList<>();
                JdTradesHttp.init(2);

                while (iterator.hasNext()) {
                    StringBuilder sb = new StringBuilder();
                    String line = iterator.next();
                    if (StringUtils.isNoneBlank(line)) {
                        String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, "\t");
                        //uid、icdc_position_id、resume_id、tob_resume_id、stage_type_id、stage_type_from、updated_at、cv_json（tob_resume_id）、jd_json
                        if (null != split && split.length == 9) {
                            String uid = split[0];
                            String jid = split[1];
                            String resumeId = split[2];
                            String tobResumeId = split[3];
                            String stageTypeId = split[4];
                            String stageTypeFrom = split[5];
                            String updatedAt = split[6];
                            String cvJsonStr = split[7];
                            String jdJsonStr = split[8];
                            String format = String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s", uid, jid, resumeId,
                                tobResumeId, stageTypeId, stageTypeFrom, updatedAt, cvJsonStr);
                            sb.append(format);
                            sb.append("\t");

                            //jd
                            if (StringUtils.isNoneBlank(jdJsonStr)) {
                                JSONObject jdJson = JSONObject.parseObject(jdJsonStr);
                                if (null != jdJson && jdJson.size() > 0) {
                                    JSONObject jsonObject = jdJson.getJSONObject(jid);
                                    if (null != jsonObject && jsonObject.size() > 0) {
                                        String positionId = jsonObject.getString("id");
                                        String description = jsonObject.getString("description");
                                        String name = jsonObject.getString("name");
                                        String corporationName = jsonObject.getString("corporation_name");
                                        String userId = jsonObject.getString("user_id");
                                        String corpId = jdJson.getString("corporation_id");
                                        String jdTag = jdJson.getString("jd_tags");
                                        String tagFlag = jdJson.getString("tag_flag");
                                        Map<String, Object> tagFlagMap = new HashMap<>();
                                        if (StringUtils.isNoneBlank(tagFlag)) {
                                            try {
                                                tagFlagMap = JSONObject.parseObject(tagFlag);
                                            } catch (Exception e) {
                                                LOG.error("id:{} tag_flag:{} parse to {} error:{}", positionId, tagFlag, e.getMessage());
                                                e.printStackTrace();
                                            }
                                        }
                                        String jdCorporations = jdJson.getString("jd_corporations");
                                        String jdSchools = jdJson.getString("jd_schools");
                                        String industries = jdJson.getString("industries");
                                        String requirement = jdJson.getString("requirement");
                                        String address = jdJson.getString("address");

                                        //jd_tags
                                        String jdTrades = JdAlgorithmsTagUtil.callJdTrades(positionId, userId, corpId,
                                            jdTag, tagFlagMap, jdCorporations,
                                            jdSchools, industries,
                                            description, name, requirement,
                                            corporationName, address);
                                        LOG.info("id:{},jd_trades:{}", positionId, jdTrades);
                                        jsonObject.put("jd_trades", jdTrades);

                                        jdJson.put(jid, jsonObject);
                                        sb.append(jdJson);
                                    } else {
                                        sb.append(jdJsonStr);
                                    }
                                } else {
                                    sb.append(jdJsonStr);
                                }
                            } else {
                                sb.append(jdJsonStr);
                            }
                            accumulator.add(1L);
                        }
                    }
                    String res = sb.toString();
                    list.add(res);
                }
                return list.iterator();
            }, Encoders.STRING());


        Long value = accumulator.value();
        LOG.info("process number:{}", value);
        result.write().text(savePath);

    }
}
