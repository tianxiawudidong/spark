package com.ifchange.spark.algorithms.jc.ats.recruitstep;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.algorithms.CvAlgorithmsTagUtil;
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

import java.util.*;

/**
 * ats new 非第一次刷全量
 * 刷 技能、公司、职能
 * 输入：     /basic_data/tob/tob_ats/recruit_step_v2_2019-09-08
 * 处理逻辑： 解析每一行中的cvJSON、jdJSON
 * cvJSON 由原始的compress重新调算法服务 覆盖掉cvJSON
 * jdJSON 根据json中的相关参数重新调算法服务 覆盖掉jdJSON
 * 输出：   /basic_data/tob/tob_ats/recruit_step_v2_2019-09-08_new
 * <p>
 * <p>
 * a.uid, b.icdc_position_id, a.resume_id, a.stage_type_id, a.stage_type_from, a.updated_at
 */
public class FlushAtsRecruitStepV2Algo {

    private static final Logger LOG = LoggerFactory.getLogger(FlushAtsRecruitStepV2Algo.class);

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

                CropTagHttp.init(2);
                CvTagHttp.init(2);
                CvSkillHttp.init(2);

                CvTitleHttp.init(2);
                JdTagHttp.init(2);
                JdSkillHttp.init(2);
                JdOriginalCorporationsHttp.init(2);

                while (iterator.hasNext()) {
                    StringBuilder sb = new StringBuilder();
                    String line = iterator.next();
                    if (StringUtils.isNoneBlank(line)) {
                        String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, "\t");
                        if (null != split && split.length == 8) {
                            String uid = split[0];
                            String jid = split[1];
                            String resumeId = split[2];
                            String stageTypeId = split[3];
                            String stageTypeFrom = split[4];
                            String updatedAt = split[5];
                            String cvJsonStr = split[6];
                            String jdJsonStr = split[7];
                            String format = String.format("%s\t%s\t%s\t%s\t%s\t%s", uid, jid, resumeId,
                                stageTypeId, stageTypeFrom, updatedAt);
                            sb.append(format);
                            sb.append("\t");

                            //cv
                            if (StringUtils.isNoneBlank(cvJsonStr)) {
                                try {
                                    JSONObject cvJson = JSONObject.parseObject(cvJsonStr);
                                    if (null != cvJson && cvJson.size() > 0) {
                                        JSONObject jsonObject = cvJson.getJSONObject(resumeId);
                                        if (null != jsonObject && jsonObject.size() > 0) {
                                            JSONObject algorithm = jsonObject.getJSONObject("algorithm");
                                            if (null != algorithm && algorithm.size() > 0) {
                                                Map<String, String> map = new HashMap<>();
                                                try {
                                                    map = CvAlgorithmsTagUtil.callCvTradeTagAndSkillTag(resumeId,
                                                        jsonObject);
                                                } catch (Exception e) {
                                                    LOG.info("id:{} call cv_trade,tag,skill error:{}", resumeId, e.getMessage());
                                                }
                                                if (null != map && map.size() > 0) {
                                                    for (Map.Entry<String, String> entry : map.entrySet()) {
                                                        algorithm.put(entry.getKey(), entry.getValue());
                                                    }
                                                }
                                                jsonObject.put("algorithm", algorithm);
                                                cvJson.put(resumeId, jsonObject);
                                                sb.append(cvJson);
                                            } else {
                                                sb.append(cvJsonStr);
                                            }
                                        } else {
                                            sb.append(cvJsonStr);
                                        }
                                    } else {
                                        sb.append(cvJsonStr);
                                    }
                                } catch (Exception e) {
                                    LOG.info("id:{} process cv_json:{} error:{}", resumeId, cvJsonStr, e.getMessage());
                                    sb.append(cvJsonStr);
                                }
                            } else {
                                sb.append(cvJsonStr);
                            }
                            sb.append("\t");

                            //jd
                            if (StringUtils.isNoneBlank(jdJsonStr)) {
                                JSONObject jdJson = JSONObject.parseObject(jdJsonStr);
                                if (null != jdJson && jdJson.size() > 0) {
                                    JSONObject jsonObject = jdJson.getJSONObject(jid);
                                    if (null != jsonObject && jsonObject.size() > 0) {
                                        String positionId = jsonObject.getString("id");

                                        String requirement = jsonObject.getString("requirement");
                                        String description = jsonObject.getString("description");
                                        String name = jsonObject.getString("name");
                                        String corporationName = jsonObject.getString("corporation_name");
                                        //jd_tags
                                        String jdTag = JdAlgorithmsTagUtil.callJdTag(positionId, requirement, name, description);
                                        jsonObject.put("jd_tags", jdTag);

                                        //jd_ner_skill
                                        String jdEntity = JdAlgorithmsTagUtil.callJdSkill(positionId, requirement, name, description);
                                        jsonObject.put("jd_ner_skill", jdEntity);

                                        //jd_original_corporations
                                        String jdOriginalCorporation = JdAlgorithmsTagUtil.callJdOriginalCorporation(positionId, corporationName);
                                        jsonObject.put("jd_original_corporations", jdOriginalCorporation);

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
