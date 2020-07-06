package com.ifchange.spark.algorithms.jc.ats.recruitstep.general;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ats recruit_step 通用数据（行为数据）tob_ats_1--tob_ats_10 非第一次刷全量
 * 刷 cv jd所有算法
 * 输入：
 * 处理逻辑： 解析每一行中的cvJSON、jdJSON
 * cvJSON 由原始的compress重新调算法服务 覆盖掉cvJSON
 * jdJSON 根据json中的相关参数重新调算法服务 覆盖掉jdJSON
 * 输出：
 * <p>
 * <p>
 * uid, icdc_position_id, resume_id, stage_type_id, stage_type_from, updated_at,cvJson,jdJson
 */
public class FlushAtsRecruitStepAlgoAll {

    private static final Logger LOG = LoggerFactory.getLogger(FlushAtsRecruitStepAlgoAll.class);

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
        LongAccumulator accumulator = spark.sparkContext().longAccumulator("recruit_step_number");
        LongAccumulator errAcc = spark.sparkContext().longAccumulator("recruit_step_err");

        Dataset<String> result = spark.read()
            .textFile(path)
            .repartition(partition)
            .mapPartitions((MapPartitionsFunction<String, String>) iterator -> {
                List<String> list = new ArrayList<>();

                CropTagHttp.init(2);
                CvCertificateHttp.init(2);
                CvCurrentStatus.init(2);
                CvDegreeHttp.init(2);
                CvEducationHttp.init(2);
                CvEntityHttp.init(2);
                CvFeatureHttp.init(2);
                CvLanguageHttp.init(2);
                CvQualityHttp.init(2);
//                CvResignHttp.init(2);
                CvTagHttp.init(2);
                CvWorkYearHttp.init();
                CvSkillHttp.init(2);
                CvTitleHttp.init(2);

                JdCorporationsHttp.init(2);
                JdEntityHttp.init(2);
                JdFeatureHttp.init(2);
                JdOriginalCorporationsHttp.init(2);
                JdRealCorporationsHttp.init(2);
                JdSchoolHttp.init(2);
                JdSkillHttp.init(2);
                JdTagHttp.init(2);
                JdTradesHttp.init(2);
                TobInternalAccountHttp.init(2);

                while (iterator.hasNext()) {
                    StringBuilder sb = new StringBuilder();
                    String line = iterator.next();
                    if (StringUtils.isNoneBlank(line)) {
                        String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, "\t");
                        if (null != split && split.length == 8) {
                            //uid, icdc_position_id, resume_id, stage_type_id, stage_type_from, updated_at,cvJson,jdJson
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
                                                    map = CvAlgorithmsTagUtil.algorithmsTagForCv(resumeId,
                                                        jsonObject);
                                                } catch (Exception e) {
                                                    LOG.error("id:{} call algorithms error:{}", resumeId, e.getMessage());
                                                    e.printStackTrace();
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
                                JSONObject json = JSONObject.parseObject(jdJsonStr);
                                if (null != json && json.size() > 0) {
                                    try {
                                        JSONObject jsonObject = new JSONObject();
                                        JSONObject jdJson = json.getJSONObject(jid);
                                        JSONObject jdNewJson = JdAlgorithmsTagUtil.flushAllJdAlgoForJc(jdJson);
                                        jsonObject.put(jid, jdNewJson);
                                        sb.append(jsonObject);
                                    } catch (Exception e) {
                                        LOG.error(e.getMessage());
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
                    } else {
                        errAcc.add(1L);
                    }
                    String res = sb.toString();
                    list.add(res);
                }
                return list.iterator();
            }, Encoders.STRING());

        LOG.info("-------------------------------------------------------------------------------");
        LOG.info("process recruit_step_number:{}", accumulator.value());
        LOG.info("process recruit_step_size_error number:{}", errAcc.value());
        LOG.info("-------------------------------------------------------------------------------");
        result.write().text(savePath);

    }
}
