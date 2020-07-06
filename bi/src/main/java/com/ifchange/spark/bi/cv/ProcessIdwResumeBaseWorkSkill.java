package com.ifchange.spark.bi.cv;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.bi.bean.cv.Algorithms;
import com.ifchange.spark.bi.bean.cv.BaseWorkSkill;
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
 * base_work_skill
 */
public class ProcessIdwResumeBaseWorkSkill {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwResumeBaseWorkSkill.class);

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String algoPath = args[2];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        Dataset<BaseWorkSkill> baseWorkSkillDs = sparkSession.read()
            .textFile(algoPath)
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
            }).map((MapFunction<String, Algorithms>) s -> {
                Algorithms algorithms = new Algorithms();
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                try {
                    Long id = Long.parseLong(split[0]);
                    algorithms.setId(id);
                    String json = split[1];
                    if (StringUtils.isNoneBlank(json)) {
                        JSONObject jsonObject = JSONObject.parseObject(json);
                        String skillTag = jsonObject.getString("skill_tag");
                        algorithms.setSkillTag(skillTag);
                    }
                } catch (Exception e) {
                    LOG.error("parse algorithms data error:{}", e.getMessage());
                    e.printStackTrace();
                }
                String updatedAt = split[2];
                String createdAt = split[3];
                algorithms.setCreatedAt(createdAt);
                algorithms.setUpdatedAt(updatedAt);
                return algorithms;
            }, Encoders.bean(Algorithms.class))
            .flatMap((FlatMapFunction<Algorithms, BaseWorkSkill>) algorithms -> {
                List<BaseWorkSkill> baseWorkSkills = new ArrayList<>();
                Long resumeId = algorithms.getId();
                String skillTag = algorithms.getSkillTag();
                String updatedAt = algorithms.getUpdatedAt();
                JSONObject skillTagJson = null;
                try {
                    skillTagJson = JSONObject.parseObject(skillTag);
                } catch (Exception e) {
                    LOG.info("id:{} algorithms parse json error:{}", resumeId, e.getMessage());
                    e.printStackTrace();
                }
                if (null != skillTagJson && skillTagJson.size() > 0) {
                    JSONObject workJson = skillTagJson.getJSONObject("work");
                    if (null != workJson && workJson.size() > 0) {
                        for (Map.Entry<String, Object> entry : workJson.entrySet()) {
                            String wid = entry.getKey();
                            Map<String, Object> value = (Map<String, Object>) entry.getValue();
                            Object desc1 = value.get("desc");
                            List<Map<String, Object>> descList = (desc1 instanceof List) ?
                                (List<Map<String, Object>>) desc1 : null;
                            if (null != descList && descList.size() > 0) {
                                for (Map<String, Object> desc : descList) {
                                    Object entityIdCandidates1 = desc.get("entityIdCandidates");
                                    List<Map<String, Object>> entityIdCandidatesList = (entityIdCandidates1 instanceof List) ?
                                        (List<Map<String, Object>>) entityIdCandidates1 : null;
                                    if (null != entityIdCandidatesList && entityIdCandidatesList.size() > 0) {
                                        for (Map<String, Object> entityIdCandidates : entityIdCandidatesList) {
                                            BaseWorkSkill baseWorkSkill = new BaseWorkSkill();
                                            baseWorkSkill.setUpdated_at(updatedAt);
                                            baseWorkSkill.setResume_id(resumeId);
                                            baseWorkSkill.setWid(wid);
                                            //entityname
                                            Object entityName1 = entityIdCandidates.get("entityName");
                                            String entityName = null != entityName1 ? String.valueOf(entityName1) : "";
                                            baseWorkSkill.setEntityname(entityName);
                                            //entityid
                                            Object entityID1 = entityIdCandidates.get("entityID");
                                            String entityID = null != entityID1 ? String.valueOf(entityID1) : "";
                                            baseWorkSkill.setEntityid(entityID);
                                            baseWorkSkills.add(baseWorkSkill);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                return baseWorkSkills.iterator();
            }, Encoders.bean(BaseWorkSkill.class))
            .filter((FilterFunction<BaseWorkSkill>) Objects::nonNull);

        //save data to hive
        baseWorkSkillDs.distinct().write().mode(SaveMode.Overwrite).saveAsTable("idw_resume.base_work_skill");


    }

}
