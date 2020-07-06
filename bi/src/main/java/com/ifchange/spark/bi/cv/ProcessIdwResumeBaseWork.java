package com.ifchange.spark.bi.cv;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.bi.bean.cv.BaseWork;
import com.ifchange.spark.bi.bean.cv.ResumeExtra;
import com.ifchange.spark.util.MyString;
import com.ifchange.spark.util.SortMapUtils;
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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * idw_resume 数据处理
 * base_work
 */
public class ProcessIdwResumeBaseWork {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwResumeBaseWork.class);


    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String extraPath = args[2];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        Dataset<BaseWork> baseWorkDataset = sparkSession.read()
            .textFile(extraPath)
            .filter((FilterFunction<String>) s -> {
                boolean flag = false;
                if (StringUtils.isNoneBlank(s)) {
                    String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                    if (split.length == 10) {
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
                String updatedAt = split[8];
                String createdAt = split[9];
                resumeExtra.setCompress(compress);
                resumeExtra.setCreatedAt(createdAt);
                resumeExtra.setUpdatedAt(updatedAt);
                return resumeExtra;
            }, Encoders.bean(ResumeExtra.class))
            .flatMap((FlatMapFunction<ResumeExtra, BaseWork>) resumeExtra -> {
                List<BaseWork> baseWorks = new ArrayList<>();
                String compressStr = resumeExtra.getCompress();
                Long resumeId = resumeExtra.getId();
                String updatedAt = resumeExtra.getUpdatedAt();
                if (StringUtils.isNoneBlank(compressStr)) {
                    String compress = "";
                    try {
                        compress = MyString.unzipString(MyString.hexStringToBytes(compressStr));
                    } catch (Exception e) {
                        LOG.info("id:{} unzip compress error:{}", resumeId, e.getMessage());
                        e.printStackTrace();
                    }
                    JSONObject jsonObject = null;
                    if (StringUtils.isNoneBlank(compress)) {
                        try {
                            jsonObject = JSONObject.parseObject(compress);
                        } catch (Exception e) {
                            LOG.info("id:{} compress cannot parse to json,msg:{}", resumeId, e.getMessage());
                            e.printStackTrace();
                        }
                    }

                    if (null != jsonObject) {
                        Object work = jsonObject.get("work");
                        if (work instanceof Map) {
                            Map<String, Object> workJson = (Map<String, Object>) work;
                            Map<String, Object> sortWork = SortMapUtils.workSort(workJson);
                            if (sortWork.size() > 0) {
                                AtomicInteger size = new AtomicInteger(1);
                                for (Map.Entry<String, Object> entry : sortWork.entrySet()) {
                                    Map<String, Object> workDetail = (Map<String, Object>) entry.getValue();
                                    String isDeleted = null != workDetail.get("is_deleted") ?
                                        String.valueOf(workDetail.get("is_deleted")) : "N";
                                    if ("N".equals(isDeleted)) {
                                        BaseWork baseWork = new BaseWork();
                                        baseWork.setResume_id(resumeId);
                                        String wid = entry.getKey();
                                        baseWork.setWid(wid);
                                        Object sortId1 = workDetail.get("sort_id");
                                        int sortId = 0;
                                        try {
                                            sortId = null != sortId1
                                                ? Integer.parseInt(String.valueOf(sortId1)) : size.get();
                                        } catch (NumberFormatException e) {
                                            LOG.info("sort_id:{} parse error:{}", sortId1, e.getMessage());
                                        }
                                        size.addAndGet(1);
                                        baseWork.setSort_id(sortId);
                                        String startTime = String.valueOf(workDetail.get("start_time"));
                                        baseWork.setStart_time(startTime);
                                        String endTime = String.valueOf(workDetail.get("end_time"));
                                        baseWork.setEnd_time(endTime);
                                        //sofar CASE WHEN work_json->work_key->>'so_far' ='Y' THEN 1 ELSE 0 END AS so_far,
                                        String soFarStr = String.valueOf(workDetail.get("so_far"));
                                        Integer soFar = "Y".equals(soFarStr) ? 1 : 0;
                                        baseWork.setSo_far(soFar);
                                        //CASE WHEN work_json->work_key->>'management_experience'='N' THEN 0 WHEN work_json->work_key->>'management_experience'='Y' THEN 1 ELSE 2 END AS management_experience,
                                        Integer managementExperience;
                                        String managementExperienceStr = String.valueOf(workDetail.get("management_experience"));
                                        if (StringUtils.isNoneBlank(managementExperienceStr)) {
                                            switch (managementExperienceStr) {
                                                case "N":
                                                    managementExperience = 0;
                                                    break;
                                                case "Y":
                                                    managementExperience = 1;
                                                    break;
                                                default:
                                                    managementExperience = 2;
                                                    break;
                                            }
                                        } else {
                                            managementExperience = 2;
                                        }
                                        baseWork.setManagement_experience(managementExperience);
                                        //  CASE WHEN work_json->work_key->>'is_oversea'='N' THEN 0 WHEN work_json->work_key->>'is_oversea'='Y' THEN 1 ELSE 2 END AS is_oversea,
                                        Integer isOversea;
                                        String isOverseaStr = String.valueOf(workDetail.get("is_oversea"));
                                        if (StringUtils.isNoneBlank(isOverseaStr)) {
                                            switch (isOverseaStr) {
                                                case "N":
                                                    isOversea = 0;
                                                    break;
                                                case "Y":
                                                    isOversea = 1;
                                                    break;
                                                default:
                                                    isOversea = 2;
                                                    break;
                                            }
                                        } else {
                                            isOversea = 2;
                                        }
                                        baseWork.setIs_oversea(isOversea);

                                        //updated_at
                                        baseWork.setUpdated_at(updatedAt);
                                        //scale
                                        String scale = String.valueOf(workDetail.get("scale"));
                                        baseWork.setScale(scale);

                                        String industryName = String.valueOf(workDetail.get("industry_name"));
                                        baseWork.setIndustry_name(industryName);

                                        String companyName = String.valueOf(workDetail.get("corporation_name"));
                                        baseWork.setCompany_name(companyName);

                                        String functionName = String.valueOf(workDetail.get("position_name"));
                                        baseWork.setFunction_name(functionName);

                                        String responsibilities = String.valueOf(workDetail.get("responsibilities"));
                                        baseWork.setResponsibilities(responsibilities);

                                        baseWorks.add(baseWork);
                                    }
                                }
                            }
                        }
                    }
                }
                return baseWorks.iterator();
            }, Encoders.bean(BaseWork.class))
            .filter((FilterFunction<BaseWork>) Objects::nonNull);

        //save data to hive
        baseWorkDataset.write().mode(SaveMode.Overwrite).saveAsTable("idw_resume.base_work");


    }

}
