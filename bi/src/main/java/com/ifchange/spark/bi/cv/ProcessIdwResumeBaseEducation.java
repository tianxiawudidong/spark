package com.ifchange.spark.bi.cv;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.bi.bean.cv.Algorithms;
import com.ifchange.spark.bi.bean.cv.BaseEducation;
import com.ifchange.spark.bi.bean.cv.ResumeExtra;
import com.ifchange.spark.util.MyString;
import com.ifchange.spark.util.SortMapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * idw_resume 数据处理
 * base_education
 */
public class ProcessIdwResumeBaseEducation {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwResumeBaseEducation.class);

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String extraPath = args[2];
        String algoPath = args[3];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        Dataset<ResumeExtra> resumeExtraDataset = sparkSession.read()
            .textFile(extraPath)
            .filter((FilterFunction<String>) s -> {
                boolean flag = false;
                if (StringUtils.isNoneBlank(s)) {
                    String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                    /**
                     * resume_extra \t 10
                     */
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
            }, Encoders.bean(ResumeExtra.class));

        Dataset<Algorithms> algorithmsDataset = sparkSession.read()
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
                        String cvEducation = jsonObject.getString("cv_education");
                        algorithms.setCvEducation(cvEducation);
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
            }, Encoders.bean(Algorithms.class));


        Dataset<BaseEducation> baseEducationDataset = resumeExtraDataset
            .join(algorithmsDataset, resumeExtraDataset.col("id").equalTo(algorithmsDataset.col("id")), "left")
            .select(resumeExtraDataset.col("id"),
                resumeExtraDataset.col("compress"),
                resumeExtraDataset.col("updatedAt"),
                algorithmsDataset.col("cvEducation"),
                algorithmsDataset.col("updatedAt"))
            .flatMap((FlatMapFunction<Row, BaseEducation>) row -> {
                List<BaseEducation> list = new ArrayList<>();
                Long resumeId = row.getLong(0);
                String compressStr = null != row.get(1) ? row.getString(1) : "";
                String updatedAt = null != row.get(2) ? row.getString(2) : "";
                String cvEducation = null != row.get(3) ? row.getString(3) : "";

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
                        Object education = jsonObject.get("education");
                        if (education instanceof Map) {
                            Map<String, Object> educationJson = (Map<String, Object>) education;
                            Map<String, Object> educationSort = SortMapUtils.educationSort(educationJson);
                            if (educationSort.size() > 0) {
                                AtomicInteger size = new AtomicInteger(1);
                                for (Map.Entry<String, Object> entry : educationSort.entrySet()) {
                                    Map<String, Object> value = (Map<String, Object>) entry.getValue();
                                    //is_deleted
                                    String isDeleted = null != value.get("is_deleted") ?
                                        String.valueOf(value.get("is_deleted")) : "N";
                                    if ("N".equals(isDeleted)) {
                                        BaseEducation baseEducation = new BaseEducation();
                                        baseEducation.setResume_id(resumeId);
                                        String eid = entry.getKey();
                                        baseEducation.setEid(eid);
                                        /**
                                         * 部分education 沒有sort_id
                                         */
                                        Integer sortId = null != value.get("sort_id")
                                            ? Integer.parseInt(String.valueOf(value.get("sort_id"))) : size.get();
                                        baseEducation.setSort_id(sortId);
                                        size.addAndGet(1);
                                        //start_time
                                        Object startTime1 = value.get("start_time");
                                        String startTime = null != startTime1 ? String.valueOf(startTime1) : "";
                                        baseEducation.setStart_time(startTime);
                                        //end_time
                                        Object endTime1 = value.get("end_time");
                                        String endTime = null != endTime1 ? String.valueOf(endTime1) : "";
                                        baseEducation.setEnd_time(endTime);
                                        //CASE WHEN edu_json->>'so_far' ='Y' THEN 1 ELSE 0 END AS so_far
                                        Object soFar1 = value.get("so_far");
                                        String soFarStr = null != soFar1 ? String.valueOf(soFar1) : "";
                                        Integer soFar = "Y".equals(soFarStr) ? 1 : 0;
                                        baseEducation.setSo_far(soFar);
                                        //updated_at
                                        baseEducation.setUpdated_at(updatedAt);

                                        //t4.school_id,t4.major_id,t4.degree,t4.reindex_degree
                                        if (StringUtils.isNoneBlank(cvEducation)) {
                                            try {
                                                //{"5ab45758df13d":{"major":"","school_id":0,"major_explain":"school_is_skipped","school":"","degree":89,"school_explain":"degree_filter","major_id":0}}
                                                JSONObject cvEduJson = JSONObject.parseObject(cvEducation);
                                                String key;
                                                /**
                                                 * compress edu key 和 algorithms cv_education key大小写不一致
                                                 */
                                                if (cvEduJson.containsKey(eid)) {
                                                    key = eid;
                                                } else if (cvEduJson.containsKey(eid.toUpperCase())) {
                                                    key = eid.toUpperCase();
                                                } else if (cvEduJson.containsKey(eid.toLowerCase())) {
                                                    key = eid.toLowerCase();
                                                } else {
                                                    key = "";
                                                }
                                                if (StringUtils.isNoneBlank(key)) {
                                                    JSONObject eduDetailJson = cvEduJson.getJSONObject(key);
                                                    if (null != eduDetailJson) {
                                                        //school_id
                                                        Object schoolId1 = eduDetailJson.get("school_id");
                                                        Integer schoolId = null != schoolId1 && StringUtils.isNoneBlank(String.valueOf(schoolId1))
                                                            ? Integer.parseInt(String.valueOf(schoolId1)) : 0;
                                                        baseEducation.setSchool_id(schoolId);
                                                        //major_id
                                                        Object majorId1 = eduDetailJson.get("major_id");
                                                        Integer majorId = null != majorId1 && StringUtils.isNoneBlank(String.valueOf(majorId1))
                                                            ? Integer.parseInt(String.valueOf(majorId1)) : 0;
                                                        baseEducation.setMajor_id(majorId);
                                                        //degree
                                                        Object degree1 = eduDetailJson.get("degree");
                                                        Integer degree = null != degree1 && StringUtils.isNoneBlank(String.valueOf(degree1))
                                                            ? Integer.parseInt(String.valueOf(degree1)) : 0;
                                                        baseEducation.setDegree(degree);
                                                        //reindex_degree
                                                        Integer reindexDegree = 0;
                                                        switch (degree) {
                                                            case 1:
                                                            case 92:
                                                                reindexDegree = 3;
                                                                break;
                                                            case 4:
                                                                reindexDegree = 2;
                                                                break;
                                                            case 2:
                                                            case 3:
                                                            case 6:
                                                            case 10:
                                                            case 94:
                                                            case 95:
                                                                reindexDegree = 4;
                                                                break;
                                                            case 86:
                                                            case 87:
                                                            case 89:
                                                            case 90:
                                                            case 91:
                                                                reindexDegree = 1;
                                                                break;
                                                        }
                                                        baseEducation.setReindex_degree(reindexDegree);

                                                        //school_name 算法表
                                                        Object school = eduDetailJson.get("school");
                                                        String schoolName = null != school ? String.valueOf(school) : "";
                                                        baseEducation.setSchool_name(schoolName);
                                                        //major_name 算法表
                                                        Object major = eduDetailJson.get("major");
                                                        String majorName = null != major ? String.valueOf(major) : "";
                                                        baseEducation.setMajor_name(majorName);
                                                    }
                                                }
                                            } catch (Exception e) {
                                                LOG.info("{} parse to json error:{}", cvEducation, e.getMessage());
                                                e.printStackTrace();
                                            }
                                        }
                                        list.add(baseEducation);
                                    }
                                }
                            }
                        }
                    }
                }
                return list.iterator();
            }, Encoders.bean(BaseEducation.class))
            .filter((FilterFunction<BaseEducation>) Objects::nonNull);

        baseEducationDataset.write().mode(SaveMode.Overwrite).saveAsTable("idw_resume.base_education");


    }

}
