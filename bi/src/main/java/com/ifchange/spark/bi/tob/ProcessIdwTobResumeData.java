package com.ifchange.spark.bi.tob;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.bi.bean.cv.Algorithms;
import com.ifchange.spark.bi.bean.cv.BaseIndustryTemp;
import com.ifchange.spark.bi.bean.cv.IndustryMapping;
import com.ifchange.spark.bi.bean.cv.ResumeExtra;
import com.ifchange.spark.bi.bean.tob.*;
import com.ifchange.spark.util.DateTimeUtil;
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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * idw_tob_resume 数据处理
 * base_common
 * base_company
 * base_education
 * base_function
 * base_salary
 * base_work
 * base_work_salary
 * base_work_skill -- tob 没有skill_tag算法
 * base_level
 * base_industry
 */
public class ProcessIdwTobResumeData {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwTobResumeData.class);

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String tobResumeDetailPath = args[2];
        String tobAlgoPath = args[3];
        String industryPath = args[4];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        //加载industry_mapping /user/hive/warehouse/edw_dim_dimension.db/industry_mapping/part-00000-c0e3973b-671c-4549-a23d-6289f643bc6d-c000.txt
        Dataset<IndustryMapping> industryMappingDs = sparkSession.read()
            .textFile(industryPath)
            .map((MapFunction<String, IndustryMapping>) s -> {
                IndustryMapping industryMapping = new IndustryMapping();
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                Integer industryId = Integer.parseInt(split[0]);
                Integer pindustryId = Integer.parseInt(split[1]);
                Integer depth = Integer.parseInt(split[2]);
                industryMapping.setId(industryId);
                industryMapping.setPindustry_id(pindustryId);
                industryMapping.setDepth(depth);
                return industryMapping;
            }, Encoders.bean(IndustryMapping.class));

        Dataset<ResumeExtra> resumeExtraDataset = sparkSession.read()
            .textFile(tobResumeDetailPath)
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
                String updatedAt = split[2];
                String createdAt = split[3];
                resumeExtra.setCompress(compress);
                resumeExtra.setCreatedAt(createdAt);
                resumeExtra.setUpdatedAt(updatedAt);
                return resumeExtra;
            }, Encoders.bean(ResumeExtra.class));

        Dataset<Algorithms> algorithmsDataset = sparkSession.read()
            .textFile(tobAlgoPath)
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
                        JSONObject jsonObject = null;
                        try {
                            jsonObject = JSONObject.parseObject(json);
                        } catch (Exception e) {
                            LOG.info("id:{} algorithms parse json error:{}", id, e.getMessage());
                        }
                        if (null != jsonObject) {
                            String cvDegreeStr = null != jsonObject.get("cv_degree") ?
                                jsonObject.getString("cv_degree") : "";
                            String cvDegree = StringUtils.isNoneBlank(cvDegreeStr) ?
                                new String(MyString.gzipUncompress(Base64.getDecoder().decode(cvDegreeStr))) : "";
                            algorithms.setCvDegree(cvDegree);

                            String cvWorkyearStr = null != jsonObject.get("cv_workyear") ?
                                jsonObject.getString("cv_workyear") : "";
                            String cvWorkyear = StringUtils.isNoneBlank(cvWorkyearStr) ?
                                new String(MyString.gzipUncompress(Base64.getDecoder().decode(cvWorkyearStr))) : "";
                            algorithms.setCvWorkyear(cvWorkyear);

                            String cvQualityStr = null != jsonObject.get("cv_quality") ?
                                jsonObject.getString("cv_quality") : "";
                            String cvQuality = StringUtils.isNoneBlank(cvQualityStr) ?
                                new String(MyString.gzipUncompress(Base64.getDecoder().decode(cvQualityStr))) : "";
                            algorithms.setCvQuality(cvQuality);

                            String cvTradeStr = null != jsonObject.get("cv_trade") ?
                                jsonObject.getString("cv_trade") : "";
                            String cvTrade = StringUtils.isNoneBlank(cvTradeStr) ?
                                new String(MyString.gzipUncompress(Base64.getDecoder().decode(cvTradeStr))) : "";
                            algorithms.setCvTrade(cvTrade);

                            String cvEducationStr = null != jsonObject.get("cv_education") ?
                                jsonObject.getString("cv_education") : "";
                            String cvEducation = StringUtils.isNoneBlank(cvEducationStr) ?
                                new String(MyString.gzipUncompress(Base64.getDecoder().decode(cvEducationStr))) : "";
                            algorithms.setCvEducation(cvEducation);

                            String cvTagStr = null != jsonObject.get("cv_tag") ?
                                jsonObject.getString("cv_tag") : "";
                            String cvTag = StringUtils.isNoneBlank(cvTagStr) ?
                                new String(MyString.gzipUncompress(Base64.getDecoder().decode(cvTagStr))) : "";
                            algorithms.setCvTag(cvTag);

                            String cvTitleStr = null != jsonObject.get("cv_title") ?
                                jsonObject.getString("cv_title") : "";
                            String cvTitle = StringUtils.isNoneBlank(cvTitleStr) ?
                                new String(MyString.gzipUncompress(Base64.getDecoder().decode(cvTitleStr))) : "";
                            algorithms.setCvTitle(cvTitle);
                        }
                    }
                } catch (Exception e) {
                    LOG.error("parse algorithms data error:{}", e.getMessage());
                }

                String updatedAt = split[2];
                String createdAt = split[3];
                algorithms.setCreatedAt(createdAt);
                algorithms.setUpdatedAt(updatedAt);
                return algorithms;
            }, Encoders.bean(Algorithms.class));

        //resume_extra left join algorithms
        Dataset<IdwTobBaseData> idwTobBaseDataDataset = resumeExtraDataset
            .join(algorithmsDataset, resumeExtraDataset.col("id").equalTo(algorithmsDataset.col("id")), "left")
            .select(resumeExtraDataset.col("id"),
                resumeExtraDataset.col("compress"),
                resumeExtraDataset.col("updatedAt"),
                algorithmsDataset.col("cvDegree"),
                algorithmsDataset.col("cvWorkyear"),
                algorithmsDataset.col("cvQuality"),
                algorithmsDataset.col("cvTrade"),
                algorithmsDataset.col("cvEducation"),
                algorithmsDataset.col("cvTag"),
                algorithmsDataset.col("cvTitle"),
                algorithmsDataset.col("updatedAt"))
            .map((MapFunction<Row, IdwTobBaseData>) row -> {
                IdwTobBaseData idwTobBaseData = new IdwTobBaseData();

                Long resumeId = row.getLong(0);
                String compressStr = null != row.get(1) ? row.getString(1) : "";
                String updatedAt = null != row.get(2) ? row.getString(2) : "";
                String cvDegree = null != row.get(3) ? row.getString(3) : "";
                String cvWorkyear = null != row.get(4) ? row.getString(4) : "";
                String cvQuality = null != row.get(5) ? row.getString(5) : "";
                String cvTrade = null != row.get(6) ? row.getString(6) : "";
                String cvEducation = null != row.get(7) ? row.getString(7) : "";
                String cvTag = null != row.get(8) ? row.getString(8) : "";
                String cvTitle = null != row.get(9) ? row.getString(9) : "";
                String algoUpdatedAt = null != row.get(10) ? row.getString(10) : "";

                if (StringUtils.isNoneBlank(compressStr)) {
                    String compress = new String(MyString.gzipUncompress(Base64.getDecoder().decode(compressStr)));
                    JSONObject jsonObject = null;
                    if (StringUtils.isNoneBlank(compress)) {
                        try {
                            jsonObject = JSONObject.parseObject(compress);
                        } catch (Exception e) {
                            LOG.info("id:{} compress cannot parse to json,msg:{}", resumeId, e.getMessage());
                        }
                    }

                    if (null != jsonObject) {
                        JSONObject basic = null;
                        try {
                            basic = jsonObject.getJSONObject("basic");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        if (null != basic) {
                            /*
                             * base_common
                             */
                            IdwTobBaseCommon baseCommon = new IdwTobBaseCommon();
                            baseCommon.setResume_id(resumeId);
                            String name = null != basic.get("name") ? basic.getString("name") : "";
                            baseCommon.setName(name);
                            String genderStr = null != basic.get("gender") ? basic.getString("gender") : "";
                            Integer gender;
                            //CASE WHEN basic->>'gender'='M' THEN 0 WHEN basic->>'gender'='F' THEN 1 ELSE 2 END AS gender,
                            if (StringUtils.isNoneBlank(genderStr)) {
                                switch (genderStr) {
                                    case "M":
                                        gender = 0;
                                        break;
                                    case "F":
                                        gender = 1;
                                        break;
                                    default:
                                        gender = 2;
                                        break;
                                }
                            } else {
                                gender = 2;
                            }
                            baseCommon.setGender(gender);
                            //expect_position_name
                            String expectPositionName = null != basic.get("expect_position_name") ? basic.getString("expect_position_name") : "";
                            baseCommon.setExpect_position_name(expectPositionName);
                            String birth = null != basic.get("birth") ? basic.getString("birth") : "";
                            baseCommon.setBirth(birth);
                            String maritalStr = null != basic.get("marital") ? basic.getString("marital") : "";
                            Integer marital;
                            // CASE WHEN basic->>'marital'='N' THEN 0 WHEN basic->>'marital'='Y' THEN 1 ELSE 2 END AS marital,
                            if (StringUtils.isNoneBlank(maritalStr)) {
                                switch (maritalStr) {
                                    case "N":
                                        marital = 0;
                                        break;
                                    case "Y":
                                        marital = 1;
                                        break;
                                    default:
                                        marital = 2;
                                        break;
                                }
                            } else {
                                marital = 2;
                            }
                            baseCommon.setMarital(marital);

                            Integer accountProvince = 0;
                            try {
                                Object accountProvinceObj = basic.get("account_province");
                                accountProvince = null != accountProvinceObj
                                    && StringUtils.isNoneBlank(String.valueOf(accountProvinceObj))
                                    ? Integer.parseInt(String.valueOf(accountProvince)) : 0;
                            } catch (Exception e) {
                                LOG.info("id:{} basic.account_province parse error:{}", resumeId, e.getMessage());
                            }
                            baseCommon.setAccount_province(accountProvince);

                            Integer accountCity = 0;
                            try {
                                Object account = basic.get("account");
                                accountCity = null != account && StringUtils.isNoneBlank(String.valueOf(account))
                                    ? Integer.parseInt(String.valueOf(account)) : 0;
                            } catch (Exception e) {
                                LOG.info("id:{} basic.account parse error:{}", resumeId, e.getMessage());
                            }
                            baseCommon.setAccount_city(accountCity);

                            Integer addressProvince = 0;
                            try {
                                Object addressProvinceObj = basic.get("address_province");
                                addressProvince = null != addressProvinceObj
                                    && StringUtils.isNoneBlank(String.valueOf(addressProvinceObj))
                                    ? Integer.parseInt(String.valueOf(addressProvinceObj)) : 0;
                            } catch (Exception e) {
                                LOG.info("id:{} basic.address_province parse error:{}", resumeId, e.getMessage());
                            }
                            baseCommon.setAddress_province(addressProvince);

                            Integer address = 0;
                            try {
                                Object addressObj = basic.get("address");
                                address = null != addressObj && StringUtils.isNoneBlank(String.valueOf(addressObj))
                                    ? Integer.parseInt(String.valueOf(addressObj)) : 0;
                            } catch (Exception e) {
                                LOG.info("id:{} basic.address parse error:{}", resumeId, e.getMessage());
                            }
                            baseCommon.setAddress(address);

                            Integer currentStatus = 0;
                            try {
                                Object currentStatusObj = basic.get("current_status");
                                currentStatus = null != currentStatusObj
                                    && StringUtils.isNoneBlank(String.valueOf(currentStatusObj)) ?
                                    Integer.parseInt(String.valueOf(currentStatusObj)) : 0;
                            } catch (Exception e) {
                                LOG.info("id:{} basic.current_status parse error:{}", resumeId, e.getMessage());
                            }
                            baseCommon.setCurrent_status(currentStatus);

                            //CASE WHEN basic->>'management_experience'='N' THEN '0' WHEN basic->>'management_experience'='Y' THEN 1 ELSE 2 END AS
                            Integer managementExperience;
                            Object managementExperienceObj = basic.get("management_experience");
                            String managementExperienceStr = null != managementExperienceObj ?
                                basic.getString("management_experience") : "";
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
                            baseCommon.setManagement_experience(managementExperience);

                            String resumeUpdateAt = null != basic.get("resume_updated_at") ? basic.getString("resume_updated_at") : "";
                            baseCommon.setResume_updated_at(resumeUpdateAt);

                            baseCommon.setUpdated_at(updatedAt);

                            //degree algorithms.cv_degree
                            //work_experience  algorithms.cv_workyear
                            //quality algorithms.cv_quality

                            Integer degree = 0;
                            try {
                                degree = StringUtils.isNoneBlank(cvDegree) ? Integer.parseInt(cvDegree) : 0;
                            } catch (NumberFormatException e) {
                                LOG.info("cvDegree:{} parse to int error:{}", cvDegree, e.getMessage());
                            }
                            baseCommon.setDegree(degree);

                            Integer workYear = 0;
                            try {
                                workYear = StringUtils.isNoneBlank(cvWorkyear) ? Integer.parseInt(String.valueOf(cvWorkyear)) : 0;
                            } catch (NumberFormatException e) {
                                LOG.info("cvWorkyear:{} parse to int error:{}", cvWorkyear, e.getMessage());
                            }
                            baseCommon.setWork_experience(workYear);

                            Double quality = 0.0;
                            if (StringUtils.isNoneBlank(cvQuality)) {
                                try {
                                    quality = Double.parseDouble(cvQuality);
                                } catch (Exception e) {
                                    LOG.info("cvQuality:{} parse to double error:{}", cvWorkyear, e.getMessage());
                                }
                            }
                            baseCommon.setQuality(quality);

                            Double expectSalaryFrom = 0.0;
                            Object expectSalaryFromObj = basic.get("expect_salary_from");
                            try {
                                if (null != expectSalaryFromObj && StringUtils.isNoneBlank(String.valueOf(expectSalaryFromObj))) {
                                    String expectSalaryFromStr = String.valueOf(expectSalaryFromObj).trim();
                                    if (expectSalaryFromStr.contains(",")) {
                                        expectSalaryFromStr = expectSalaryFromStr.replace(",", "");
                                    }
                                    expectSalaryFrom = Double.parseDouble(expectSalaryFromStr);
                                }
                                if (expectSalaryFrom >= 200) {
                                    expectSalaryFrom = expectSalaryFrom / 1000.0;
                                }
                                BigDecimal bd2 = new BigDecimal(expectSalaryFrom);
                                expectSalaryFrom = bd2.setScale(1, RoundingMode.HALF_UP).doubleValue();
                            } catch (Exception e) {
                                LOG.info("compute expect_salary_from:{} error:{}", expectSalaryFromObj, e.getMessage());
                                e.printStackTrace();
                            }
                            baseCommon.setExpect_salary_from(expectSalaryFrom);

                            Double expectSalaryTo = 0.0;
                            Object expectSalaryToObj = basic.get("expect_salary_to");
                            try {
                                if (null != expectSalaryToObj && StringUtils.isNoneBlank(String.valueOf(expectSalaryToObj))) {
                                    String expectSalaryToStr = String.valueOf(expectSalaryToObj).trim();
                                    if (expectSalaryToStr.contains(",")) {
                                        expectSalaryToStr = expectSalaryToStr.replace(",", "");
                                    }
                                    expectSalaryTo = Double.parseDouble(expectSalaryToStr);
                                }
                                if (expectSalaryTo >= 200) {
                                    expectSalaryTo = expectSalaryTo / 1000.0;
                                }
                                BigDecimal bd2 = new BigDecimal(expectSalaryTo);
                                expectSalaryTo = bd2.setScale(1, RoundingMode.HALF_UP).doubleValue();
                            } catch (Exception e) {
                                LOG.info("compute expect_salary_from:{} error:{}", expectSalaryFromObj, e.getMessage());
                                e.printStackTrace();
                            }
                            baseCommon.setExpect_salary_to(expectSalaryTo);

                            idwTobBaseData.setBaseCommon(baseCommon);
                        }

                        /*
                         * base_contact
                         */
                        IdwTobBaseContact baseContact = new IdwTobBaseContact();
                        Object contact = jsonObject.get("contact");
                        if (contact instanceof Map) {
                            Map<String, Object> contactMap = (Map<String, Object>) contact;
                            if (contactMap.size() > 0) {
                                String name = null != contactMap.get("name") ? String.valueOf(contactMap.get("name")) : "";
                                String phone = null != contactMap.get("phone") ? String.valueOf(contactMap.get("phone")) : "";
                                String email = null != contactMap.get("email") ? String.valueOf(contactMap.get("email")) : "";
                                baseContact.setResume_id(resumeId);
                                baseContact.setName(name);
                                baseContact.setPhone(phone);
                                baseContact.setEmail(email);
                                idwTobBaseData.setBaseContact(baseContact);
                            }
                        } else if (contact instanceof List) {
                            List contactList = (List) contact;
                            if (null != contactList && contactList.size() > 0) {
                                Object contactObj = contactList.get(0);
                                if (contactObj instanceof Map) {
                                    Map<String, Object> contactMap = (Map<String, Object>) contactObj;
                                    if (contactMap.size() > 0) {
                                        String name = null != contactMap.get("name") ? String.valueOf(contactMap.get("name")) : "";
                                        String phone = null != contactMap.get("phone") ? String.valueOf(contactMap.get("phone")) : "";
                                        String email = null != contactMap.get("email") ? String.valueOf(contactMap.get("email")) : "";
                                        baseContact.setResume_id(resumeId);
                                        baseContact.setName(name);
                                        baseContact.setPhone(phone);
                                        baseContact.setEmail(email);
                                        idwTobBaseData.setBaseContact(baseContact);
                                    }
                                }
                            }
                        } else {
                            LOG.info("contact:{} is not {} or []", contact);
                        }


                        /*
                         * base_education
                         */
                        List<IdwTobBaseEducation> baseEducations = new ArrayList<>();
                        Object education = jsonObject.get("education");
                        if (education instanceof Map) {
                            Map<String, Object> educationJson = (Map<String, Object>) education;
                            Map<String, Object> educationSort = SortMapUtils.educationSort(educationJson);
                            if (educationSort.size() > 0) {
                                AtomicInteger size = new AtomicInteger(1);
                                for (Map.Entry<String, Object> entry : educationSort.entrySet()) {
                                    Object value1 = entry.getValue();
                                    if (value1 instanceof Map) {
                                        Map<String, Object> value = (Map<String, Object>) value1;
                                        //is_deleted
                                        String isDeleted = null != value.get("is_deleted") ?
                                            String.valueOf(value.get("is_deleted")) : "N";
                                        IdwTobBaseEducation baseEducation = new IdwTobBaseEducation();
                                        baseEducation.setResume_id(resumeId);
                                        String eid = entry.getKey();
                                        baseEducation.setEid(eid);
                                        /*
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
                                                /*
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
                                        baseEducations.add(baseEducation);
                                    }
                                }
                            }
                            idwTobBaseData.setBaseEducations(baseEducations);
                        }


                        /*
                         * base_function
                         */
                        List<IdwTobBaseFunction> baseFunctions = new ArrayList<>();
                        if (StringUtils.isNoneBlank(cvTag) && !StringUtils.equals("\"\"", cvTag)) {
                            JSONObject cvTagJson = null;
                            try {
                                cvTagJson = JSONObject.parseObject(cvTag);
                            } catch (Exception e) {
                                LOG.info("cv_tag:{} cannot parse to json", cvTag);
                            }
                            if (null != cvTagJson && cvTagJson.size() > 0) {
                                for (Map.Entry<String, Object> entry : cvTagJson.entrySet()) {
                                    String wid = entry.getKey();
                                    Object value1 = entry.getValue();
                                    if (value1 instanceof Map) {
                                        Map<String, Object> value = (Map<String, Object>) value1;
                                        //二级职能
                                        String category = null != value.get("category") ? String.valueOf(value.get("category")) : "";
                                        if (StringUtils.isNoneBlank(category)) {
                                            String[] split = category.split(":");
                                            if (split.length == 2) {
                                                //function_id
                                                Integer functionId = 0;
                                                try {
                                                    functionId = Integer.parseInt(split[0]);
                                                } catch (Exception e) {
                                                    LOG.error("id:{} cv_tag.category:{} parse Integer error", resumeId, category);
                                                }
                                                IdwTobBaseFunction baseFunction = new IdwTobBaseFunction();
                                                baseFunction.setUpdated_at(algoUpdatedAt);
                                                baseFunction.setResume_id(resumeId);
                                                baseFunction.setWid(wid);
                                                baseFunction.setFunction_id(functionId);
                                                //value
                                                Double values = Double.parseDouble(split[1]);
                                                baseFunction.setValue(values);
                                                baseFunctions.add(baseFunction);
                                            }
                                        }

                                        //三级职能
                                        Object must1 = value.get("must");
                                        List<String> mustList = (must1 instanceof List) ? (List<String>) must1 : null;
                                        if (null != mustList && mustList.size() > 0) {
                                            String must = mustList.get(0);
                                            if (StringUtils.isNoneBlank(must)) {
                                                String[] split1 = must.split(":");
                                                if (split1.length == 2) {
                                                    IdwTobBaseFunction baseFunction = new IdwTobBaseFunction();
                                                    baseFunction.setUpdated_at(algoUpdatedAt);
                                                    baseFunction.setResume_id(resumeId);
                                                    baseFunction.setWid(wid);
                                                    //function_id
                                                    Integer functionId3 = Integer.parseInt(split1[0]);
                                                    baseFunction.setFunction_id(functionId3);
                                                    //value
                                                    Double values3 = Double.parseDouble(split1[1]);
                                                    baseFunction.setValue(values3);
                                                    baseFunctions.add(baseFunction);
                                                }
                                            }
                                        }

                                        //四级职能
                                        Object should1 = value.get("should");
                                        List<String> shouldList = (should1 instanceof List) ? (List<String>) should1 : null;
                                        if (null != shouldList && shouldList.size() > 0) {
                                            String should = shouldList.get(0);
                                            if (StringUtils.isNoneBlank(should)) {
                                                String[] split2 = should.split(":");
                                                if (split2.length == 2) {
                                                    IdwTobBaseFunction baseFunction = new IdwTobBaseFunction();
                                                    baseFunction.setUpdated_at(algoUpdatedAt);
                                                    baseFunction.setResume_id(resumeId);
                                                    baseFunction.setWid(wid);
                                                    //function_id
                                                    Integer functionId4 = Integer.parseInt(split2[0]);
                                                    baseFunction.setFunction_id(functionId4);
                                                    //value
                                                    Double values4 = Double.parseDouble(split2[1]);
                                                    baseFunction.setValue(values4);
                                                    baseFunctions.add(baseFunction);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            idwTobBaseData.setBaseFunctions(baseFunctions);
                        }

                        /*
                         * base_company
                         */
                        List<IdwTobBaseCompany> baseCompanys = new ArrayList<>();
                        if (StringUtils.isNoneBlank(cvTrade)) {
                            JSONArray array = null;
                            try {
                                array = JSONObject.parseArray(cvTrade);
                            } catch (Exception e) {
                                LOG.info("id:{} cvTrade parse json error:{}", resumeId, e.getMessage());
                            }
                            if (null != array && array.size() > 0) {
                                for (Integer i = 0; i < array.size(); i++) {
                                    IdwTobBaseCompany baseCompany = new IdwTobBaseCompany();
                                    baseCompany.setResume_id(resumeId);
                                    baseCompany.setUpdated_at(algoUpdatedAt);
                                    JSONObject json = array.getJSONObject(i);
                                    if (null != json && json.size() > 0) {
                                        String wid = json.getString("work_id");
                                        Integer companyId = json.getInteger("company_id");
                                        JSONObject companyInfoJson = json.getJSONObject("company_info");
                                        String regionStr = "";
                                        if (null != companyInfoJson) {
                                            Object region = companyInfoJson.get("region");
                                            if (region instanceof List) {
                                                List regionList = (List) region;
                                                if (null != regionList && regionList.size() > 0) {
                                                    regionStr = String.valueOf(regionList.get(0));
                                                }
                                            }
                                        }
                                        baseCompany.setWid(wid);
                                        baseCompany.setCompany_id(companyId);
                                        baseCompany.setRegion(regionStr);
                                        baseCompanys.add(baseCompany);
                                    }
                                }
                            }
                            idwTobBaseData.setBaseCompanys(baseCompanys);
                        }

                        /*
                         * base_salary
                         */
                        if (null != basic) {
                            IdwTobBaseSalary baseSalary = new IdwTobBaseSalary();
                            baseSalary.setResume_id(resumeId);
                            //basic_salary 1、整数OR浮点数 2、数字+万元/年 3、字符串：null 4、空("")
                            //换算成月薪大于1000的转换为K
                            Double basicSalary = 0.0;
                            Object basicSalaryObj = basic.get("basic_salary");
                            try {
                                if (null != basicSalaryObj && StringUtils.isNoneBlank(String.valueOf(basicSalaryObj))) {
                                    String basicSalaryStr = String.valueOf(basicSalaryObj).trim();
                                    if (basicSalaryStr.contains("万元")) {
                                        basicSalaryStr = basicSalaryStr.substring(0, basicSalaryStr.indexOf("万"));
                                        basicSalary = Double.parseDouble(basicSalaryStr) * 10000 / 12;
                                    } else {
                                        basicSalary = Double.parseDouble(basicSalaryStr);
                                    }
                                }
                                if (basicSalary >= 200) {
                                    basicSalary = basicSalary / 1000.0;
                                }
                                BigDecimal bd = new BigDecimal(basicSalary);
                                basicSalary = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                            } catch (Exception e) {
                                LOG.info("compute basic_salary:{} error:{}", basicSalaryObj, e.getMessage());
                                e.printStackTrace();
                            }
                            baseSalary.setBasic_salary(basicSalary);

                            //--basic_salary_from原始数据类型：1、整数OR浮点数 2、空("") 3、字符串：保密 4、其他
                            Double basicSalaryFrom = 0.0;
                            Object basicSalaryFromObj = basic.get("basic_salary_from");
                            try {
                                if (null != basicSalaryFromObj && StringUtils.isNoneBlank(String.valueOf(basicSalaryFromObj))) {
                                    String basicSalaryFromStr = String.valueOf(basicSalaryFromObj).trim();
                                    basicSalaryFrom = Double.parseDouble(basicSalaryFromStr);
                                }
                                if (basicSalaryFrom >= 200) {
                                    basicSalaryFrom = basicSalaryFrom / 1000.0;
                                }
                                BigDecimal bd = new BigDecimal(basicSalaryFrom);
                                basicSalaryFrom = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                            } catch (Exception e) {
                                LOG.info("compute basic_salary_from:{} error:{}", basicSalaryFromObj, e.getMessage());
                                e.printStackTrace();
                            }
                            baseSalary.setBasic_salary_from(basicSalaryFrom);

                            //--basic_salary_to原始数据类型：1、整数OR浮点数 2、空("") 3、字符串：保密 4、其他
                            Double basicSalaryTo = 0.0;
                            Object basicSalaryToObj = basic.get("basic_salary_to");
                            try {
                                if (null != basicSalaryToObj && StringUtils.isNoneBlank(String.valueOf(basicSalaryToObj))) {
                                    String basicSalaryToStr = String.valueOf(basicSalaryToObj).trim();
                                    basicSalaryTo = Double.parseDouble(basicSalaryToStr);
                                }
                                if (basicSalaryTo >= 200) {
                                    basicSalaryTo = basicSalaryTo / 1000.0;
                                }
                                BigDecimal bd = new BigDecimal(basicSalaryTo);
                                basicSalaryTo = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                            } catch (Exception e) {
                                LOG.info("compute basic_salary_to:{} error:{}", basicSalaryToObj, e.getMessage());
                                e.printStackTrace();
                            }
                            baseSalary.setBasic_salary_to(basicSalaryTo);

                            //--annual_salary原始数据类型：1、整数OR浮点数 2、空("") 3、字符串：null 4、其他
                            Double annualSalary = 0.0;
                            Object annualSalaryObj = basic.get("annual_salary");
                            try {
                                if (null != annualSalaryObj && StringUtils.isNoneBlank(String.valueOf(annualSalaryObj))) {
                                    String annualSalaryStr = String.valueOf(annualSalaryObj).trim();
                                    annualSalary = Double.parseDouble(annualSalaryStr);
                                }
                                if (annualSalary >= 200 * 12) {
                                    annualSalary = annualSalary / 1000.0;
                                }
                                BigDecimal bd = new BigDecimal(annualSalary);
                                annualSalary = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                            } catch (Exception e) {
                                LOG.info("compute annual_salary:{} error:{}", annualSalaryObj, e.getMessage());
                                e.printStackTrace();
                            }
                            baseSalary.setAnnual_salary(annualSalary);


                            //--annual_salary_from原始数据类型：1、整数OR浮点数 2、空("") 3、带逗号的数字（1,000.00） 4、其他
                            Double annualSalaryFrom = 0.0;
                            Object annualSalaryFromObj = basic.get("annual_salary_from");
                            try {
                                if (null != annualSalaryFromObj && StringUtils.isNoneBlank(String.valueOf(annualSalaryFromObj))) {
                                    String annualSalaryFromStr = String.valueOf(annualSalaryFromObj).trim();
//                                                            Pattern.compile("^(\\d+|\\d+\\.\\d+)$");
                                    if (annualSalaryFromStr.contains(",")) {
                                        annualSalaryFromStr = annualSalaryFromStr.replace(",", "");
                                    }
                                    annualSalaryFrom = Double.parseDouble(annualSalaryFromStr);
                                }
                                if (annualSalaryFrom >= 200 * 12) {
                                    annualSalaryFrom = annualSalaryFrom / 1000.0;
                                }
                                BigDecimal bd = new BigDecimal(annualSalaryFrom);
                                annualSalaryFrom = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                            } catch (Exception e) {
                                LOG.info("compute annual_salary_from:{} error:{}", annualSalaryFromObj, e.getMessage());
                                e.printStackTrace();
                            }
                            baseSalary.setAnnual_salary_from(annualSalaryFrom);

                            //--annual_salary_to原始数据类型：1、整数OR浮点数 2、空("") 3、带逗号的数字（1,000.00） 4、其他
                            Double annualSalaryTo = 0.0;
                            Object annualSalaryToObj = basic.get("annual_salary_to");
                            try {
                                if (null != annualSalaryToObj && StringUtils.isNoneBlank(String.valueOf(annualSalaryToObj))) {
                                    String annualSalaryToStr = String.valueOf(annualSalaryToObj).trim();
//                                                            Pattern.compile("^(\\d+|\\d+\\.\\d+)$");
                                    if (annualSalaryToStr.contains(",")) {
                                        annualSalaryToStr = annualSalaryToStr.replace(",", "");
                                    }
                                    annualSalaryTo = Double.parseDouble(annualSalaryToStr);
                                }
                                if (annualSalaryTo >= 200 * 12) {
                                    annualSalaryTo = annualSalaryTo / 1000.0;
                                }
                                BigDecimal bd = new BigDecimal(annualSalaryTo);
                                annualSalaryTo = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                            } catch (Exception e) {
                                LOG.info("compute annual_salary_to:{} error:{}", annualSalaryToObj, e.getMessage());
                                e.printStackTrace();
                            }
                            baseSalary.setAnnual_salary_to(annualSalaryTo);

                            //expect_salary_from原始数据类型：1、整数OR浮点数 2、空("") 3、带逗号的数字（1,000.00） 4、其他
                            Double expectSalaryFrom = 0.0;
                            Object expectSalaryFromObj = basic.get("expect_salary_from");
                            try {
                                if (null != expectSalaryFromObj && StringUtils.isNoneBlank(String.valueOf(expectSalaryFromObj))) {
                                    String expectSalaryFromStr = String.valueOf(expectSalaryFromObj).trim();
//                                                            Pattern.compile("^(\\d+|\\d+\\.\\d+)$");
                                    if (expectSalaryFromStr.contains(",")) {
                                        expectSalaryFromStr = expectSalaryFromStr.replace(",", "");
                                    }
                                    expectSalaryFrom = Double.parseDouble(expectSalaryFromStr);
                                }
                                if (expectSalaryFrom >= 200) {
                                    expectSalaryFrom = expectSalaryFrom / 1000.0;
                                }
                                BigDecimal bd = new BigDecimal(expectSalaryFrom);
                                expectSalaryFrom = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                            } catch (Exception e) {
                                LOG.info("compute expect_salary_from:{} error:{}", expectSalaryFromObj, e.getMessage());
                                e.printStackTrace();
                            }
                            baseSalary.setExpect_salary_from(expectSalaryFrom);

                            //expect_salary_to原始数据类型：1、整数OR浮点数 2、空("") 3、带逗号的数字（1,000.00） 4、其他
                            Double expectSalaryTo = 0.0;
                            Object expectSalaryToObj = basic.get("expect_salary_to");
                            try {
                                if (null != expectSalaryToObj && StringUtils.isNoneBlank(String.valueOf(expectSalaryToObj))) {
                                    String expectSalaryToStr = String.valueOf(expectSalaryToObj).trim();
//                                                            Pattern.compile("^(\\d+|\\d+\\.\\d+)$");
                                    if (expectSalaryToStr.contains(",")) {
                                        expectSalaryToStr = expectSalaryToStr.replace(",", "");
                                    }
                                    expectSalaryTo = Double.parseDouble(expectSalaryToStr);
                                }
                                if (expectSalaryTo >= 200) {
                                    expectSalaryTo = expectSalaryTo / 1000.0;
                                }
                                BigDecimal bd = new BigDecimal(expectSalaryTo);
                                expectSalaryTo = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                            } catch (Exception e) {
                                LOG.info("compute expect_salary_to:{} error:{}", expectSalaryToObj, e.getMessage());
                                e.printStackTrace();
                            }
                            baseSalary.setExpect_salary_to(expectSalaryTo);

                            //--expect_annual_salary原始数据类型：1、整数OR浮点数 2、空("") 3、带逗号的数字（1,000.00） 4、其他
                            Double expectAnnualSalary = 0.0;
                            Object expectAnnualSalaryObj = basic.get("expect_annual_salary");
                            try {
                                if (null != expectAnnualSalaryObj && StringUtils.isNoneBlank(String.valueOf(expectAnnualSalaryObj))) {
                                    String expectAnnualSalaryStr = String.valueOf(expectAnnualSalaryObj).trim();
                                    if (expectAnnualSalaryStr.contains(",")) {
                                        expectAnnualSalaryStr = expectAnnualSalaryStr.replace(",", "");
                                    }
                                    expectAnnualSalary = Double.parseDouble(expectAnnualSalaryStr);
                                }
                                if (expectAnnualSalary >= 200 * 12) {
                                    expectAnnualSalary = expectAnnualSalary / 1000.0;
                                }
                                BigDecimal bd = new BigDecimal(expectAnnualSalary);
                                expectAnnualSalary = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                            } catch (Exception e) {
                                LOG.info("compute expect_annual_salary:{} error:{}", expectAnnualSalaryObj, e.getMessage());
                                e.printStackTrace();
                            }
                            baseSalary.setExpect_annual_salary(expectAnnualSalary);

                            //--expect_annual_salary_from原始数据类型：1、整数OR浮点数 2、空("") 3、带逗号的数字（1,000.00） 4、其他
                            Double expectAnnualSalaryFrom = 0.0;
                            Object expectAnnualSalaryFromObj = basic.get("expect_annual_salary_from");
                            try {
                                if (null != expectAnnualSalaryFromObj && StringUtils.isNoneBlank(String.valueOf(expectAnnualSalaryFromObj))) {
                                    String expectAnnualSalaryFromStr = String.valueOf(expectAnnualSalaryFromObj).trim();
                                    if (expectAnnualSalaryFromStr.contains(",")) {
                                        expectAnnualSalaryFromStr = expectAnnualSalaryFromStr.replace(",", "");
                                    }
                                    expectAnnualSalaryFrom = Double.parseDouble(expectAnnualSalaryFromStr);
                                }
                                if (expectAnnualSalaryFrom >= 200 * 12) {
                                    expectAnnualSalaryFrom = expectAnnualSalaryFrom / 1000.0;
                                }
                                BigDecimal bd = new BigDecimal(expectAnnualSalaryFrom);
                                expectAnnualSalaryFrom = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                            } catch (Exception e) {
                                LOG.info("compute expect_annual_salary_from:{} error:{}", expectAnnualSalaryFromObj, e.getMessage());
                                e.printStackTrace();
                            }
                            baseSalary.setExpect_annual_salary_from(expectAnnualSalaryFrom);

                            //--expect_annual_salary_to原始数据类型：1、整数OR浮点数 2、空("") 3、带逗号的数字（1,000.00） 4、其他
                            Double expectAnnualSalaryTo = 0.0;
                            Object expectAnnualSalaryToObj = basic.get("expect_annual_salary_to");
                            try {
                                if (null != expectAnnualSalaryToObj && StringUtils.isNoneBlank(String.valueOf(expectAnnualSalaryToObj))) {
                                    String expectAnnualSalaryToStr = String.valueOf(expectAnnualSalaryToObj).trim();
                                    if (expectAnnualSalaryToStr.contains(",")) {
                                        expectAnnualSalaryToStr = expectAnnualSalaryToStr.replace(",", "");
                                    }
                                    expectAnnualSalaryTo = Double.parseDouble(expectAnnualSalaryToStr);
                                }
                                if (expectAnnualSalaryTo >= 200 * 12) {
                                    expectAnnualSalaryTo = expectAnnualSalaryTo / 1000.0;
                                }
                                BigDecimal bd = new BigDecimal(expectAnnualSalaryTo);
                                expectAnnualSalaryTo = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                            } catch (Exception e) {
                                LOG.info("compute expect_annual_salary_to:{} error:{}", expectAnnualSalaryToObj, e.getMessage());
                                e.printStackTrace();
                            }
                            baseSalary.setExpect_annual_salary_to(expectAnnualSalaryTo);
                            baseSalary.setUpdated_at(updatedAt);
                            idwTobBaseData.setBaseSalary(baseSalary);
                        }

                        /*
                         * base_work
                         */
                        List<IdwTobBaseWork> baseWorks = new ArrayList<>();
                        Object work = jsonObject.get("work");
                        if (work instanceof Map) {
                            Map<String, Object> workJson = (Map<String, Object>) work;
                            //tob
                            //发现部分work 没有sort_id
                            //重排
                            Map<String, Object> sortWork = SortMapUtils.workSort(workJson);
                            int totalWorkTime = DateTimeUtil.computeTotalWorkTime(sortWork);
                            String firstWorkTime = DateTimeUtil.getFirstWorkTime(sortWork);
                            LOG.info("id:{},first_work_time:{},total_work_time:{}", resumeId, firstWorkTime, totalWorkTime);
                            if (sortWork.size() > 0) {
                                for (Map.Entry<String, Object> entry : sortWork.entrySet()) {
                                    Object value = entry.getValue();
                                    if (value instanceof Map) {
                                        Map<String, Object> workDetail = (Map<String, Object>) value;
                                        IdwTobBaseWork baseWork = new IdwTobBaseWork();
                                        baseWork.setResume_id(resumeId);
                                        String wid = entry.getKey();
                                        baseWork.setWid(wid);
                                        Object sortId1 = workDetail.get("sort_id");
                                        int sortId;
                                        try {
                                            sortId = Integer.parseInt(String.valueOf(sortId1));
                                        } catch (Exception e) {
                                            LOG.info("id:{},wid:{},sort_id :{} parse error:{}", resumeId, wid,
                                                sortId1, e.getMessage());
                                            sortId = 0;
                                        }
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
                                        baseWork.setFirst_work_time(firstWorkTime);
                                        baseWork.setTotal_work_time(totalWorkTime);

                                        String responsibilities = String.valueOf(workDetail.get("responsibilities"));
                                        baseWork.setResponsibilities(responsibilities);

                                        baseWorks.add(baseWork);
                                    }
                                }
                            }
                            idwTobBaseData.setBaseWorks(baseWorks);
                        }


                        /*
                         * base_work_salary
                         */
                        List<IdwTobBaseWorkSalary> baseWorkSalaries = new ArrayList<>();
                        if (work instanceof Map) {
                            Map<String, Object> workJson = (Map<String, Object>) work;
                            if (workJson.size() > 0) {
                                Map<String, Object> sortWork = SortMapUtils.workSort(workJson);
                                for (Map.Entry<String, Object> entry : sortWork.entrySet()) {
                                    Object value = entry.getValue();
                                    if (value instanceof Map) {
                                        Map<String, Object> workDetail = (Map<String, Object>) value;
                                        String isDeleted = null != workDetail.get("is_deleted") ?
                                            String.valueOf(workDetail.get("is_deleted")) : "N";
                                        IdwTobBaseWorkSalary baseWorkSalary = new IdwTobBaseWorkSalary();
                                        baseWorkSalary.setResume_id(resumeId);
                                        String wid = entry.getKey();
                                        baseWorkSalary.setWid(wid);
                                        Object sortId1 = workDetail.get("sort_id");
                                        int sortId;
                                        try {
                                            sortId = Integer.parseInt(String.valueOf(sortId1));
                                        } catch (Exception e) {
                                            LOG.info("sort_id :{} parse error:{}", sortId1, e.getMessage());
                                            sortId = 0;
                                        }
                                        baseWorkSalary.setSort_id(sortId);

                                        //basic_salary 1、整数OR浮点数 2、数字+万元/年 3、字符串：null 4、空("")
                                        //换算成月薪大于1000的转换为K
                                        Double basicSalary = 0.0;
                                        Object basicSalaryObj = workDetail.get("basic_salary");
                                        try {
                                            if (null != basicSalaryObj && StringUtils.isNoneBlank(String.valueOf(basicSalaryObj))) {
                                                String basicSalaryStr = String.valueOf(basicSalaryObj).trim();
                                                if (basicSalaryStr.contains("万元")) {
                                                    basicSalaryStr = basicSalaryStr.substring(0, basicSalaryStr.indexOf("万"));
                                                    basicSalary = Double.parseDouble(basicSalaryStr) * 10000 / 12;
                                                } else {
                                                    basicSalary = Double.parseDouble(basicSalaryStr);
                                                }
                                            }
                                            if (basicSalary >= 200) {
                                                basicSalary = basicSalary / 1000.0;
                                            }
                                            BigDecimal bd = new BigDecimal(basicSalary);
                                            basicSalary = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                                        } catch (Exception e) {
                                            LOG.info("compute basic_salary:{} error:{}", basicSalaryObj, e.getMessage());
                                            e.printStackTrace();
                                        }
                                        baseWorkSalary.setBasic_salary(basicSalary);

                                        //--basic_salary_from原始数据类型：1、整数OR浮点数 2、空("") 3、字符串：保密 4、其他
                                        Double basicSalaryFrom = 0.0;
                                        Object basicSalaryFromObj = workDetail.get("basic_salary_from");
                                        try {
                                            if (null != basicSalaryFromObj && StringUtils.isNoneBlank(String.valueOf(basicSalaryFromObj))) {
                                                String basicSalaryFromStr = String.valueOf(basicSalaryFromObj).trim();
                                                basicSalaryFrom = Double.parseDouble(basicSalaryFromStr);
                                            }
                                            if (basicSalaryFrom >= 200) {
                                                basicSalaryFrom = basicSalaryFrom / 1000.0;
                                            }
                                            BigDecimal bd = new BigDecimal(basicSalaryFrom);
                                            basicSalaryFrom = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                                        } catch (Exception e) {
                                            LOG.info("compute basic_salary_from:{} error:{}", basicSalaryFromObj, e.getMessage());
                                            e.printStackTrace();
                                        }
                                        baseWorkSalary.setBasic_salary_from(basicSalaryFrom);

                                        //--basic_salary_to原始数据类型：1、整数OR浮点数 2、空("") 3、字符串：保密 4、其他
                                        Double basicSalaryTo = 0.0;
                                        Object basicSalaryToObj = workDetail.get("basic_salary_to");
                                        try {
                                            if (null != basicSalaryToObj && StringUtils.isNoneBlank(String.valueOf(basicSalaryToObj))) {
                                                String basicSalaryToStr = String.valueOf(basicSalaryToObj).trim();
                                                basicSalaryTo = Double.parseDouble(basicSalaryToStr);
                                            }
                                            if (basicSalaryTo >= 200) {
                                                basicSalaryTo = basicSalaryTo / 1000.0;
                                            }
                                            BigDecimal bd = new BigDecimal(basicSalaryTo);
                                            basicSalaryTo = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                                        } catch (Exception e) {
                                            LOG.info("compute basic_salary_to:{} error:{}", basicSalaryToObj, e.getMessage());
                                            e.printStackTrace();
                                        }
                                        baseWorkSalary.setBasic_salary_to(basicSalaryTo);

                                        //--annual_salary原始数据类型：1、整数OR浮点数 2、空("") 3、字符串：null 4、其他
                                        Double annualSalary = 0.0;
                                        Object annualSalaryObj = workDetail.get("annual_salary");
                                        try {
                                            if (null != annualSalaryObj && StringUtils.isNoneBlank(String.valueOf(annualSalaryObj))) {
                                                String annualSalaryStr = String.valueOf(annualSalaryObj).trim();
                                                annualSalary = Double.parseDouble(annualSalaryStr);
                                            }
                                            if (annualSalary >= 200 * 12) {
                                                annualSalary = annualSalary / 1000.0;
                                            }
                                            BigDecimal bd = new BigDecimal(annualSalary);
                                            annualSalary = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                                        } catch (Exception e) {
                                            LOG.info("compute annual_salary:{} error:{}", annualSalaryObj, e.getMessage());
                                            e.printStackTrace();
                                        }
                                        baseWorkSalary.setAnnual_salary(annualSalary);

                                        //--annual_salary_from原始数据类型：1、整数OR浮点数 2、空("") 3、带逗号的数字（1,000.00） 4、其他
                                        Double annualSalaryFrom = 0.0;
                                        Object annualSalaryFromObj = workDetail.get("annual_salary_from");
                                        try {
                                            if (null != annualSalaryFromObj && StringUtils.isNoneBlank(String.valueOf(annualSalaryFromObj))) {
                                                String annualSalaryFromStr = String.valueOf(annualSalaryFromObj).trim();
//                                                            Pattern.compile("^(\\d+|\\d+\\.\\d+)$");
                                                if (annualSalaryFromStr.contains(",")) {
                                                    annualSalaryFromStr = annualSalaryFromStr.replace(",", "");
                                                }
                                                annualSalaryFrom = Double.parseDouble(annualSalaryFromStr);
                                            }
                                            if (annualSalaryFrom >= 200 * 12) {
                                                annualSalaryFrom = annualSalaryFrom / 1000.0;
                                            }
                                            BigDecimal bd = new BigDecimal(annualSalaryFrom);
                                            annualSalaryFrom = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                                        } catch (Exception e) {
                                            LOG.info("compute annual_salary_from:{} error:{}", annualSalaryFromObj, e.getMessage());
                                            e.printStackTrace();
                                        }
                                        baseWorkSalary.setAnnual_salary_from(annualSalaryFrom);

                                        //--annual_salary_to原始数据类型：1、整数OR浮点数 2、空("") 3、带逗号的数字（1,000.00） 4、其他
                                        Double annualSalaryTo = 0.0;
                                        Object annualSalaryToObj = workDetail.get("annual_salary_to");
                                        try {
                                            if (null != annualSalaryToObj && StringUtils.isNoneBlank(String.valueOf(annualSalaryToObj))) {
                                                String annualSalaryToStr = String.valueOf(annualSalaryToObj).trim();
//                                                            Pattern.compile("^(\\d+|\\d+\\.\\d+)$");
                                                if (annualSalaryToStr.contains(",")) {
                                                    annualSalaryToStr = annualSalaryToStr.replace(",", "");
                                                }
                                                annualSalaryTo = Double.parseDouble(annualSalaryToStr);
                                            }
                                            if (annualSalaryTo >= 200 * 12) {
                                                annualSalaryTo = annualSalaryTo / 1000.0;
                                            }
                                            BigDecimal bd = new BigDecimal(annualSalaryTo);
                                            annualSalaryTo = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                                        } catch (Exception e) {
                                            LOG.info("compute annual_salary_to:{} error:{}", annualSalaryToObj, e.getMessage());
                                            e.printStackTrace();
                                        }
                                        baseWorkSalary.setAnnual_salary_to(annualSalaryTo);

                                        //expect_salary_from原始数据类型：1、整数OR浮点数 2、空("") 3、带逗号的数字（1,000.00） 4、其他
                                        Double expectSalaryFrom = 0.0;
                                        Object expectSalaryFromObj = workDetail.get("expect_salary_from");
                                        try {
                                            if (null != expectSalaryFromObj && StringUtils.isNoneBlank(String.valueOf(expectSalaryFromObj))) {
                                                String expectSalaryFromStr = String.valueOf(expectSalaryFromObj).trim();
//                                                            Pattern.compile("^(\\d+|\\d+\\.\\d+)$");
                                                if (expectSalaryFromStr.contains(",")) {
                                                    expectSalaryFromStr = expectSalaryFromStr.replace(",", "");
                                                }
                                                expectSalaryFrom = Double.parseDouble(expectSalaryFromStr);
                                            }
                                            if (expectSalaryFrom >= 200) {
                                                expectSalaryFrom = expectSalaryFrom / 1000.0;
                                            }
                                            BigDecimal bd = new BigDecimal(expectSalaryFrom);
                                            expectSalaryFrom = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                                        } catch (Exception e) {
                                            LOG.info("compute expect_salary_from:{} error:{}", expectSalaryFromObj, e.getMessage());
                                            e.printStackTrace();
                                        }
                                        baseWorkSalary.setExpect_salary_from(expectSalaryFrom);

                                        //expect_salary_to原始数据类型：1、整数OR浮点数 2、空("") 3、带逗号的数字（1,000.00） 4、其他
                                        Double expectSalaryTo = 0.0;
                                        Object expectSalaryToObj = workDetail.get("expect_salary_to");
                                        try {
                                            if (null != expectSalaryToObj && StringUtils.isNoneBlank(String.valueOf(expectSalaryToObj))) {
                                                String expectSalaryToStr = String.valueOf(expectSalaryToObj).trim();
//                                                            Pattern.compile("^(\\d+|\\d+\\.\\d+)$");
                                                if (expectSalaryToStr.contains(",")) {
                                                    expectSalaryToStr = expectSalaryToStr.replace(",", "");
                                                }
                                                expectSalaryTo = Double.parseDouble(expectSalaryToStr);
                                            }
                                            if (expectSalaryTo >= 200) {
                                                expectSalaryTo = expectSalaryTo / 1000.0;
                                            }
                                            BigDecimal bd = new BigDecimal(expectSalaryTo);
                                            expectSalaryTo = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                                        } catch (Exception e) {
                                            LOG.info("compute expect_salary_to:{} error:{}", expectSalaryToObj, e.getMessage());
                                            e.printStackTrace();
                                        }
                                        baseWorkSalary.setExpect_salary_to(expectSalaryTo);

                                        //--expect_annual_salary原始数据类型：1、整数OR浮点数 2、空("") 3、带逗号的数字（1,000.00） 4、其他
                                        Double expectAnnualSalary = 0.0;
                                        Object expectAnnualSalaryObj = workDetail.get("expect_annual_salary");
                                        try {
                                            if (null != expectAnnualSalaryObj && StringUtils.isNoneBlank(String.valueOf(expectAnnualSalaryObj))) {
                                                String expectAnnualSalaryStr = String.valueOf(expectAnnualSalaryObj).trim();
                                                if (expectAnnualSalaryStr.contains(",")) {
                                                    expectAnnualSalaryStr = expectAnnualSalaryStr.replace(",", "");
                                                }
                                                expectAnnualSalary = Double.parseDouble(expectAnnualSalaryStr);
                                            }
                                            if (expectAnnualSalary >= 200 * 12) {
                                                expectAnnualSalary = expectAnnualSalary / 1000.0;
                                            }
                                            BigDecimal bd = new BigDecimal(expectAnnualSalary);
                                            expectAnnualSalary = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                                        } catch (Exception e) {
                                            LOG.info("compute expect_annual_salary:{} error:{}", expectAnnualSalaryObj, e.getMessage());
                                            e.printStackTrace();
                                        }
                                        baseWorkSalary.setExpect_annual_salary(expectAnnualSalary);

                                        //--expect_annual_salary_from原始数据类型：1、整数OR浮点数 2、空("") 3、带逗号的数字（1,000.00） 4、其他
                                        Double expectAnnualSalaryFrom = 0.0;
                                        Object expectAnnualSalaryFromObj = workDetail.get("expect_annual_salary_from");
                                        try {
                                            if (null != expectAnnualSalaryFromObj && StringUtils.isNoneBlank(String.valueOf(expectAnnualSalaryFromObj))) {
                                                String expectAnnualSalaryFromStr = String.valueOf(expectAnnualSalaryFromObj).trim();
                                                if (expectAnnualSalaryFromStr.contains(",")) {
                                                    expectAnnualSalaryFromStr = expectAnnualSalaryFromStr.replace(",", "");
                                                }
                                                expectAnnualSalaryFrom = Double.parseDouble(expectAnnualSalaryFromStr);
                                            }
                                            if (expectAnnualSalaryFrom >= 200 * 12) {
                                                expectAnnualSalaryFrom = expectAnnualSalaryFrom / 1000.0;
                                            }
                                            BigDecimal bd = new BigDecimal(expectAnnualSalaryFrom);
                                            expectAnnualSalaryFrom = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                                        } catch (Exception e) {
                                            LOG.info("compute expect_annual_salary_from:{} error:{}", expectAnnualSalaryFromObj, e.getMessage());
                                            e.printStackTrace();
                                        }
                                        baseWorkSalary.setExpect_annual_salary_from(expectAnnualSalaryFrom);

                                        //--expect_annual_salary_to原始数据类型：1、整数OR浮点数 2、空("") 3、带逗号的数字（1,000.00） 4、其他
                                        Double expectAnnualSalaryTo = 0.0;
                                        Object expectAnnualSalaryToObj = workDetail.get("expect_annual_salary_to");
                                        try {
                                            if (null != expectAnnualSalaryToObj && StringUtils.isNoneBlank(String.valueOf(expectAnnualSalaryToObj))) {
                                                String expectAnnualSalaryToStr = String.valueOf(expectAnnualSalaryToObj).trim();
                                                if (expectAnnualSalaryToStr.contains(",")) {
                                                    expectAnnualSalaryToStr = expectAnnualSalaryToStr.replace(",", "");
                                                }
                                                expectAnnualSalaryTo = Double.parseDouble(expectAnnualSalaryToStr);
                                            }
                                            if (expectAnnualSalaryTo >= 200 * 12) {
                                                expectAnnualSalaryTo = expectAnnualSalaryTo / 1000.0;
                                            }
                                            BigDecimal bd = new BigDecimal(expectAnnualSalaryTo);
                                            expectAnnualSalaryTo = bd.setScale(1, RoundingMode.HALF_UP).doubleValue();
                                        } catch (Exception e) {
                                            LOG.info("compute expect_annual_salary_to:{} error:{}", expectAnnualSalaryToObj, e.getMessage());
                                            e.printStackTrace();
                                        }
                                        baseWorkSalary.setExpect_annual_salary_to(expectAnnualSalaryTo);
                                        //updated_at
                                        baseWorkSalary.setUpdated_at(updatedAt);
                                        baseWorkSalaries.add(baseWorkSalary);
                                    }
                                }
                            }
                            idwTobBaseData.setBaseWorkSalaries(baseWorkSalaries);
                        }

                        /*
                         * base_industry_temp
                         */
                        List<BaseIndustryTemp> baseIndustryTemps = new ArrayList<>();
                        if (StringUtils.isNoneBlank(cvTrade)) {
                            JSONArray array = null;
                            try {
                                array = JSONObject.parseArray(cvTrade);
                            } catch (Exception e) {
                                LOG.info("id:{} cvTrade parse json error:{}", resumeId, e.getMessage());
                            }
                            if (null != array && array.size() > 0) {
                                for (int i = 0; i < array.size(); i++) {
                                    JSONObject tradeJson = array.getJSONObject(i);
                                    if (null != tradeJson && tradeJson.size() > 0) {
                                        String wid = tradeJson.getString("work_id");
                                        List<Integer> tradeList = null != tradeJson.get("second_trade_list") ?
                                            (List<Integer>) tradeJson.get("second_trade_list") :
                                            (List<Integer>) tradeJson.get("first_trade_list");
                                        if (null != tradeList && tradeList.size() > 0) {
                                            for (int industryId : tradeList) {
                                                BaseIndustryTemp baseIndustryTemp = new BaseIndustryTemp();
                                                baseIndustryTemp.setResume_id(resumeId);
                                                baseIndustryTemp.setWid(wid);
                                                baseIndustryTemp.setUpdated_at(updatedAt);
                                                baseIndustryTemp.setIndustry_id(industryId);
                                                LOG.info("id:{}->wid:{}->industry_id:{}", resumeId, wid, industryId);
                                                baseIndustryTemps.add(baseIndustryTemp);
                                            }
                                        }
                                    }
                                }
                            }
                            idwTobBaseData.setBaseIndustryTemps(baseIndustryTemps);
                        }


                        /*
                         * base_level
                         */
                        List<IdwTobBaseLevel> baseLevels = new ArrayList<>();
                        if (StringUtils.isNoneBlank(cvTitle) && !StringUtils.equals("\"\"", cvTitle)) {
                            JSONObject cvTitleJson = null;
                            try {
                                cvTitleJson = JSONObject.parseObject(cvTitle);
                            } catch (Exception e) {
                                LOG.info("cv_title:{} cannot parse to json", cvTitle);
                            }
                            if (null != cvTitleJson && cvTitleJson.size() > 0) {
                                for (Map.Entry<String, Object> entry : cvTitleJson.entrySet()) {
                                    IdwTobBaseLevel baseLevel = new IdwTobBaseLevel();
                                    baseLevel.setResume_id(resumeId);
                                    baseLevel.setUpdated_at(algoUpdatedAt);
                                    String wid = entry.getKey();
                                    baseLevel.setWid(wid);
                                    Object value1 = entry.getValue();
                                    if (value1 instanceof Map) {
                                        Map<String, Object> value = (Map<String, Object>) value1;
                                        Integer level = 0;
                                        try {
                                            level = null != value.get("level") ? (Integer) value.get("level") : 0;
                                        } catch (Exception e) {
                                            LOG.error("id:{} cv_title level parse Integer error", resumeId);
                                        }
                                        baseLevel.setLevel(level);
                                    }
                                    baseLevels.add(baseLevel);
                                }
                            }
                            idwTobBaseData.setBaseLevels(baseLevels);
                        }
                    }
                }
                return idwTobBaseData;
            }, Encoders.bean(IdwTobBaseData.class));

        idwTobBaseDataDataset.persist();

        Dataset<IdwTobBaseCommon> baseCommonDs = idwTobBaseDataDataset
            .filter((FilterFunction<IdwTobBaseData>) ds -> null != ds.getBaseCommon())
            .map((MapFunction<IdwTobBaseData, IdwTobBaseCommon>) IdwTobBaseData::getBaseCommon, Encoders.bean(IdwTobBaseCommon.class));

//        Dataset<IdwTobBaseContact> baseContactDs = idwTobBaseDataDataset
//            .filter((FilterFunction<IdwTobBaseData>) ds -> null != ds.getBaseContact())
//            .map((MapFunction<IdwTobBaseData, IdwTobBaseContact>) IdwTobBaseData::getBaseContact, Encoders.bean(IdwTobBaseContact.class));

        Dataset<IdwTobBaseEducation> baseEducationDs = idwTobBaseDataDataset
            .filter((FilterFunction<IdwTobBaseData>) odsBaseData -> null != odsBaseData.getBaseEducations() && odsBaseData.getBaseEducations().size() > 0)
            .flatMap((FlatMapFunction<IdwTobBaseData, IdwTobBaseEducation>) odsBaseData -> {
                List<IdwTobBaseEducation> baseEducations = odsBaseData.getBaseEducations();
                return baseEducations.iterator();
            }, Encoders.bean(IdwTobBaseEducation.class));

        Dataset<IdwTobBaseFunction> baseFunctionDs = idwTobBaseDataDataset
            .filter((FilterFunction<IdwTobBaseData>) odsBaseData -> null != odsBaseData.getBaseFunctions() && odsBaseData.getBaseFunctions().size() > 0)
            .flatMap((FlatMapFunction<IdwTobBaseData, IdwTobBaseFunction>) odsBaseData -> {
                List<IdwTobBaseFunction> baseFunctions = odsBaseData.getBaseFunctions();
                return baseFunctions.iterator();
            }, Encoders.bean(IdwTobBaseFunction.class));


        Dataset<IdwTobBaseCompany> baseCompanyDs = idwTobBaseDataDataset
            .filter((FilterFunction<IdwTobBaseData>) odsBaseData -> null != odsBaseData.getBaseCompanys() && odsBaseData.getBaseCompanys().size() > 0)
            .flatMap((FlatMapFunction<IdwTobBaseData, IdwTobBaseCompany>) odsBaseData -> {
                List<IdwTobBaseCompany> baseCompanies = odsBaseData.getBaseCompanys();
                return baseCompanies.iterator();
            }, Encoders.bean(IdwTobBaseCompany.class));


        Dataset<IdwTobBaseSalary> baseSalaryDs = idwTobBaseDataDataset
            .filter((FilterFunction<IdwTobBaseData>) odsBaseData -> null != odsBaseData.getBaseSalary())
            .map((MapFunction<IdwTobBaseData, IdwTobBaseSalary>) IdwTobBaseData::getBaseSalary, Encoders.bean(IdwTobBaseSalary.class));

        Dataset<IdwTobBaseWork> baseWorkDs = idwTobBaseDataDataset
            .filter((FilterFunction<IdwTobBaseData>) odsBaseData -> null != odsBaseData.getBaseWorks() && odsBaseData.getBaseWorks().size() > 0)
            .flatMap((FlatMapFunction<IdwTobBaseData, IdwTobBaseWork>) odsBaseData -> {
                List<IdwTobBaseWork> baseWorks = odsBaseData.getBaseWorks();
                return baseWorks.iterator();
            }, Encoders.bean(IdwTobBaseWork.class));

        Dataset<IdwTobBaseWorkSalary> baseWorkSalaryDs = idwTobBaseDataDataset
            .filter((FilterFunction<IdwTobBaseData>) odsBaseData -> null != odsBaseData.getBaseWorkSalaries() && odsBaseData.getBaseWorkSalaries().size() > 0)
            .flatMap((FlatMapFunction<IdwTobBaseData, IdwTobBaseWorkSalary>) odsBaseData -> {
                List<IdwTobBaseWorkSalary> baseWorkSalaries = odsBaseData.getBaseWorkSalaries();
                return baseWorkSalaries.iterator();
            }, Encoders.bean(IdwTobBaseWorkSalary.class));

        Dataset<IdwTobBaseLevel> baseLevelDs = idwTobBaseDataDataset
            .filter((FilterFunction<IdwTobBaseData>) odsBaseData -> null != odsBaseData.getBaseLevels() && odsBaseData.getBaseLevels().size() > 0)
            .flatMap((FlatMapFunction<IdwTobBaseData, IdwTobBaseLevel>) odsBaseData -> {
                List<IdwTobBaseLevel> baseLevels = odsBaseData.getBaseLevels();
                return baseLevels.iterator();
            }, Encoders.bean(IdwTobBaseLevel.class));

        Dataset<BaseIndustryTemp> baseIndustryTempDs = idwTobBaseDataDataset
            .filter((FilterFunction<IdwTobBaseData>) odsBaseData -> null != odsBaseData.getBaseIndustryTemps() && odsBaseData.getBaseIndustryTemps().size() > 0)
            .flatMap((FlatMapFunction<IdwTobBaseData, BaseIndustryTemp>) odsBaseData -> {
                List<BaseIndustryTemp> baseIndustryTemps = odsBaseData.getBaseIndustryTemps();
                return baseIndustryTemps.iterator();
            }, Encoders.bean(BaseIndustryTemp.class));

        //save data to hive
        baseCommonDs.write().mode(SaveMode.Overwrite).saveAsTable("idw_tob_resume.base_common");

//        baseContactDs.write().mode(SaveMode.Overwrite).saveAsTable("idw_tob_resume.base_contact");

        baseCompanyDs.write().mode(SaveMode.Overwrite).saveAsTable("idw_tob_resume.base_company");

        baseEducationDs.write().mode(SaveMode.Overwrite).saveAsTable("idw_tob_resume.base_education");

        baseFunctionDs.write().mode(SaveMode.Overwrite).saveAsTable("idw_tob_resume.base_function");

        baseSalaryDs.write().mode(SaveMode.Overwrite).saveAsTable("idw_tob_resume.base_salary");

        baseWorkDs.write().mode(SaveMode.Overwrite).saveAsTable("idw_tob_resume.base_work");

        baseWorkSalaryDs.write().mode(SaveMode.Overwrite).saveAsTable("idw_tob_resume.base_work_salary");

        baseLevelDs.write().mode(SaveMode.Overwrite).saveAsTable("idw_tob_resume.base_level");


        baseIndustryTempDs.join(industryMappingDs,
            baseIndustryTempDs.col("industry_id").equalTo(industryMappingDs.col("id")), "left")
            .select(baseIndustryTempDs.col("resume_id"),
                baseIndustryTempDs.col("wid"),
                industryMappingDs.col("pindustry_id"),
                industryMappingDs.col("depth"),
                baseIndustryTempDs.col("updated_at"))
            .write().mode(SaveMode.Overwrite).saveAsTable("idw_tob_resume.base_industry");


        idwTobBaseDataDataset.unpersist();

//        //base_common
//        odsBaseDataDs.selectExpr("resume_id", "name", "gender", "birth",
//            "marital", "account_province", "account_city", "address_province",
//            "address", "current_status", "management_experience", "degree",
//            "work_experience", "quality", "resume_updated_at", "updated_at")
//            .distinct().write().mode(SaveMode.Overwrite).saveAsTable("edw_ods_resumes.base_common");
//
//
//        //base_education
//        odsBaseDataDs.selectExpr("resume_id", "eid", "sort_id", "start_time", "end_time",
//            "so_far", "school_id", "major_id", "edu_degree as degree", "reindex_degree",
//            "updated_at", "school_name", "major_name").where("eid is not null")
//            .distinct().write().mode(SaveMode.Overwrite).saveAsTable("edw_ods_resumes.base_education");
//
//        //base_function
//        odsBaseDataDs.selectExpr("resume_id", "wid", "function_id", "value", "algo_updated_at as updated_at")
//            .distinct().write().mode(SaveMode.Overwrite).saveAsTable("edw_ods_resumes.base_function");
//
//        //base_company
//        odsBaseDataDs.selectExpr("resume_id", "wid", "company_id", "algo_updated_at as updated_at")
//            .distinct().write().mode(SaveMode.Overwrite).saveAsTable("edw_ods_resumes.base_company");

    }

}
