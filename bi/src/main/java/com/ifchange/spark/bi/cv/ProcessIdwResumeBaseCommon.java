package com.ifchange.spark.bi.cv;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.bi.bean.cv.Algorithms;
import com.ifchange.spark.bi.bean.cv.BaseCommon;
import com.ifchange.spark.bi.bean.cv.ResumeExtra;
import com.ifchange.spark.util.MyString;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

/**
 * idw_resume 数据处理
 * base_common
 */
public class ProcessIdwResumeBaseCommon {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwResumeBaseCommon.class);

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
                    /**
                     * algorithms \t 4
                     */
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
                        String cvDegree = jsonObject.getString("cv_degree");
                        algorithms.setCvDegree(cvDegree);
                        String cvWorkyear = jsonObject.getString("cv_workyear");
                        algorithms.setCvWorkyear(cvWorkyear);
                        String cvQuality = jsonObject.getString("cv_quality");
                        algorithms.setCvQuality(cvQuality);
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

        //resume_extra left join algorithms
        Dataset<BaseCommon> baseCommonDataset = resumeExtraDataset
            .join(algorithmsDataset, resumeExtraDataset.col("id").equalTo(algorithmsDataset.col("id")), "left")
            .select(resumeExtraDataset.col("id"),
                resumeExtraDataset.col("compress"),
                resumeExtraDataset.col("updatedAt"),
                algorithmsDataset.col("cvDegree"),
                algorithmsDataset.col("cvWorkyear"),
                algorithmsDataset.col("cvQuality"),
                algorithmsDataset.col("updatedAt"))
            .map((MapFunction<Row, BaseCommon>) row -> {
                Long resumeId = null != row.get(0) ? row.getLong(0) : 0L;
                String compressStr = null != row.get(1) ? row.getString(1) : "";
                String updatedAt = null != row.get(2) ? row.getString(2) : "";
                String cvDegree = null != row.get(3) ? row.getString(3) : "";
                String cvWorkyear = null != row.get(4) ? row.getString(4) : "";
                String cvQuality = null != row.get(5) ? row.getString(5) : "";
                BaseCommon baseCommon = null;
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
                        JSONObject basic = null;
                        try {
                            basic = jsonObject.getJSONObject("basic");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        if (null != basic) {
                            baseCommon = new BaseCommon();
                            baseCommon.setResume_id(resumeId);
                            String name = null != basic.get("name") ? basic.getString("name") : "";
                            baseCommon.setName(name);
                            String genderStr = null != basic.get("gender") ? basic.getString("gender") : "";
                            int gender;
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
                            try {
                                quality = Double.parseDouble(cvQuality);
                            } catch (Exception e) {
                                LOG.info("cvQuality:{} parse to double error:{}", cvWorkyear, e.getMessage());
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
                        }
                    }
                }
                return baseCommon;
            }, Encoders.bean(BaseCommon.class))
            .filter((FilterFunction<BaseCommon>) Objects::nonNull);

        //save data to hive
        baseCommonDataset.write().mode(SaveMode.Overwrite).saveAsTable("idw_resume.base_common");

    }

}
