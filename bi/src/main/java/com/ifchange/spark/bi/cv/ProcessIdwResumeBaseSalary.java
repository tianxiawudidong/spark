package com.ifchange.spark.bi.cv;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.bi.bean.cv.BaseSalary;
import com.ifchange.spark.bi.bean.cv.ResumeExtra;
import com.ifchange.spark.util.MyString;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

/**
 * idw_resume 数据处理
 * base_salary
 */
public class ProcessIdwResumeBaseSalary {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwResumeBaseSalary.class);

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String extraPath = args[2];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        Dataset<BaseSalary> baseSalaryDataset = sparkSession.read()
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
            .map((MapFunction<ResumeExtra, BaseSalary>) resumeExtra -> {
                BaseSalary baseSalary = new BaseSalary();
                Long resumeId = resumeExtra.getId();
                String updatedAt = resumeExtra.getUpdatedAt();
                String compressStr = resumeExtra.getCompress();
                if (StringUtils.isNoneBlank(compressStr)) {
                    String compress = "";
                    try {
                        compress = MyString.unzipString(MyString.hexStringToBytes(compressStr));
                    } catch (Exception e) {
                        LOG.info("id:{} unzip compress error:{}", resumeId, e.getMessage());
                        e.printStackTrace();
                    }

                    JSONObject jsonObject = null;
                    try {
                        jsonObject = JSONObject.parseObject(compress);
                    } catch (Exception e) {
                        LOG.info("id:{} compress cannot parse to json,msg:{}", resumeId, e.getMessage());
                        e.printStackTrace();
                    }
                    if (null != jsonObject) {
                        JSONObject basic = null;
                        try {
                            basic = jsonObject.getJSONObject("basic");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        if (null != basic) {
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
                            Double basicSalaryFrom =0.0;
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
                            Double expectSalaryFrom =0.0;
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
                        }
                    }
                }

                return baseSalary;
            }, Encoders.bean(BaseSalary.class))
            .filter((FilterFunction<BaseSalary>) Objects::nonNull);


        baseSalaryDataset.write().mode(SaveMode.Overwrite).saveAsTable("idw_resume.base_salary");

    }

}
