package com.ifchange.spark.util;

import java.io.Serializable;

public class ResumeUtil implements Serializable {

    private static final double RECORD_NUM = 5000000;


    public static String getIcdcDBNameById(long resumeId) {
        String suffix = "icdc_";
        Double v = (resumeId % 8) + Math.floor(resumeId / 40000000) * 8;
        suffix = suffix + v.intValue();
        return suffix;
    }

    /**
     * 根据简历id计算出在哪个db
     *
     * @param resumeId 简历id
     * @return db
     */
    public static String getDBNameById(long resumeId) {
        String suffix = "icdc_";
//        Double v = (resumeId % 8) + Math.floor(resumeId / 40000000) * 8;
//        suffix = suffix + v.intValue();
//        String environment = PropertyUtils.getProperty("runtime.environment");
//        if (!environment.equals("production")) {
//            Double floor = Math.floor(resumeId / RECORDNUM);
//            suffix = suffix + floor.intValue();
//        } else {
//            Double v = (resumeId % 8) + Math.floor(resumeId / 40000000) * 8;
//            suffix = suffix + v.intValue();
//        }
        long l = resumeId % 8;
        suffix = suffix + l;
        return suffix;
    }

    /**
     * positionId入库逻辑
     *
     * @param positionId 职位id
     * @return db
     */
    public static String getPositionDBNameByPid(long positionId) {
        String suffix = "position_";
        Double v = (positionId % 8) + Math.floor(positionId / 40000000) * 8;
        suffix = suffix + v.intValue();
        return suffix;
    }

    public static String getDBNameById2(long resumeId) {
        String suffix = "icdc_";
        Double v = (resumeId % 8) + Math.floor(resumeId / 40000000) * 8;
        suffix = suffix + v.intValue();
        return suffix;
    }


    /**
     * 根据tob简历id计算出相应的数据库
     *
     * @param tobResumeId tob简历id
     * @return dbName
     */
    public static String getTobDBName(String tobResumeId) {
        String index = tobResumeId.substring(tobResumeId.length() - 2, tobResumeId.length() - 1);
        return "tob_resume_pool_" + index;
    }

    /**
     * 根据tob简历id计算出相应的算法数据库
     * 倒数第2位是库标示 倒数第1位是表标示
     *
     * @param tobResumeId tob简历id
     * @return dbName
     */
    public static String getTobAlgorithmsDBName(String tobResumeId) {
        String index = tobResumeId.substring(tobResumeId.length() - 2, tobResumeId.length() - 1);
        return "tob_algorithms_pool_" + index;
    }

    /**
     * 根据tob简历id计算出相应的表
     * 倒数第2位是库标示 倒数第1位是表标示
     *
     * @param tobResumeId tob简历id
     * @return dbName
     */
    public static String getTobAlgorithmsTableName(String tobResumeId) {
        String prefix = "algorithms_";
        String suffix = tobResumeId.substring(tobResumeId.length() - 1);
        return prefix + suffix;
    }

    /**
     * 根据tob简历id计算出相应的表
     * 倒数第2位是库标示 倒数第1位是表标示
     *
     * @param tobResumeId tob简历id
     * @return dbName
     */
    public static String getTobResumesUpdateTableName(String tobResumeId) {
        String prefix = "resumes_update_";
        String suffix = tobResumeId.substring(tobResumeId.length() - 1);
        return prefix + suffix;
    }

    public static String getTobResumesExtraTableName(String tobResumeId) {
        String prefix = "resume_extra_";
        String suffix = tobResumeId.substring(tobResumeId.length() - 1);
        return prefix + suffix;
    }


    /**
     * 根据tob简历id计算出相应的表
     * 倒数第2位是库标示 倒数第1位是表标示
     *
     * @param tobResumeId tob简历id
     * @return dbName
     */
    public static String getTobDetailTableName(String tobResumeId) {
        String prefix = "resume_detail_";
        String suffix = tobResumeId.substring(tobResumeId.length() - 1);
        return prefix + suffix;
    }

    public static String getTobTagTableName(String tobResumeId) {
        String prefix = "resume_tag_";
        String suffix = tobResumeId.substring(tobResumeId.length() - 1);
        return prefix + suffix;
    }

    /**
     * 根据tob简历id计算出相应的表
     * 倒数第2位是库标示 倒数第1位是表标示
     *
     * @param tobResumeId tob简历id
     * @return dbName
     */
    public static String getTobResumeTableName(String tobResumeId) {
        String prefix = "resume_";
        String suffix = tobResumeId.substring(tobResumeId.length() - 1, tobResumeId.length());
        return prefix + suffix;
    }


    public static void main(String[] args) {
        long resumeId = 11804223;

        String db = getIcdcDBNameById(resumeId);
        System.out.println(db);
    }

}
