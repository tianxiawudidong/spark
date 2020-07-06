package com.ifchange.spark.util;


import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;

public class SortMapUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SortMapUtils.class);

    /**
     * 工作经历排序
     * 当前简历解析结果中工作经历顺序为源文本中填写顺序，可能存在时间上的乱序影响bi、展示等。
     * 需求在简历入库前对工作经历进行排序，按时间顺序由新到旧。
     * 排序规则：
     * 1、全部工作经历中，有任意一段无begin_time，不做重排；
     * 2、全部工作经历中，有任意一段无end_time且so_far不是Y，不做重排；
     * 3、全部工作经历按开始时间由新到旧排序；
     * 4、开始时间相同的，按原相对顺序排序；
     * 5、开始时间需要注意9月和09月，即1位数字和2位数字的比较；
     * 补充一个异常case，时间可能有的有月份 有的没有。
     * 比较策略统一按先比年份大小，后比月份大小，如果有缺失，按原相对顺序。
     * 即如果原顺序中是2014年和2014年9月两种开始时间比较，那么原来哪段在先的就保持在先
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> workSort(Map<String, Object> workMap) {
        Map<String, Object> newWork = new LinkedHashMap<>();
        if (null != workMap && workMap.size() > 0) {
            for (Entry<String, Object> entry : workMap.entrySet()) {
                String workId = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof Map) {
                    Map<String, Object> workDetail = (Map<String, Object>) value;
                    String startTime = null != workDetail.get("start_time")
                        ? String.valueOf(workDetail.get("start_time")) : "";
                    String endTime = null != workDetail.get("end_time")
                        ? String.valueOf(workDetail.get("end_time")) : "";
                    String soFar = null != workDetail.get("so_far")
                        ? String.valueOf(workDetail.get("so_far")) : "N";
                    //全部工作经历中，有任意一段无begin_time，不做重排；
                    if (StringUtils.isBlank(startTime)) {
                        LOG.info("workId:{} start time is empty", workId);
                        return workMap;
                    }
                    //全部工作经历中，有任意一段无end_time且so_far不是Y，不做重排；
                    if (StringUtils.isBlank(endTime) && !soFar.equals("Y")) {
                        LOG.info("workId:{} end_time is empty and so_far is not Y", workId);
                        return workMap;
                    }
                    //如果格式不满足 年 月  不做重排
//                if (!startTime.contains("年") || startTime.length() < 5) {
//                    break;
//                }
                } else {
                    return workMap;
                }

            }

            List<Entry<String, Object>> workList = new ArrayList<>(workMap.entrySet());
            //开始排序 全部工作经历按开始时间由新到旧排序；

            workList.sort((entry1, entry2) -> {
                int result;
                Map<String, Object> work1 = (Map<String, Object>) entry1.getValue();
                Map<String, Object> work2 = (Map<String, Object>) entry2.getValue();
                String startTime1 = String.valueOf(work1.get("start_time"));
                String startTime2 = String.valueOf(work2.get("start_time"));
                int year1 = 0;
                int year2 = 0;
                String yearStr1 = DateTimeUtil.extractDate(startTime1, "Y");
                String yearStr2 = DateTimeUtil.extractDate(startTime2, "Y");
                try {
                    year1 = Integer.parseInt(yearStr1);
                } catch (Exception e) {
                    LOG.info("cannot parse work.start_time:{} year ,error:{}", startTime1, e.getMessage());
                }
                try {
                    year2 = Integer.parseInt(yearStr2);
                } catch (Exception e) {
                    LOG.info("cannot parse work.start_time:{} year,error:{}", startTime2, e.getMessage());
                }

                if (year1 > year2) {
                    result = -1;
                } else if (year1 < year2) {
                    result = 1;
                } else {
                    int month1 = 0;
                    int month2 = 0;
                    String monthStr1 = DateTimeUtil.extractDate(startTime1, "m");
                    String monthStr2 = DateTimeUtil.extractDate(startTime2, "m");
                    try {
                        month1 = Integer.parseInt(monthStr1);
                    } catch (Exception e) {
                        LOG.info("cannot parse work.start_time:{},error:{}", startTime1, e.getMessage());
                    }
                    try {
                        month2 = Integer.parseInt(monthStr2);
                    } catch (Exception e) {
                        LOG.info("cannot parse work.start_time:{},error:{}", startTime2, e.getMessage());
                    }
                    result = Integer.compare(month2, month1);
                }
                return result;
            });


            int i = 1;
            for (Entry<String, Object> tmpEntry : workList) {
                if (i > 20) {
                    LOG.info("工作经历段数:{}超过限制20", i);
                    continue;
                }
                String workId = tmpEntry.getKey();
                Map<String, Object> value = (Map<String, Object>) tmpEntry.getValue();
                value.put("sort_id", i);
                i++;
                newWork.put(workId, value);
            }
        }

        return newWork;
    }


    /**
     * 教育经历排序
     * 先比较开始时间年月
     * 如果相同比较结束时间年月
     * 如果结束时间年月相同 比较学校名称
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> educationSort(Map<String, Object> eduMap) {
        Map<String, Object> newEduMap = new LinkedHashMap<>();
        if (null != eduMap && eduMap.size() > 0) {
            List<Entry<String, Object>> eduList = new ArrayList<>(eduMap.entrySet());
            eduList.sort((entry1, entry2) -> {
                int result;
                Object value1 = entry1.getValue();
                Object value2 = entry2.getValue();
                if (value1 instanceof Map && value2 instanceof Map) {
                    Map<String, Object> education1 = (Map<String, Object>) value1;
                    Map<String, Object> education2 = (Map<String, Object>) value2;
                    String startTime1 = null != education1.get("start_time")
                        ? String.valueOf(education1.get("start_time")) : "";
                    String startTime2 = null != education2.get("start_time")
                        ? String.valueOf(education2.get("start_time")) : "";
                    int yearMonth1 = 0;
                    int yearMonth2 = 0;
                    if (StringUtils.isBlank(startTime1) && StringUtils.isBlank(startTime2)) {
                        return 0;
                    } else if (StringUtils.isNoneBlank(startTime1) && StringUtils.isBlank(startTime2)) {
                        return 1;
                    } else if (StringUtils.isBlank(startTime1) && StringUtils.isNoneBlank(startTime2)) {
                        return -1;
                    } else {
                        String year1 = DateTimeUtil.extractDate(startTime1, "Y");
                        String year2 = DateTimeUtil.extractDate(startTime2, "Y");
                        String month1 = DateTimeUtil.extractDate(startTime1, "m");
                        String month2 = DateTimeUtil.extractDate(startTime2, "m");
                        try {
                            yearMonth1 = Integer.parseInt(year1 + month1);
                        } catch (Exception e) {
                            LOG.info("cannot parse education.start_time:{},error:{}", startTime1, e.getMessage());
                        }
                        try {
                            yearMonth2 = Integer.parseInt(year2 + month2);
                        } catch (Exception e) {
                            LOG.info("cannot parse education.start_time:{},error:{}", startTime2, e.getMessage());
                        }
                    }

                    if (yearMonth1 > yearMonth2) {
                        result = -1;
                    } else if (yearMonth1 < yearMonth2) {
                        result = 1;
                    } else {//教育经历开始时间相同
                        //比较结束时间
                        String endTime1 = null != education1.get("end_time")
                            ? String.valueOf(education1.get("end_time")) : "";
                        String endTime2 = null != education2.get("end_time")
                            ? String.valueOf(education2.get("end_time")) : "";
                        int endYearMonth1 = 0;
                        int endYearMonth2 = 0;
                        if (StringUtils.isBlank(endTime1) && StringUtils.isBlank(endTime2)) {
                            return 0;
                        } else if (StringUtils.isNoneBlank(endTime1) && StringUtils.isBlank(endTime2)) {
                            return 1;
                        } else if (StringUtils.isBlank(endTime1) && StringUtils.isNoneBlank(endTime2)) {
                            return -1;
                        } else {
                            String year1 = DateTimeUtil.extractDate(endTime1, "Y");
                            String year2 = DateTimeUtil.extractDate(endTime2, "Y");
                            String month1 = DateTimeUtil.extractDate(endTime1, "m");
                            String month2 = DateTimeUtil.extractDate(endTime2, "m");
                            try {
                                endYearMonth1 = Integer.parseInt(year1 + month1);
                            } catch (Exception e) {
                                LOG.info("cannot parse education.end_time:{},error:{}", endTime1, e.getMessage());
                            }
                            try {
                                endYearMonth2 = Integer.parseInt(year2 + month2);
                            } catch (Exception e) {
                                LOG.info("cannot parse education.end_time:{},error:{}", endTime2, e.getMessage());
                            }
                        }

                        if (endYearMonth1 > endYearMonth2) {
                            result = -1;
                        } else if (endYearMonth1 < endYearMonth2) {
                            result = 1;
                        } else { //结束时间相同
                            //比较学校名称
                            String schoolName1 = null != education1.get("school_name")
                                ? String.valueOf(education1.get("school_name")) : "";
                            String schoolName2 = null != education2.get("school_name")
                                ? String.valueOf(education2.get("school_name")) : "";
                            result = schoolName1.compareTo(schoolName2);
                        }
                    }
                } else {
                    result = 0;
                }
                return result;
            });

            int i = 1;
            for (Entry<String, Object> tmpEntry : eduList) {
                if (i > 20) {
                    LOG.info("教育经历段数:{}超过限制20", i);
                    continue;
                }
                String eduId = tmpEntry.getKey();
                Object value1 = tmpEntry.getValue();
                if(value1 instanceof Map){
                    Map<String, Object> value = (Map<String, Object>) value1;
                    value.put("sort_id", i);
                    i++;
                    newEduMap.put(eduId, value);
                }
            }
        }

        return newEduMap;
    }


}
