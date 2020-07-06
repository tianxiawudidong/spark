package com.ifchange.spark.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;

public class DateTimeUtil {

    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final Logger LOG = LoggerFactory.getLogger(DateTimeUtil.class);

    /**
     * use java8 time
     * 返回毫秒
     *
     * @param timeStr time
     * @return 毫秒
     * @throws Exception
     */
    public static long parseStrToLongMill(String timeStr) throws Exception {
        try {
            LocalDateTime localDateTime = LocalDateTime.parse(timeStr, dtf);
            return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        } catch (Exception e) {
            throw new Exception("time parse error" + e.getMessage());
        }
    }

    /**
     * use java8 time
     * 返回秒
     *
     * @param timeStr time
     * @return 秒
     * @throws Exception
     */
    public static long parseStrToLongSec(String timeStr) throws Exception {
        try {
            LocalDateTime localDateTime = LocalDateTime.parse(timeStr, dtf);
            return localDateTime.toEpochSecond(ZoneOffset.of("+8"));
        } catch (Exception e) {
            throw new Exception("time parse error" + e.getMessage());
        }
    }

    /**
     * LocalDate转Date
     * LocalDateTime转Date
     */
    public static Date parseLocalDateToDate(LocalDate localDate) {
        return Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
        //return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    /**
     * Date转LocalDateTime(LocalDate)
     */
    public static LocalDate parseLocalDateTimeToDate(Date date) {
//        LocalDateTime localDateTime = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        return localDate;
    }

    /**
     * LocalDate转时间戳
     * LocalDateTime转时间戳
     */
    public static long parseLocalDateToTimeStamp(LocalDate localDate) {
//        LocalDate localDate = LocalDate.now();
        long timestamp = localDate.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();

//        LocalDateTime localDateTime = LocalDateTime.now();
//        long timestamp2 = localDateTime.toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
        return timestamp;
    }

    /**
     * 时间戳转LocalDate
     */
    public static LocalDate parseTimeStampToLocalDate(long timestamp) {
        LocalDate localDate = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDate();
        LocalDateTime localDateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime();
        return localDate;
    }

    public static String formatLongTimeToStandard(long timestamp) {
        LocalDateTime localDateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime();
        String format = dtf.format(localDateTime);
        return format;
    }

    /**
     * 从一段时间里提取出年 或月
     */
    public static String extractDate(String time, String format) {
        String result = "";
        if (StringUtils.isNoneBlank(time)) {
            switch (format) {
                case "Y":
                    try {
                        if (time.contains("年")) {
                            result = time.substring(0, time.indexOf("年"));
                        } else if (time.contains("-")) {
                            result = time.split("-")[0].trim();
                        } else if (time.length() >= 6) {
                            result = time.substring(0, 4).trim();
                        } else if (time.contains(".")) {
                            result = time.split(".")[0];
                        } else {
                            LOG.info("cannot extract year from time:{}", time);
                        }
                    } catch (Exception e) {
                        LOG.info("extract year from :{} error:{}", time, e.getMessage());
                    }
                    break;
                case "m":
                    try {
                        if (time.contains("年") && time.contains("月")) {
                            result = time.substring(time.indexOf("年") + 1, time.indexOf("月"));
                        } else if (time.contains("年") && !time.contains("月")) {
                            int length = time.length();
                            result = time.substring(time.indexOf("年") + 1, length);
                        } else if (!time.contains("年") && time.contains("月")) {
                            int index = time.indexOf("月");
                            if (index >= 4) {
                                if (index == 6) {
                                    result = time.substring(4, 6);
                                }
                                if (index == 5) {
                                    result = time.substring(4, 5);
                                }
                                if (index == 4) {
                                    result = Integer.parseInt(time.substring(2, 4)) > 12 ?
                                        time.substring(3, 4) : time.substring(2, 4);
                                }
                            } else {
                                LOG.info("can not extract month:{}", time);
                            }
                        } else if (time.contains("-")) {
                            result = time.split("-")[1].trim();
                        } else if (time.length() >= 6) {
                            result = time.substring(4, 6).trim();
                        } else if (time.contains(".")) {
                            result = time.split(".")[1];
                        } else {
                            LOG.info("can not extract month:{}", time);
                        }
                    } catch (Exception e) {
                        LOG.info("extract month from :{} error:{}", time, e.getMessage());
                    }
                    if (StringUtils.isNoneBlank(result) && result.length() < 2) {
                        result = "0" + result;
                    }
                    break;
                default:
                    break;
            }
        }
        return result;
    }

    public static int computeWorkTime(String startTime, String endTime) {
        int result = 0;
        endTime = StringUtils.isBlank(endTime) ? dtf.format(LocalDateTime.now()) : endTime;
        try {
            int startYear = Integer.parseInt(extractDate(startTime, "Y"));
            int startMonth = Integer.parseInt(extractDate(startTime, "m"));
            int endYear = Integer.parseInt(extractDate(endTime, "Y"));
            int endMonth = Integer.parseInt(extractDate(endTime, "m"));
            result = (endYear - startYear) * 12 + (endMonth - startMonth);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 计算工作经历中 第一份工作经历的开始时间 yyyy年MM月
     */
    public static String getFirstWorkTime(Map<String, Object> work) {
        String result = "";
        if (null != work && work.size() > 0) {
            int size = work.size();
            for (Map.Entry<String, Object> entry : work.entrySet()) {
                Object value = entry.getValue();
                if (value instanceof Map) {
                    Map<String, Object> workDetail = (Map<String, Object>) value;
                    int sortId = 0;
                    Object sortId1 = workDetail.get("sort_id");
                    if (null != sortId1) {
                        String s = String.valueOf(sortId1);
                        if (StringUtils.isNoneBlank(s) && !StringUtils.equals("null", s)) {
                            try {
                                sortId = Integer.parseInt(s);
                            } catch (NumberFormatException e) {
                                LOG.info("sort_id:{} parse int error", sortId1);
                            }
                        }
                    }
                    //第一次工作经历
                    if (sortId == size) {
                        result = String.valueOf(workDetail.get("start_time"));
                    }
                }
            }
        }
        return result;
    }

    /**
     * 计算工作经历 总工作时长
     */
    public static int computeTotalWorkTime(Map<String, Object> work) {
        int totalWorkTime = 0;
        if (null != work && work.size() > 0) {
            for (Map.Entry<String, Object> entry : work.entrySet()) {
                String workId = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof Map) {
                    Map<String, Object> workDetail = (Map<String, Object>) value;
                    String startTime = String.valueOf(workDetail.get("start_time"));
                    String endTime = String.valueOf(workDetail.get("end_time"));
                    int workTime = computeWorkTime(startTime, endTime);
                    LOG.info("work_id:{},start:{},end:{} workTime:{}", workId, startTime, endTime, workTime);
                    totalWorkTime += workTime;
                }
            }
        }

        return totalWorkTime;
    }

    public static boolean compareTime(String time1, String time2) {
        boolean flag;
        if (StringUtils.isBlank(time1) && StringUtils.isNoneBlank(time2)) {
            flag = true;
        } else if (StringUtils.isNoneBlank(time1) && StringUtils.isBlank(time2)) {
            flag = false;
        } else if (StringUtils.isNoneBlank(time1) && StringUtils.isNoneBlank(time2)) {
            Timestamp timestamp1 = Timestamp.valueOf(time1);
            Timestamp timestamp2 = Timestamp.valueOf(time2);
            flag = timestamp1.before(timestamp2);
        } else {
            flag = false;
        }
        return flag;
    }


    public static void main(String[] args) throws Exception {
        boolean b = DateTimeUtil.compareTime("2019-10-23 15:30:33", "2019-10-21 12:21:21");
        System.out.println(b);
    }
}
