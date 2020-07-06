package com.ifchange.spark.algorithms.http;

import com.ifchange.spark.util.ExecutorFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class CvCurrentStatus implements Callable<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(CvCurrentStatus.class);

    private static ThreadPoolExecutor threadPoolExecutor;

    private static final int ThreadNumber = 2;

    private String resumeId;

    private Map<String, Object> compress;

    public CvCurrentStatus(String resumeId, Map<String, Object> compress) throws Exception {
        if (null == threadPoolExecutor) {
            throw new Exception("thread pool not init,please call init method");
        }
        if (null == compress || compress.isEmpty()) {
            throw new Exception("compress is null or empty,please check");
        }
        this.resumeId = resumeId;
        this.compress = compress;
    }

    @Override
    public Integer call() {
        int result = 0;
        /*
         * 1）如果basic/current_status=1且最后一份工作end_time值不为空，cv_current_status=1
         * 2）否则cv_current_status=0
         */
        Map<String, Object> basicMap = null;
        Object basic = compress.get("basic");
        if (basic instanceof Map) {
            basicMap = (Map<String, Object>) basic;
        }
        Map<String, Object> workMap = null;
        Object work = compress.get("work");
        if (work instanceof Map) {
            workMap = (Map<String, Object>) work;
        }
        Map<String, Object> currentWork = currentWork(workMap);
        if (null != basicMap) {
            Object currentStatusObj = basicMap.get("current_status");
            int currentStatus = 0;
            try {
                currentStatus = null != currentStatusObj ? Integer.parseInt(String.valueOf(currentStatusObj)) : 0;
            } catch (Exception e) {
                logger.info("{} parse int error", currentStatusObj);
            }
            if (null != currentWork) {
                Object endTimeObj = currentWork.get("end_time");
                String endTime = null != endTimeObj ? String.valueOf(endTimeObj) : "";
                if (currentStatus == 1 && StringUtils.isNoneBlank(endTime)) {
                    result = 1;
                }
            }
        }
        logger.info("id:{},cv_current_status:{}", resumeId, result);
        return result;
    }

    public Integer start(CvCurrentStatus cvCurrentStatus) throws Exception {
        Future<Integer> submit = threadPoolExecutor.submit(cvCurrentStatus);
        return submit.get();
    }

    public static void init() {
        threadPoolExecutor = (ThreadPoolExecutor) ExecutorFactory.newFixedThreadPool(ThreadNumber);
    }

    public static void init(int threadNumber) {
        threadPoolExecutor = (ThreadPoolExecutor) ExecutorFactory.newFixedThreadPool(threadNumber);
    }

    private Map<String, Object> currentWork(Map<String, Object> workMap) {
        Map<String, Object> currentWork = null;
        if (null != workMap && !workMap.isEmpty()) {
            for (Map.Entry<String, Object> entry : workMap.entrySet()) {
                Map<String, Object> work = (Map<String, Object>) entry.getValue();
                if (null != work) {
                    int sortId = 0;
                    try {
                        Object sortIdObj = work.get("sort_id");
                        sortId = null != sortIdObj ? Integer.parseInt(String.valueOf(sortIdObj)) : 0;
                    } catch (NumberFormatException e) {
                        logger.error("sort_id:{} parse error:{}", sortId, e.getMessage());
                    }
                    if (sortId == 1) {
                        currentWork = work;
                        break;
                    }
                }
            }
        }
        return currentWork;
    }
}
