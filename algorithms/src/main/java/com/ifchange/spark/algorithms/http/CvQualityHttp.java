package com.ifchange.spark.algorithms.http;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.util.ExecutorFactory;
import com.ifchange.talent.quality.TalentQualityEngine;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author root
 */
public class CvQualityHttp implements Callable<String> {


    private Map<String, Object> pMap = new HashMap<>();
    private static final int ThreadNumber = 10;
    private static ThreadPoolExecutor threadPool;
    private static final Logger logger = LoggerFactory.getLogger(CvQualityHttp.class);
    private static TalentQualityEngine talentQualityEngine;

    public CvQualityHttp(String resumeId, String cvEducation, String cvDegree,
                         String cvTrade, String cvWorkYear, String cvTag,
                         Map<String, Object> compress) throws Exception {
        if (null == threadPool) {
            throw new Exception("thread pool is not set,please call init method");
        }
        pMap.put("cvid", resumeId);
        pMap.put("skills", new ArrayList<>());
        process(cvEducation, cvDegree, cvTrade, cvWorkYear, cvTag, compress);
    }

    @SuppressWarnings("unchecked")
    private void process(String cvEducation, String cvDegree, String cvTrade, String cvWorkYear,
                         String cvTag, Map<String, Object> compress) {
        int workExpYear = 0;
        if (StringUtils.isNoneBlank(cvWorkYear)) {
            try {
                workExpYear = Math.round(Float.valueOf(cvWorkYear) / 12);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        pMap.put("workexpyears", String.valueOf(workExpYear));


        Map<String, Object> workDesc = new HashMap<>();

        Map<String, Object> workMap = new HashMap<>();
        Object work1 = compress.get("work");
        if (work1 instanceof Map) {
            workMap = (Map<String, Object>) work1;
        }
        String currentWorkId = "";
        if (workMap.size() > 0) {
            for (Map.Entry<String, Object> entry : workMap.entrySet()) {
                Map<String, Object> work = (Map<String, Object>) entry.getValue();
                String workId = entry.getKey();
                workDesc.put(workId, work.get("responsibilities"));
                Object sortId = work.get("sort_id");
                try {
                    if (null != sortId && Integer.parseInt(String.valueOf(sortId)) == 1) {
                        currentWorkId = workId;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        pMap.put("work_desc", workDesc);


        List<String> schoolIds = new ArrayList<>();
        if (StringUtils.isNoneBlank(cvEducation)) {
            JSONObject schools = JSONObject.parseObject(cvEducation);
            if (null != schools && schools.size() > 0) {
                for (Map.Entry<String, Object> entry : schools.entrySet()) {
                    Map<String, Object> value = (Map<String, Object>) entry.getValue();
                    String schoolId = null != value.get("school_id") ? String.valueOf(value.get("school_id")) : "0";
                    schoolIds.add(schoolId);
                }
            }
        }
        pMap.put("schoolid", schoolIds);

        String degreeId = StringUtils.isNoneBlank(cvDegree) ? cvDegree : "0";
        pMap.put("degreeid", degreeId);


        List<String> companyIds = new ArrayList<>();
        if (StringUtils.isNoneBlank(cvTrade)) {
            JSONArray array = JSONObject.parseArray(cvTrade);
            for (int i = 0; i < array.size(); i++) {
                JSONObject json = array.getJSONObject(i);
                String companyId = String.valueOf(json.getLong("company_id"));
                companyIds.add(companyId);
            }
        }
        pMap.put("corpid", companyIds);

        String funId = "";
        if (StringUtils.isNoneBlank(cvTag)) {
            JSONObject results = JSONObject.parseObject(cvTag);
            if (null != results && results.size() > 0) {
                JSONObject json = results.getJSONObject(currentWorkId);
                if (null != json) {
                    JSONArray mustArray = json.getJSONArray("must");
                    if (null != mustArray && mustArray.size() > 0) {
                        String mustStr = mustArray.getString(0);
                        funId = mustStr.split(":")[0];
                    }
                }
            }
        }
        pMap.put("func_id3", funId);
    }


    @Override
    public String call() throws Exception {
        Map<String, Object> result;
        if (null != talentQualityEngine) {
//            logger.info("pMap:{}", JSON.toJSONString(pMap));
            try {
                result = talentQualityEngine.qualityScore(pMap);
            } catch (Exception e) {
                e.printStackTrace();
                throw new Exception(e.getMessage());
            }
        } else {
            throw new Exception("talentQualityEngine is not constructor");
        }
        return JSON.toJSONString(result);
    }

    public static void init() {
        logger.info("init cv_quality2....");
        String path = "/opt/userhome/hadoop/quality";
        threadPool = (ThreadPoolExecutor) ExecutorFactory.newFixedThreadPool(ThreadNumber);
        talentQualityEngine = TalentQualityEngine.getDefaultTalentQualityEngine(path);
    }

    public static void init(int threadNumber) {
        logger.info("init cv_quality2....");
        threadPool = (ThreadPoolExecutor) ExecutorFactory.newFixedThreadPool(threadNumber);
        String path = "/opt/userhome/hadoop/quality";
        talentQualityEngine = TalentQualityEngine.getDefaultTalentQualityEngine(path);
    }

    public String start(CvQualityHttp cvQualityForHttp) throws Exception {
        Future<String> submit = threadPool.submit(cvQualityForHttp);
        return submit.get();
    }


}
