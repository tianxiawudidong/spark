package com.ifchange.spark.algorithms.http;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.util.ExecutorFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * cv 技能识别
 *
 * @author root
 */
public class CvSkillHttp extends AlgorithmsHttp implements Callable<String> {

    //test
//    private static final String url = "http://10.9.10.54:51657/ner_for_batch";

    //prod
    private static final String url = "http://nlp.rpc/ner_for_batch";

    private static final Logger LOG = LoggerFactory.getLogger(CvSkillHttp.class);

    private static ThreadPoolExecutor threadPoolExecutor;

    private Map<String, Object> workMap = new LinkedHashMap<>();

    private Map<String, Object> skillMap = new LinkedHashMap<>();

    private Map<String, Object> projectMap = new LinkedHashMap<>();

    private static final int ThreadNumber = 2;

    public CvSkillHttp(String resumeId, Map<String, Object> compress) throws Exception {
        super();
        if (null == threadPoolExecutor) {
            throw new Exception("thread pool not init,please call init method");
        }
        requestMap.put("c", "cv_tag");
        requestMap.put("m", "get_skill_tags");
        Map<String, Object> pMap = new HashMap<>();
        requestMap.put("p", pMap);
        headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));
        pMap.put("cv_id", resumeId);
        pMap.put("skill", skillMap);
        pMap.put("work_map", workMap);
        try {
            processCompress(compress);
        } catch (Exception e) {
            LOG.error("{} msg:{}", resumeId, e.getMessage());
        }
    }

    /**
     * updated_at 2019/06/05
     * 输入 "work_map" 增加 function_id
     * <p>
     * updated_at 2019/12/09
     * 输入 增加project
     */
    public CvSkillHttp(String resumeId, Map<String, Object> compress, Map<String, String> functionMap) {
        super();
        requestMap.put("c", "cv_tag");
        requestMap.put("m", "get_skill_tags");
        Map<String, Object> pMap = new HashMap<>();
        requestMap.put("p", pMap);
        headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));
        pMap.put("cv_id", resumeId);
        pMap.put("skill", skillMap);
        pMap.put("work_map", workMap);
        //updated_at 2019/12/09  输入 增加project
        pMap.put("project", projectMap);
        try {
            processCompressWithFunctionMap(compress, functionMap);
        } catch (Exception e) {
            LOG.error("{} msg:{}", resumeId, e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private void processCompress(Map<String, Object> compress) throws Exception {
        try {
            Map<String, Object> workMap = new HashMap<>();
            Object work1 = compress.get("work");
            if (work1 instanceof Map) {
                workMap = (Map<String, Object>) work1;
            }
            if (workMap.size() > 0) {
                for (Map.Entry<String, Object> entry : workMap.entrySet()) {
                    String workId = entry.getKey();
                    Map<String, Object> work = (Map<String, Object>) entry.getValue();
                    String isDeleted = null != work.get("is_deleted") ? String.valueOf(work.get("is_deleted")) : "N";
                    if (StringUtils.isNotBlank(isDeleted) && isDeleted.equals("Y")) continue;
                    putWorkList(workId,
                        null != work.get("position_name") ? String.valueOf(work.get("position_name")) : "",
                        null != work.get("responsibilities") ? String.valueOf(work.get("responsibilities")) : "",
                        null != work.get("corporation_name") ? String.valueOf(work.get("corporation_name")) : "",
                        null != work.get("industry_name") ? String.valueOf(work.get("industry_name")) : "");
                }
            }

            Map<String, Object> skillMap = new HashMap<>();
            Object skill1 = compress.get("skill");
            if (skill1 instanceof Map) {
                skillMap = (Map<String, Object>) skill1;
            }
            if (skillMap.size() > 0) {
                for (Map.Entry<String, Object> entry : skillMap.entrySet()) {
                    String skillId = entry.getKey();
                    Map<String, Object> skill = (Map<String, Object>) entry.getValue();
                    String isDeleted = null != skill.get("is_deleted") ? String.valueOf(skill.get("is_deleted")) : "N";
                    if (StringUtils.isNotBlank(isDeleted) && isDeleted.equals("Y")) continue;
                    putSkillList(skillId, null != skill.get("name") ? String.valueOf(skill.get("name")) : "");
                }
            }
        } catch (Exception e) {
            throw new Exception(e);
        }

    }

    @SuppressWarnings("unchecked")
    private void processCompressWithFunctionMap(Map<String, Object> compress, Map<String, String> functionMap) throws Exception {
        try {
            Map<String, Object> workMap = new HashMap<>();
            Object work1 = compress.get("work");
            if (work1 instanceof Map) {
                workMap = (Map<String, Object>) work1;
            }
            if (workMap.size() > 0) {
                for (Map.Entry<String, Object> entry : workMap.entrySet()) {
                    String workId = entry.getKey();
                    Map<String, Object> work = (Map<String, Object>) entry.getValue();
                    String isDeleted = null != work.get("is_deleted") ? String.valueOf(work.get("is_deleted")) : "N";
                    String functionId = null != functionMap.get(workId) ? functionMap.get(workId) : "";
                    if (StringUtils.isNotBlank(isDeleted) && isDeleted.equals("Y")) continue;
                    putWorkListWithFunctionId(workId,
                        null != work.get("position_name") ? String.valueOf(work.get("position_name")) : "",
                        null != work.get("responsibilities") ? String.valueOf(work.get("responsibilities")) : "",
                        null != work.get("corporation_name") ? String.valueOf(work.get("corporation_name")) : "",
                        null != work.get("industry_name") ? String.valueOf(work.get("industry_name")) : "",
                        functionId);
                }
            }


            Map<String, Object> skillMap = new HashMap<>();
            Object skill1 = compress.get("skill");
            if (skill1 instanceof Map) {
                skillMap = (Map<String, Object>) skill1;
            }
            if (skillMap.size() > 0) {
                for (Map.Entry<String, Object> entry : skillMap.entrySet()) {
                    String skillId = entry.getKey();
                    Map<String, Object> skill = (Map<String, Object>) entry.getValue();
                    String isDeleted = null != skill.get("is_deleted") ? String.valueOf(skill.get("is_deleted")) : "N";
                    if (StringUtils.isNotBlank(isDeleted) && isDeleted.equals("Y")) continue;
                    putSkillList(skillId, null != skill.get("name") ? String.valueOf(skill.get("name")) : "");
                }
            }

            Map<String, Object> projectMaps = new HashMap<>();
            Object project = compress.get("project");
            if (project instanceof Map) {
                projectMaps = (Map<String, Object>) project;
            }
            if (projectMaps.size() > 0) {
                for (Map.Entry<String, Object> entry : projectMaps.entrySet()) {
                    //describe
                    //responsibilities
                    String projectId = entry.getKey();
                    Map<String, Object> projectDetailMap = (Map<String, Object>) entry.getValue();
                    String describe = null != projectDetailMap.get("describe")
                        ? String.valueOf(projectDetailMap.get("describe")) : "";
                    String responsibilities = null != projectDetailMap.get("responsibilities")
                        ? String.valueOf(projectDetailMap.get("responsibilities")) : "";
                    String isDeleted = null != projectDetailMap.get("is_deleted")
                        ? String.valueOf(projectDetailMap.get("is_deleted")) : "N";
                    if (StringUtils.isNotBlank(isDeleted) && isDeleted.equals("Y")) continue;
                    putProjectList(projectId, describe, responsibilities);
                }
            }

        } catch (Exception e) {
            throw new Exception(e);
        }

    }

    private void putWorkList(String workId, String positionName, String responsibilities, String corporationName, String industryName) {
        Map<String, Object> workList = new HashMap<>();
        workList.put("id", workId);
        workList.put("title", StringUtils.isNoneBlank(positionName) ? positionName : "");
        workList.put("desc", StringUtils.isNoneBlank(responsibilities) ? responsibilities : "");
        workList.put("corporation_name", StringUtils.isNoneBlank(corporationName) ? corporationName : "");
        workList.put("industry_name", StringUtils.isNoneBlank(industryName) ? industryName : "");
        workMap.put(workId, workList);
    }

    private void putWorkListWithFunctionId(String workId, String positionName, String responsibilities,
                                           String corporationName, String industryName, String functionId) {
        Map<String, Object> workList = new HashMap<>();
        workList.put("id", workId);
        workList.put("title", StringUtils.isNoneBlank(positionName) ? positionName : "");
        workList.put("desc", StringUtils.isNoneBlank(responsibilities) ? responsibilities : "");
        workList.put("corporation_name", StringUtils.isNoneBlank(corporationName) ? corporationName : "");
        workList.put("industry_name", StringUtils.isNoneBlank(industryName) ? industryName : "");
        workList.put("function_id", functionId);
        workMap.put(workId, workList);
    }

    private void putSkillList(String skillId, String name) {
        Map<String, Object> skillList = new HashMap<>();
        skillList.put("name", name);
        skillMap.put(skillId, skillList);
    }

    private void putProjectList(String projectId, String describe, String responsibilities) {
        Map<String, Object> projectMaps = new HashMap<>();
        projectMaps.put("describe", describe);
        projectMaps.put("responsibilities", responsibilities);
        projectMap.put(projectId, projectMaps);
    }

    @Override
    public String call() {
        String result = "";
        if (!workMap.isEmpty() || !skillMap.isEmpty()) {
            String param = JSON.toJSONString(msgMap);
            HttpClientBuilder hb = HttpClientBuilder.create();
            CloseableHttpClient client = hb.build();
            HttpPost method = new HttpPost(url);
            RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(5000)//建立连接的超时时间
                .setConnectionRequestTimeout(1000)
                .setSocketTimeout(10000)//指客户端和服务进行数据交互的时间，是指两者之间如果两个数据包之间的时间大于该时间则认为超时
                .build();
            method.setConfig(requestConfig);

            //解决中文乱码问题
            StringEntity entity = new StringEntity(param, "utf-8");
            entity.setContentEncoding("UTF-8");
            entity.setContentType("application/json");
            method.setEntity(entity);
            try {
                HttpResponse httpResponse = client.execute(method);
                //请求发送成功，并得到响应
                if (httpResponse.getStatusLine().getStatusCode() == 200) {
                    // 读取服务器返回过来的json字符串数据
                    result = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
                }
                client.close();
            } catch (IOException e) {
                LOG.error("skill_tag call http error:{},param:{}", e.getMessage(), param);
            }
        }
        return result;
    }

    public String start(CvSkillHttp cvSkillHttp) throws Exception {
        String result = "";
        Future<String> submit = threadPoolExecutor.submit(cvSkillHttp);
        String cvSkillResult = submit.get();
//        logger.info("cv_skill:{}", cvSkillResult);
        if (StringUtils.isNoneBlank(cvSkillResult)) {
            JSONObject jsonObject = JSONObject.parseObject(cvSkillResult);
            if (null != jsonObject) {
                JSONObject response = jsonObject.getJSONObject("response");
                if (null != response) {
                    result = response.getString("results");
                }
            }
        }
        return result;
    }

    public static void init() {
        threadPoolExecutor = (ThreadPoolExecutor) ExecutorFactory.newFixedThreadPool(ThreadNumber);
    }

    public static void init(int threadNumber) {
        threadPoolExecutor = (ThreadPoolExecutor) ExecutorFactory.newFixedThreadPool(threadNumber);
    }


}
