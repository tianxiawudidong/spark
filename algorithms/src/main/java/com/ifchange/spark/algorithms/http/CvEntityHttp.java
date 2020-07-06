package com.ifchange.spark.algorithms.http;

import com.alibaba.fastjson.JSON;
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
 * cv_entity
 * 提取jd描述和cv内容中的专业、技能、职位title、证书、工作经验年限、管理经验年限等
 */
public class CvEntityHttp extends AlgorithmsHttp implements Callable<String> {

    private static final String url = "http://algo.rpc/cv_entity_multi";

//    private static final String url="http://192.168.8.78:51649/cv_entity";

    private Map<String, Object> workMap = new HashMap<>();

    private Map<String, Object> projectMap = new HashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(CvEntityHttp.class);

    private static ThreadPoolExecutor threadPoolExecutor;

    private static final int ThreadNumber = 2;


    public CvEntityHttp(String resumeId, Map<String, Object> compress) throws Exception {
        super();
        if (null == threadPoolExecutor) {
            throw new Exception("thread pool not init,please call init method");
        }
        Map<String, Object> pMap = new LinkedHashMap<>();
        requestMap.put("c", "cv_entity");
        requestMap.put("m", "get_cv_entitys");
        requestMap.put("p", pMap);
        headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));
        pMap.put("cv_id", resumeId);
        pMap.put("work_map", workMap);
        pMap.put("project_map", projectMap);
        try {
            processCompress(compress);
        } catch (Exception e) {
            logger.info("{} msg:{}", resumeId, e.getMessage());
        }
    }

    private void processCompress(Map<String, Object> compress) throws Exception {
        Map<String, Object> workMap = new HashMap<>();
        Object work = compress.get("work");
        if (work instanceof Map) {
            workMap = (Map<String, Object>) work;
        }
        if (workMap.isEmpty()) {
            throw new Exception("work is null or empty");
        }
        for (Map.Entry<String, Object> entry : workMap.entrySet()) {
            String workId = entry.getKey();
            Map<String, Object> workDetail = (Map<String, Object>) entry.getValue();
            String isDeleted = null != workDetail.get("is_deleted") ? String.valueOf(workDetail.get("is_deleted")) : "N";
            if (StringUtils.isNotBlank(isDeleted) && isDeleted.equals("Y")) continue;
            putWorkList(workId, String.valueOf(workDetail.get("position_name")),
                String.valueOf(workDetail.get("responsibilities")));
        }


        Map<String, Object> projectMap = new HashMap<>();
        Object project = compress.get("project");
        if (project instanceof Map) {
            projectMap = (Map<String, Object>) project;
        }

        if (projectMap.size() > 0) {
            for (Map.Entry<String, Object> entry : projectMap.entrySet()) {
                String projectId = entry.getKey();
                Map<String, Object> projectDetail = (Map<String, Object>) entry.getValue();
                String isDeleted = null != projectDetail.get("is_deleted") ? String.valueOf(projectDetail.get("is_deleted")) : "N";
                if (StringUtils.isNotBlank(isDeleted) && isDeleted.equals("Y")) continue;
                putProjectList(projectId,
                    String.valueOf(projectDetail.get("name")),
                    String.valueOf(projectDetail.get("describe")),
                    String.valueOf(projectDetail.get("responsibilities")));
            }
        }
    }

    private void putWorkList(String workId, String positionName, String responsibilities) {
        Map<String, Object> workList = new HashMap<>();
        workList.put("id", workId);
        workList.put("type", 0);
        workList.put("title", StringUtils.isNoneBlank(positionName) ? positionName : "");
        workList.put("desc", StringUtils.isNoneBlank(responsibilities) ? responsibilities : "");
        workMap.put(workId, workList);
    }

    private void putProjectList(String projectId, String name, String describe, String responsibilities) {
        Map<String, Object> proMap = new HashMap<>();
        proMap.put("id", projectId);
        proMap.put("name", StringUtils.isNoneBlank(name) ? name : "");
        proMap.put("describe", StringUtils.isNoneBlank(describe) ? describe : "");
        proMap.put("responsibilities", StringUtils.isNoneBlank(responsibilities) ? responsibilities : "");
        projectMap.put(projectId, proMap);
    }


    @Override
    public String call() {
        String result = "";
        if (null != workMap && workMap.size() > 0) {
            String param = JSON.toJSONString(msgMap);
//            logger.info(param);
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
                logger.info("cv_entity call http error:{},param:{}", e.getMessage(), param);
            }
        }
        return result;
    }

    public String start(CvEntityHttp cvEntityHttp) throws Exception {
        Future<String> submit = threadPoolExecutor.submit(cvEntityHttp);
        return submit.get();
    }

    public static void init() {
        threadPoolExecutor = (ThreadPoolExecutor) ExecutorFactory.newFixedThreadPool(ThreadNumber);
    }

    public static void init(int threadNumber) {
        threadPoolExecutor = (ThreadPoolExecutor) ExecutorFactory.newFixedThreadPool(threadNumber);
    }
}
