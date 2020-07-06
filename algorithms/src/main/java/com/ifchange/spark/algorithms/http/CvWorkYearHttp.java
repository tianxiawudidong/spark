package com.ifchange.spark.algorithms.http;

import com.alibaba.fastjson.JSON;
import com.ifchange.spark.util.ExecutorFactory;
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

public class CvWorkYearHttp extends AlgorithmsHttp implements Callable<String> {

    private static final String url = "http://search.offline.rpc/ner_servers_workyear";

    private Map<String, Object> pMap = new LinkedHashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(CvWorkYearHttp.class);

    private static final int ThreadNumber = 2;

    private static ThreadPoolExecutor threadPoolExecutor;

    public CvWorkYearHttp(String resumeId, Map<String, Object> compress) throws Exception {
        super();
        if (null == threadPoolExecutor) {
            throw new Exception("thread pool not init,please call init method");
        }
        super.headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));
        requestMap.put("c", "CVWorkYear");
        requestMap.put("m", "query");
        requestMap.put("p", pMap);
        try {
            processCompress(compress);
        } catch (Exception e) {
            logger.info("{} msg:{}", resumeId, e.getMessage());
        }
    }

    private void processCompress(Map<String, Object> compress) throws Exception {
        Map<String, Object> workMap = new HashMap<>();
        Map<String, Object> works = new HashMap<>();
        Object work1 = compress.get("work");
        if (work1 instanceof Map) {
            works = (Map<String, Object>) work1;
        }
        if (works.isEmpty()) {
            throw new Exception("work is null or empty");
        }
        for (Map.Entry<String, Object> entry : works.entrySet()) {
            Map<String, String> map = new HashMap<>();
            String workId = entry.getKey();
            Map<String, Object> workDetail = (Map<String, Object>) entry.getValue();
            map.put("start_time", null != workDetail.get("start_time") ? String.valueOf(workDetail.get("start_time")) : "");
            map.put("end_time", null != workDetail.get("end_time") ? String.valueOf(workDetail.get("end_time")) : "");
            map.put("so_far", null != workDetail.get("so_far") ? String.valueOf(workDetail.get("so_far")) : "");
            map.put("corporation_name", null != workDetail.get("corporation_name") ? String.valueOf(workDetail.get("corporation_name")) : "");
            map.put("position_name", null != workDetail.get("position_name") ? String.valueOf(workDetail.get("position_name")) : "");
            workMap.put(workId, map);
        }
        pMap.put("works", workMap);

        Map<String, Object> educationMap = new HashMap<>();
        Map<String, Object> educations= new HashMap<>();
        Object education1 = compress.get("education");
        if (education1 instanceof Map) {
            educations = (Map<String, Object>) education1;
        }
        if (educations.size() > 0) {
            for (Map.Entry<String, Object> entry : educations.entrySet()) {
                Map<String, String> map = new HashMap<>();
                String eduId = entry.getKey();
                Map<String, Object> education = (Map<String, Object>) entry.getValue();
                map.put("start_time", null != education.get("start_time") ? String.valueOf("start_time") : "");
                map.put("end_time", null != education.get("end_time") ? String.valueOf("end_time") : "");
                map.put("so_far", null != education.get("so_far") ? String.valueOf("so_far") : "");
                map.put("school_name", null != education.get("school_name") ? String.valueOf("school_name") : "");
                educationMap.put(eduId, map);
            }
        }
        pMap.put("educations", educationMap);
    }



    @Override
    public String call() {
        String result = "";
        if (null != pMap && pMap.size() > 0) {
            String param = JSON.toJSONString(msgMap);
            HttpClientBuilder hb = HttpClientBuilder.create();
            CloseableHttpClient client = hb.build();
            HttpPost method = new HttpPost(url);
            RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(5000)//建立连接的超时时间
                .setConnectionRequestTimeout(1000)
                .setSocketTimeout(3000)//指客户端和服务进行数据交互的时间，是指两者之间如果两个数据包之间的时间大于该时间则认为超时
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
                logger.info("cv_workyear call http error:{},param:{}", e.getMessage(), param);
            }
        }
        return result;
    }

    public String start(CvWorkYearHttp cvTitleHttp) throws Exception {
        Future<String> submit = threadPoolExecutor.submit(cvTitleHttp);
        return submit.get();
    }

    public static void init() {
        threadPoolExecutor = (ThreadPoolExecutor) ExecutorFactory.newFixedThreadPool(ThreadNumber);
    }

    public static void init(int threadNumber) {
        threadPoolExecutor = (ThreadPoolExecutor) ExecutorFactory.newFixedThreadPool(threadNumber);
    }

}
