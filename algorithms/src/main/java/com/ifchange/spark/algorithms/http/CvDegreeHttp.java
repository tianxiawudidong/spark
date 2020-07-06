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

public class CvDegreeHttp extends AlgorithmsHttp implements Callable<String> {

    private static final String url = "http://search.offline.rpc/ner_servers_degree";

    private Map<String, Object> pMap = new LinkedHashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(CvDegreeHttp.class);

    private static ThreadPoolExecutor threadPoolExecutor;

    private static final int ThreadNumber = 2;


    public CvDegreeHttp(String resumeId, Map<String, Object> compress) throws Exception {
        super();
        if (null == threadPoolExecutor) {
            throw new Exception("thread pool not init,please call init method");
        }
        requestMap.put("c", "CVDegree");
        requestMap.put("m", "query");
        requestMap.put("p", pMap);
        super.headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));

        if (null != compress) {
            Object basic = compress.get("basic");
            if (basic instanceof Map) {
                Map<String, Object> basicMap = (Map<String, Object>) basic;
                int degree = 0;
                try {
                    degree = null != basicMap.get("degree") ? Integer.parseInt(String.valueOf(basicMap.get("degree")))
                        : 0;
                } catch (Exception e) {
                    logger.info("{} parse int error", basicMap.get("degree"));
                }
                pMap.put("basic_degree", degree);
            }
            try {
                processCompress(compress);
            } catch (Exception e) {
                logger.info("{} msg:{}", resumeId, e.getMessage());
            }
        }

    }

    private void processCompress(Map<String, Object> compress) throws Exception {
        Map<String, Object> educationMap = new HashMap<>();
        Object educationObj = compress.get("education");
        Map<String, Object> edu = new HashMap<>();
        if (educationObj instanceof Map) {
            edu = (Map<String, Object>) educationObj;
        }
        if (edu.isEmpty()) {
            throw new Exception("edu is null or empty");
        }
        for (Map.Entry<String, Object> entry : edu.entrySet()) {
            Map<String, String> map = new HashMap<>();
            String eduId = entry.getKey();
            Map<String, Object> education = (Map<String, Object>) entry.getValue();
            map.put("school", null != education.get("school_name") ? String.valueOf(education.get("school_name")) : "");
            map.put("major", null != education.get("discipline_name") ? String.valueOf(education.get("discipline_name")) : "");
            map.put("degree", null != education.get("degree") ? String.valueOf(education.get("degree")) : "0");
            educationMap.put(eduId, map);
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
                logger.info("cv_degree call http error:{},param:{}", e.getMessage(), param);
            }
        }
        return result;
    }

    public String start(CvDegreeHttp cvTitleHttp) throws Exception {
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
