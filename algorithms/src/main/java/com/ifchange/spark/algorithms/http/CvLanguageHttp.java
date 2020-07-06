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

public class CvLanguageHttp extends AlgorithmsHttp implements Callable<String> {

    private static final String url = "http://search.offline.rpc/ner_servers_language";

    private Map<String, Object> pMap = new LinkedHashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(CvLanguageHttp.class);

    private static ThreadPoolExecutor threadPoolExecutor;

    private static final int ThreadNumber = 2;

    public CvLanguageHttp(String resumeId, Map<String, Object> compress) throws Exception {
        super();
        if (null == threadPoolExecutor) {
            throw new Exception("thread pool or queue not init,please call init method");
        }
        super.headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));
        requestMap.put("c", "CVLanguage");
        requestMap.put("m", "query");
        requestMap.put("p", pMap);
        try {
            processCompress(compress);
        } catch (Exception e) {
            logger.info("{} msg:{}", resumeId, e.getMessage());
        }
    }

    private void processCompress(Map<String, Object> compress) throws Exception {
        Map<String, Object> langMap = new HashMap<>();
        Map<String, Object> languageMap = new HashMap<>();
        Object language = compress.get("language");
        if (language instanceof Map) {
            languageMap = (Map<String, Object>) language;
        }
        if (languageMap.isEmpty()) {
            throw new Exception("language is null or empty");
        }
        for (Map.Entry<String, Object> entry : languageMap.entrySet()) {
            Map<String, String> map = new HashMap<>();
            String languageId = entry.getKey();
            Map<String, Object> languageDetail = (Map<String, Object>) entry.getValue();
            map.put("certificate", null != languageDetail.get("certificate") ? String.valueOf(languageDetail.get("certificate")) : "");
            map.put("name", null != languageDetail.get("name") ? String.valueOf(languageDetail.get("name")) : "");
            map.put("level", null != languageDetail.get("level") ? String.valueOf(languageDetail.get("level")) : "");
            langMap.put(languageId, map);
        }
        pMap.put("languages", langMap);
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
                logger.info("cv_language call http error:{},param:{}", e.getMessage(), param);
            }
        }
        return result;
    }

    public String start(CvLanguageHttp cvTitleHttp) throws Exception {
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
