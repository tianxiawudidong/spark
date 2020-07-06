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
 * @author root
 */
public class CvTagHttpForAbTest extends AlgorithmsHttp implements Callable<String> {

    //    private static final String url = "http://10.9.10.54:51697/function_tag";
    private static String url;

    private static final int ThreadNumber = 2;

    private static final Logger logger = LoggerFactory.getLogger(CvTagHttpForAbTest.class);

    private static ThreadPoolExecutor threadPoolExecutor;

    private Map<String, Object> paramMap = new LinkedHashMap<>();

    public CvTagHttpForAbTest(String resumeId, Map<String, Object> compress) throws Exception {
        super();
        if (null == threadPoolExecutor) {
            throw new Exception("thread pool not init,please call init method");
        }
        requestMap.put("c", "cv_tag");
        requestMap.put("m", "get_cv_tags");
        Map<String, Object> pMap = new HashMap<>();
        requestMap.put("p", pMap);
        headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));
        pMap.put("cv_id", String.valueOf(resumeId));
        pMap.put("work_map", paramMap);
        try {
            processCompress(compress);
        } catch (Exception e) {
            logger.info("{} msg:{}", resumeId, e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private void processCompress(Map<String, Object> compress) throws Exception {
        Map<String, Object> workMap = new HashMap<>();
        Object work1 = compress.get("work");
        if (work1 instanceof Map) {
            workMap = (Map<String, Object>) work1;
        }
        if (workMap.isEmpty()) {
            throw new Exception("work is null");
        }
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

    private void putWorkList(String work_id, String positionName, String responsibilities, String corporationName, String industryName) {
        Map<String, Object> workList = new HashMap<>();
        workList.put("id", work_id);
        workList.put("type", 0);
        workList.put("title", (StringUtils.isNoneBlank(positionName) ? positionName : ""));
        workList.put("desc", (StringUtils.isNoneBlank(responsibilities) ? responsibilities : ""));
        workList.put("corporation_name", (StringUtils.isNoneBlank(corporationName) ? corporationName : ""));
        workList.put("industry_name", (StringUtils.isNoneBlank(industryName) ? industryName : ""));
        paramMap.put(work_id, workList);
    }

    @Override
    public String call() {
        String result = "";
        if (null != paramMap && paramMap.size() > 0) {
            String param = JSON.toJSONString(msgMap);
//            logger.info(param);
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
                logger.info("call http cv_tag_ab_test  error:{},param:{}", e.getMessage(), param);
            }
        }
        return result;
    }

    public String start(CvTagHttpForAbTest cvTagHttpForAbTest) throws Exception {
        String result = "";
        Future<String> submit = threadPoolExecutor.submit(cvTagHttpForAbTest);
        String cvTagResult = submit.get();
//        logger.info(cvTagResult);
        if (StringUtils.isNoneBlank(cvTagResult)) {
            JSONObject jsonObject = JSONObject.parseObject(cvTagResult);
            if (null != jsonObject) {
                JSONObject response = jsonObject.getJSONObject("response");
                if (null != response) {
                    result = response.getString("results");
                }
            }
        }
        return result;
    }


    public static void init(String urls) {
        threadPoolExecutor = (ThreadPoolExecutor) ExecutorFactory.newFixedThreadPool(ThreadNumber);
        url = urls;
    }

    public static void init(int threadNumber, String urls) {
        threadPoolExecutor = (ThreadPoolExecutor) ExecutorFactory.newFixedThreadPool(threadNumber);
        url = urls;
    }

}
