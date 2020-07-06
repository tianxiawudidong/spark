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
 * cv 领域标签
 *
 * @author root
 */
public class CvDomainHttp extends AlgorithmsHttp implements Callable<String> {

    private static final String url = "http://nlp.rpc/ner_for_batch";

    private static final Logger logger = LoggerFactory.getLogger(CvDomainHttp.class);

    private static ThreadPoolExecutor threadPoolExecutor;

    private Map<String, Object> workMap = new LinkedHashMap<>();

    private static final int ThreadNumber = 2;

    public CvDomainHttp(String resumeId, Map<String, Object> compress, Map<String, String> functionMap) throws Exception {
        super();
        if (null == threadPoolExecutor) {
            throw new Exception("thread pool not init,please call init method");
        }
        requestMap.put("c", "cv_tag");
        requestMap.put("m", "get_domain_tags");
        Map<String, Object> pMap = new HashMap<>();
        requestMap.put("p", pMap);
        headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));
        pMap.put("cv_id", resumeId);
        pMap.put("work_map", workMap);
        try {
            processResumeWithFunctionMap(compress, functionMap);
        } catch (Exception e) {
            logger.info("{} msg:{}", resumeId, e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private void processResumeWithFunctionMap(Map<String, Object> compress, Map<String, String> functionMap) throws Exception {
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
        } catch (Exception e) {
            throw new Exception(e);
        }

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


    @Override
    public String call() {
        String result = "";
        if (!workMap.isEmpty()) {
            String param = JSON.toJSONString(msgMap);
//            logger.info(param);
            HttpClientBuilder hb = HttpClientBuilder.create();
            CloseableHttpClient client = hb.build();
            HttpPost method = new HttpPost(url);
            //设置超时时间
            RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(3000)//建立连接的超时时间
                .setConnectionRequestTimeout(5000)
                .setSocketTimeout(5000)//指客户端和服务进行数据交互的时间，是指两者之间如果两个数据包之间的时间大于该时间则认为超时
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
                logger.info("cv_domain call http error:{}", e.getMessage());
            }
        }
        return result;
    }

    public String start(CvDomainHttp cvDomainHttp) throws Exception {
        String result = "";
        Future<String> submit = threadPoolExecutor.submit(cvDomainHttp);
        String cvDomainResult = submit.get();
//        logger.info(cvDomainResult);
        if (StringUtils.isNoneBlank(cvDomainResult)) {
            JSONObject jsonObject = JSONObject.parseObject(cvDomainResult);
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
