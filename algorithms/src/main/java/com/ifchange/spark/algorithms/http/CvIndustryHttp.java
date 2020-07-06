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
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * cv_industry
 * 2019/12/09
 */
public class CvIndustryHttp extends AlgorithmsHttp implements Callable<String> {

    private static final Logger LOG = LoggerFactory.getLogger(CvIndustryHttp.class);

    private static ThreadPoolExecutor threadPoolExecutor;

    private static final int ThreadNumber = 2;

    private static final String url = "http://nlp.rpc/industry_tag_offline";

    private Map<String, Object> pMap = new LinkedHashMap<>();

    private String resumeId;

    public CvIndustryHttp(String resumeId, Map<String, Object> compress, Map<String, String> functionMap,
                          Map<String, List<Long>> corpIndustryIdsMap) throws Exception {
        super();
        requestMap.put("c", "cv_tag");
        requestMap.put("m", "get_cv_work_tags");
        requestMap.put("p", pMap);
        if (null == threadPoolExecutor) {
            throw new Exception("thread pool not init,please call init method");
        }
        this.resumeId = resumeId;
        super.headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));
        try {
            processCompress(compress, functionMap, corpIndustryIdsMap);
        } catch (Exception e) {
            LOG.error("{} msg:{}", resumeId, e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private void processCompress(Map<String, Object> compress, Map<String, String> functionMap,
                                 Map<String, List<Long>> corpIndustryIdsMap) throws Exception {
        Object workObj = compress.get("work");
        Map<String, Object> work = new HashMap<>();
        if (workObj instanceof Map) {
            work = (Map<String, Object>) workObj;
        }
        if (work.isEmpty()) {
            throw new Exception("work is null or empty");
        }
        Map<String, Object> workMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : work.entrySet()) {
            Map<String, Object> workMapDetail = new HashMap<>();
            String workId = entry.getKey();
            Map<String, Object> workDetail = (Map<String, Object>) entry.getValue();
            String corporationName = null != workDetail.get("corporation_name") ?
                String.valueOf(workDetail.get("corporation_name")) : "";
            String positionName = null != workDetail.get("position_name") ?
                String.valueOf(workDetail.get("position_name")) : "";
            String responsibilities = null != workDetail.get("responsibilities") ?
                String.valueOf(workDetail.get("responsibilities")) : "";
            String functionId = functionMap.getOrDefault(workId, "");
            List<Long> corpIndustryIdsList = corpIndustryIdsMap.getOrDefault(workId, new ArrayList<>());
            workMapDetail.put("corporation_name", corporationName);
            workMapDetail.put("title", positionName);
            workMapDetail.put("desc", responsibilities);
            workMapDetail.put("function_id", functionId);
            workMapDetail.put("corp_industry_ids", corpIndustryIdsList);
            workMap.put(workId, workMapDetail);
        }
        pMap.put("work_map", workMap);
    }


    @Override
    public String call() {
        String result = "";
        if (null != pMap && pMap.size() > 0) {
            long start = System.currentTimeMillis();
            String param = JSON.toJSONString(msgMap);
//            LOG.info("cv_industry param:{}", param);
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
//                    LOG.info("id:{}->cv_industry result:{}", resumeId, result);
                    long end = System.currentTimeMillis();
                    LOG.info("id:{} call cv_industry use time:{}", resumeId, (end - start));
                }
            } catch (Exception e) {
                LOG.error("id:{} cv_industry call http error:{}", resumeId, e.getMessage());
            } finally {
                try {
                    client.close();
                } catch (IOException e) {
                    LOG.error("id:{} client close error:{}", resumeId, e.getMessage());
                }
            }
        }
        return result;
    }

    public String start(CvIndustryHttp cvIndustryHttp) {
        String result = "";
        try {
            Future<String> submit = threadPoolExecutor.submit(cvIndustryHttp);
            String cvIndustryResult = submit.get();
            if (StringUtils.isNoneBlank(cvIndustryResult)) {
                JSONObject jsonObject = JSONObject.parseObject(cvIndustryResult);
                if (null != jsonObject) {
                    JSONObject response = jsonObject.getJSONObject("response");
                    if (null != response) {
                        result = response.getString("results");
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("id:{} cvIndustryResult parse error:{}", resumeId, e.getMessage());
            e.printStackTrace();
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
