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
 * @author root
 */
public class JdOriginalCorporationsHttp extends AlgorithmsHttp implements Callable<String> {

    private List<Map<String, Object>> workList = new ArrayList<>();

    private static final Logger logger = LoggerFactory.getLogger(JdOriginalCorporationsHttp.class);

    private static ThreadPoolExecutor threadPoolExecutor;

    private static final int ThreadNumber = 2;

    private static final String url = "http://algo.rpc/corp_tag";

    /**
     * @param positionId      id
     * @param corporationName corporationName
     * @throws Exception exception
     */
    public JdOriginalCorporationsHttp(String positionId, String corporationName) throws Exception {
        super();
        if (null == threadPoolExecutor) {
            throw new Exception("thread pool not init,please call init method");
        }
        super.headerMap.put("log_id", String.format("%s_%d_%d", positionId, System.currentTimeMillis(), Thread.currentThread().getId()));
        Map<String, Object> pMap = new LinkedHashMap<>();
        requestMap.put("c", "cv_corp_tag");
        requestMap.put("m", "corp_tag_by_work_info");
        requestMap.put("p", pMap);
        Map<String, Object> map = new HashMap<>();
        map.put("work_id", "0");
        map.put("company_name", corporationName);
        workList.add(map);
        pMap.put("cv_id", positionId);
        pMap.put("work_list", workList);
    }

    @Override
    public String call() {
        String result = "";
        if (null != workList && workList.size() > 0) {
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
                logger.info("crop_tag call http error:{}", e.getMessage());
            }
        }
        return result;
    }

    public String start(JdOriginalCorporationsHttp jdOriginalCorporationsHttp) throws Exception {
        String result = "";
        Future<String> submit = threadPoolExecutor.submit(jdOriginalCorporationsHttp);
        String get = submit.get();
        if (StringUtils.isNoneBlank(get)) {
            JSONObject jsonObject = JSONObject.parseObject(get);
            if (null != jsonObject) {
                JSONObject response = jsonObject.getJSONObject("response");
                if (null != response) {
                    result = response.getString("result");
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
