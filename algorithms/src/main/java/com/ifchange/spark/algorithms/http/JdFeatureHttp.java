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
public class JdFeatureHttp extends AlgorithmsHttp implements Callable<String> {

//    private static final String url = "http://192.168.1.111:51650/jd_term_weight";

//    private static final String url="http://testing2.algo.rpc/jd_term_weight";

    private static final String url = "http://algo.rpc/jd_term_weight";

    private static final Logger logger = LoggerFactory.getLogger(JdFeatureHttp.class);

    private static ThreadPoolExecutor threadPoolExecutor;

    private static final int ThreadNumber = 2;

    private Map<String, Object> pMap = new LinkedHashMap<>();

    /**
     * @param positionId      id
     * @param corporationName corporation_name
     * @param editedAt        edited_at
     * @param requirement     requirement
     * @param corporationId   corporation_id
     * @param userId          user_id
     * @param description     description
     * @param name            name
     * @throws Exception ex
     */
    public JdFeatureHttp(String positionId, String corporationName, String editedAt, String requirement, String corporationId,
                         String userId, String description, String name) throws Exception {
        super();
        if (null == threadPoolExecutor) {
            throw new Exception("thread pool not init,please call init method");
        }
        requestMap.put("c", "rs_feature");
        requestMap.put("m", "get_jd_feature");
        requestMap.put("p", pMap);
        super.headerMap.put("log_id", String.format("%s_%d_%d", positionId, System.currentTimeMillis(), Thread.currentThread().getId()));

        Map<String, Object> map = new HashMap<>();
        Map<String, Object> positionMap = new HashMap<>();
        map.put("corporation_name", corporationName);
        map.put("edited_at", editedAt);
        map.put("requirement", requirement);
        map.put("corporation_id", corporationId);
        map.put("user_id", userId);
        map.put("id", positionId);
        map.put("description", description);
        map.put("name", name);
        positionMap.put("position", map);

        pMap.put("jd_id", positionId);
        pMap.put("jd_json", JSON.toJSONString(positionMap));
    }

    public JdFeatureHttp(String positionId, String positionJson) throws Exception {
        super();
        if (null == threadPoolExecutor) {
            throw new Exception("thread pool not init,please call init method");
        }
        requestMap.put("c", "rs_feature");
        requestMap.put("m", "get_jd_feature");
        requestMap.put("p", pMap);
        pMap.put("jd_id", positionId);
        pMap.put("jd_json", positionJson);
    }

    @Override
    public String call() {
        String result = "";
        if (null != pMap && pMap.size() > 0) {
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
                logger.info("jd_feature call http error:{}", e.getMessage());
            }
        }
        return result;
    }

    public String start(JdFeatureHttp jdFeatureHttp) throws Exception {
        String result = "";
        Future<String> submit = threadPoolExecutor.submit(jdFeatureHttp);
        String get = submit.get();
//        logger.info(get);
        if (StringUtils.isNoneBlank(get)) {
            JSONObject jsonObject = JSONObject.parseObject(get);
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
