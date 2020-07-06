package com.ifchange.spark.algorithms.http;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.util.ExecutorFactory;
import com.ifchange.spark.util.JsonBeanUtils;
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author root
 */
public class CvFeatureHttpForAbTest extends AlgorithmsHttp implements Callable<String> {

//    private static final String url="http://algo.rpc/cv_feature";

//    private static final String url = "http://192.168.8.52:1800/cv_feature";

    //http://10.9.10.25:1800/cv_feature
//    private static final String url = "http://10.9.10.25:1800/cv_feature";

    private static String url;

    private static final Logger logger = LoggerFactory.getLogger(CvFeatureHttpForAbTest.class);

    private static ThreadPoolExecutor threadPoolExecutor;

    private static final int ThreadNumber = 2;

    private Map<String, Object> pMap = new LinkedHashMap<>();

    private JsonBeanUtils jsonBeanUtils = new JsonBeanUtils();

    public CvFeatureHttpForAbTest(String resumeId, Map<String, Object> compress) throws Exception {
        super();
        if (null == threadPoolExecutor) {
            throw new Exception("thread pool not init,please call init method");
        }
        super.headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));
        requestMap.put("c", "rs_feature");
        requestMap.put("m", "get_all_feature");
        requestMap.put("p", pMap);
        pMap.put("cv_id", String.valueOf(resumeId));
        pMap.put("cv_json", jsonBeanUtils.objectToJson(compress));
    }

    @Override
    public String call() {
        String result = "";
        if (null != pMap && pMap.size() > 0) {
            String param = JSON.toJSONString(msgMap);
            logger.info(param);
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
                logger.info("cv_feature call http error:{},param:{}", e.getMessage(), param);
            }
        }
        return result;
    }

    public String start(CvFeatureHttpForAbTest cvFeatureHttpForAbTest) throws Exception {
        String result = "";
        Future<String> submit = threadPoolExecutor.submit(cvFeatureHttpForAbTest);
        String cvFeatureResult = submit.get();
        logger.info(cvFeatureResult);
        if (StringUtils.isNoneBlank(cvFeatureResult)) {
            JSONObject jsonObject = JSONObject.parseObject(cvFeatureResult);
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
