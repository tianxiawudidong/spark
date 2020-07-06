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
 * jd_ner_skill
 * @author root
 */
public class JdSkillHttp extends AlgorithmsHttp implements Callable<String> {

//    private static final String url = "http://192.168.1.210:51657/ner_for_batch";

//    private static final String url="http://10.9.10.54:51657/ner_for_batch";

    private static final String url="http://nlp.rpc/ner_for_batch";

    private static final Logger logger = LoggerFactory.getLogger(JdSkillHttp.class);

    private static ThreadPoolExecutor threadPoolExecutor;

    private static final int ThreadNumber = 2;

    private Map<String, Object> paramMap = new LinkedHashMap<>();

    public JdSkillHttp(String positionId, String requirement, String name, String description) throws Exception {
        super();
        if (null == threadPoolExecutor) {
            throw new Exception("thread pool not init,please call init method");
        }
        super.headerMap.put("log_id", String.format("%s_%d_%d", positionId, System.currentTimeMillis(), Thread.currentThread().getId()));
        requestMap.put("c", "jd_tag");
        requestMap.put("m", "get_skill_tags");
        List<Map<String, Object>> pList = new ArrayList<>();
        Map<String, Object> pMap = new HashMap<>();
        paramMap.put("id", positionId);
        paramMap.put("requirement", requirement);
        paramMap.put("name", name);
        paramMap.put("description", description);
        pList.add(paramMap);
        pMap.put("positions", pList);
        requestMap.put("p", pMap);
    }

    @Override
    public String call() {
        String result = "";
        if (null != paramMap && paramMap.size() > 0) {
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
                logger.info("jd_ner_skill call http error:{}", e.getMessage());
            }
        }
        return result;
    }

    public String start(JdSkillHttp jdTagHttp) throws Exception {
        String result="";
        Future<String> submit = threadPoolExecutor.submit(jdTagHttp);
        String get = submit.get();
        if(StringUtils.isNoneBlank(get)){
            JSONObject jsonObject = JSONObject.parseObject(get);
            if(null != jsonObject){
                JSONObject response = jsonObject.getJSONObject("response");
                if(null !=response){
                    result=response.getString("results");
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

    public static void main(String[] args) throws Exception{
        JdSkillHttp.init();
        JdSkillHttp jdSkillHttp = new JdSkillHttp("125", "具有后端项目开发经验", "java开发工程师", "");
        String start = jdSkillHttp.start(jdSkillHttp);
        logger.info(start);
    }




}
