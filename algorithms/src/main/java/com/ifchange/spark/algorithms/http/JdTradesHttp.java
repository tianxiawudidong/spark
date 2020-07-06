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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * jd_trades --->jd_bi
 * 先调用tob_internal_account
 */
public class JdTradesHttp extends AlgorithmsHttp implements Callable<String> {
//    private static final String url = "http://192.168.1.111:51641/jd_bi";

//    private static final String url = "http://testing2.rpc/jd_bi";

    private static final String url = "http://algo.rpc/jd_bi";

    private Map<String, Object> paramMap = new LinkedHashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(JdTradesHttp.class);

    private static ThreadPoolExecutor threadPoolExecutor;

    private static final int ThreadNumber = 2;

    /**
     * @param positionId     id
     * @param userId         'user_id'
     * @param corpId         'corporation_id'
     * @param tagId          ($jd_tags['ref_zhineng_multi']['must'][0]['function_id'])?0:$jd_tags['ref_zhineng_multi']['must'][0]['function_id'],
     * @param tagFlag        'tag_flag'
     * @param jdCorporations 'jd_corporations'
     * @param jdSchools      'jd_schools'
     * @param industries     'industries'
     * @param desc           'description'
     * @param name           'name'
     * @param requirement    'requirement'
     * @param corpName       'corporation_name'
     * @param address        'address'
     * @param tagLv4Ids      '$jd_tags' 四级职能
     * @throws Exception ex
     */
    public JdTradesHttp(String positionId, String requirement, String desc,
                        Map<String, Object> tagFlag, int tagId, String corpId,
                        String address, List<Long> tagLv4Ids, String industries,
                        String jdCorporations, String userId, String name,
                        String corpName, String jdSchools) throws Exception {
        super();
        if (null == threadPoolExecutor) {
            throw new Exception("thread pool not init,please call init method");
        }
        requestMap.put("c", "jd_bi");
        requestMap.put("m", "getAllPreferences");
        requestMap.put("p", paramMap);

        paramMap.put("jd_id", positionId);
        paramMap.put("requirement", requirement);
        paramMap.put("description", desc);
        paramMap.put("tag_flag", tagFlag);
        paramMap.put("prefer_type", "");
        paramMap.put("tag_id", tagId);
        paramMap.put("corp_id", corpId);
        paramMap.put("address", address);
        paramMap.put("tag_lv4_ids", tagLv4Ids);
        paramMap.put("industries", industries);
        paramMap.put("jd_corporations", jdCorporations);
        paramMap.put("user_id", userId);
        paramMap.put("name", name);
        paramMap.put("corp_name", corpName);
        paramMap.put("jd_schools", jdSchools);

    }

    /**
     * @param positionId     id
     * @param userId         'user_id'
     * @param corpId         'corporation_id'
     * @param tagId          ($jd_tags['ref_zhineng_multi']['must'][0]['function_id'])?0:$jd_tags['ref_zhineng_multi']['must'][0]['function_id'],
     * @param tagFlag        'tag_flag'
     * @param jdCorporations 'jd_corporations'
     * @param jdSchools      'jd_schools'
     * @param industries     'industries'
     * @param industryId     empty($tob_internal_account['industry'][0]['tid'])?0:$tob_internal_account['industry'][0]['tid'],
     * @param desc           'description'
     * @param name           'name'
     * @param requirement    'requirement'
     * @param corpName       'corporation_name'
     * @param address        'address'
     * @throws Exception ex
     */
    public JdTradesHttp(String positionId, String userId, String corpId,
                        int tagId, String tagFlag, List<Long> tagLv4Ids,
                        String jdCorporations,
                        String jdSchools, String industries, int industryId,
                        String desc, String name, String requirement,
                        String corpName, String address) throws Exception {
        super();
        if (null == threadPoolExecutor) {
            throw new Exception("thread pool not init,please call init method");
        }
        requestMap.put("c", "jd_bi");
        requestMap.put("m", "getAllPreferences");
        requestMap.put("p", paramMap);

        paramMap.put("user_id", userId);
        paramMap.put("corp_id", corpId);
        paramMap.put("tag_id", tagId);
        paramMap.put("tag_lv4_ids", tagLv4Ids);
        paramMap.put("tag_flag", tagFlag);
        paramMap.put("jd_corporations", jdCorporations);
        paramMap.put("jd_schools", jdSchools);
        paramMap.put("industries", industries);
        paramMap.put("industry_id", industryId);
        paramMap.put("jd_id", positionId);
        paramMap.put("description", desc);
        paramMap.put("name", name);
        paramMap.put("requirement", requirement);
        paramMap.put("prefer_type", "all");
        paramMap.put("corp_name", corpName);
        paramMap.put("address", address);
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
                logger.info("jd_bi call http error:{}", e.getMessage());
            }
        }
        return result;
    }

    public String start(JdTradesHttp jdTradesHttp) throws Exception {
        String result = "";
        Future<String> submit = threadPoolExecutor.submit(jdTradesHttp);
        String get = submit.get();
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
