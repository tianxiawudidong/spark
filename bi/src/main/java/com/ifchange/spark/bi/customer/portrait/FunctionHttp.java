package com.ifchange.spark.bi.customer.portrait;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class FunctionHttp {

    private static final Logger LOG = LoggerFactory.getLogger(FunctionHttp.class);

    private static String url = "http://nlp.rpc/function_tag";
    //private static String url = "http://192.168.1.211:51666/function_tag";

    private static String jsonFunction = "{\n" +
        "    \"header\":{\n" +
        "        \"log_id\":\"0x666\",\n" +
        "        \"user_ip\":\"192.168.8.52\",\n" +
        "        \"uid\":\"0x666\",\n" +
        "        \"product_name\":\"algo_survey\",\n" +
        "        \"provider\":\"algo_survey\"\n" +
        "    },\n" +
        "    \"request\":{\n" +
        "        \"p\":{\n" +
        "            \"allow_multiple_results\":false, \n" +
        "            \"position\":{\n" +
        "                \"id\":\"0\",\n" +
        "                \"requirement\":\"\",\n" +
        "                \"name\":\"${name}\",\n" +
        "                \"description\":\"${description}\"\n" +
        "            }\n" +
        "        },\n" +
        "        \"c\":\"jd_tag\",\n" +
        "        \"m\":\"get_jd_tags\"\n" +
        "    }\n" +
        "}";

    private HttpClientUtil httpClientUtil;


    public FunctionHttp() {
        this.httpClientUtil = HttpClientUtil.getInstance();
    }

    public Object[] getFunction(String name, String description) {

        jsonFunction.replace("${name}", name);
        jsonFunction.replace("${description}", description);

        Map<String, Object> requestMap = JsonUtils.fromJson(jsonFunction, Map.class);
        Map<String, Object> request = (Map<String, Object>) requestMap.get("request");
        Map<String, Object> position = (Map<String, Object>) ((Map) request.get("p")).get("position");
        position.put("name", name);
        position.put("description", description);

        try {
            String responseBody = httpClientUtil.doPostJson(url, requestMap);
            LOG.info("result:{}",responseBody);
            return getFunctionId(responseBody);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new Object[]{-1, "-"};
    }

    private static Object[] getFunctionId(String body) {
        Object[] result = new Object[]{-1, "-"};
        try {
            Integer functionId;
            Map<String, Object> functionMap = JsonUtils.fromJson(body, Map.class);
            Object o = ((Map) ((Map) functionMap.get("response")).get("results")).get("ref_zhineng");
            if (o != null && o instanceof List && ((List) o).size() > 0) {
                Map map = (Map) ((List) o).get(0);
                functionId = (Integer) map.get("function");
                String functionName = map.get("function_name").toString();
                result = new Object[]{functionId, functionName};
            }
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            return new Object[]{-1, "-"};
        }
    }

    public static void main(String[] args) {
        FunctionHttp functionHttp = new FunctionHttp();
        System.out.println(functionHttp.getFunction("java开发工程师", "1. 熟悉Java开发。 2. 熟悉Spring")[1]);
    }

}
