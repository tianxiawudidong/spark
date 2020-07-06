package com.ifchange.spark.bi.customer.portrait;

import java.util.Map;

public class GsystemHttp {

    private static String url = "gsystem.rpc/gsystem_basic";
    //private static String url ="http://192.168.1.28:18080/gsystem_basic";

    private String jsonIndustry = "{\n" +
            "    \"header\": {\n" +
            "        \"uname\": \"zyh\",\n" +
            "        \"version\": 1,\n" +
            "        \"uid\": 0,\n" +
            "        \"log_id\": \"xxfdfrere\",\n" +
            "        \"local_ip\": \"172.17.0.3\",\n" +
            "        \"appid\": \"19\",\n" +
            "        \"user_ip\": \"180.155.180.86\",\n" +
            "        \"provider\": \"toh\",\n" +
            "        \"req_unique\": null,\n" +
            "        \"ip\": \"192.168.8.2\",\n" +
            "        \"signid\": null,\n" +
            "        \"product_name\": \"toh_web\"\n" +
            " \n" +
            "    },\n" +
            "    \"request\": {\n" +
            "        \"w\": \"gsystem_basic\",\n" +
            "        \"c\": \"Logic_industry\",\n" +
            "        \"m\": \"detail\",\n" +
            "        \"p\": {\n" +
            "            \"id\": ${id},\n" +
            "            \"selected\": \"industry{name,weight,depth}\"\n" +
            "        }\n" +
            "    }\n" +
            "}";

    private HttpClientUtil httpClientUtil;

    public GsystemHttp(){
        httpClientUtil = HttpClientUtil.getInstance();
    }

    public String getIndustry(int id) {
        if (id <= 0) {
            return "-";
        }
        String jsonStr = jsonIndustry.replace("${id}", String.valueOf(id));
        try {
            String responseBody = httpClientUtil.doPostJson(url, jsonStr);
            Map<String, Object> result = JsonUtils.fromJson(responseBody, Map.class);
            return ((Map) ((Map) ((Map) result.get("response")).get("results")).get("industry")).get("name").toString();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public static void main(String[] args){
        GsystemHttp gsystemHttp = new GsystemHttp();
        System.out.println(gsystemHttp.getIndustry(2));
    }

}
