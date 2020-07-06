package com.ifchange.spark.bi.customer.portrait;

import java.util.HashMap;
import java.util.Map;

public class CorpHttp {

    private static String url = "http://algo.rpc/corp_tag";
    //private static String url = "http://192.168.1.211:51699/corp_tag";

    private HttpClientUtil httpClientUtil;

    public CorpHttp(){
        httpClientUtil = HttpClientUtil.getInstance();
    }

    public Object[] getCorp(String corporationName) {
        HashMap<String, Object> req = new HashMap<>();
        HashMap<String, Object> header = new HashMap<>();
        HashMap<String, Object> request = new HashMap<>();
        request.put("c", "corp_tag_simple");
        request.put("m", "corp_tag_by_name");
        HashMap<String, Object> p = new HashMap<>();
        request.put("p", p);
        p.put("corp_name", corporationName);
        req.put("header", header);
        req.put("request", request);

        //String jsonStr = JsonUtils.toJson(req);

        String responseBody = null;
        try {
            responseBody = httpClientUtil.doPostJson(url, req);

            Map map = JsonUtils.fromJson(responseBody, Map.class);
            Object o = ((Map) map.get("response")).get("result");
            if (o == null || !(o instanceof Map)) {
                return new Object[]{-1, "-"};
            }
            try {
                Object company_name = ((Map) o).get("company_name");
                Object company_id = ((Map) o).get("company_id");
                return new Object[]{Integer.valueOf(company_id.toString()), company_name};
            } catch (Throwable throwable) {
                return new Object[]{-1, "-"};
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new Object[]{-1, "-"};
    }

    public static void main(String[] args){
        CorpHttp corpHttp = new CorpHttp();
        System.out.println(corpHttp.getCorp("阿里巴巴")[1]);
    }

}
