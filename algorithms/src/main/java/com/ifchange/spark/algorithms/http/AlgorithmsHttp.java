package com.ifchange.spark.algorithms.http;


import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class AlgorithmsHttp {

    protected Map<String, String> headerMap = new LinkedHashMap<>();

    protected Map<String, Object> requestMap = new LinkedHashMap<>();

    protected Map<String, Object> msgMap = new LinkedHashMap<>();

    protected AlgorithmsHttp() {
        this.headerMap.put("session_id", "0");
        this.headerMap.put("log_id", "");
        this.headerMap.put("product_name", "data-hub");
        this.headerMap.put("uid", "0");

        try {
            this.headerMap.put("user_ip", InetAddress.getLocalHost().getHostAddress());
            this.headerMap.put("local_ip", InetAddress.getLocalHost().getHostAddress());
        } catch (Exception e) {
            e.printStackTrace();
        }

        this.msgMap.put("header", this.headerMap);
        this.msgMap.put("request", this.requestMap);
    }
}
