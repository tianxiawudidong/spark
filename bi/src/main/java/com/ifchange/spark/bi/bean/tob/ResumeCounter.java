package com.ifchange.spark.bi.bean.tob;

import java.io.Serializable;

public class ResumeCounter implements Serializable {

    private String id;

    private String data;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
