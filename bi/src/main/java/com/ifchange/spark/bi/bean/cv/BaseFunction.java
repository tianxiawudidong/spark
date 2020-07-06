package com.ifchange.spark.bi.bean.cv;

import java.io.Serializable;

public class BaseFunction implements Serializable {

    private Long resume_id;

    private String wid;

    private Integer function_id;

    private Double value;

    private String updated_at;

    public Long getResume_id() {
        return resume_id;
    }

    public void setResume_id(Long resume_id) {
        this.resume_id = resume_id;
    }

    public String getWid() {
        return wid;
    }

    public void setWid(String wid) {
        this.wid = wid;
    }

    public Integer getFunction_id() {
        return function_id;
    }

    public void setFunction_id(Integer function_id) {
        this.function_id = function_id;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public String getUpdated_at() {
        return updated_at;
    }

    public void setUpdated_at(String updated_at) {
        this.updated_at = updated_at;
    }
}
