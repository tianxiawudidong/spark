package com.ifchange.spark.bi.bean.tob;

import java.io.Serializable;

public class IdwTobBaseIndustryTempIncre implements Serializable {

    private Long resume_id;

    private String wid;

    private Integer industry_id;

    private String updated_at;

    private String day;

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

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

    public Integer getIndustry_id() {
        return industry_id;
    }

    public void setIndustry_id(Integer industry_id) {
        this.industry_id = industry_id;
    }

    public String getUpdated_at() {
        return updated_at;
    }

    public void setUpdated_at(String updated_at) {
        this.updated_at = updated_at;
    }
}
