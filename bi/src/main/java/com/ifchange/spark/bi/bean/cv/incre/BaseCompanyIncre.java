package com.ifchange.spark.bi.bean.cv.incre;

import java.io.Serializable;

public class BaseCompanyIncre implements Serializable {

    private Long resume_id;

    private String wid;

    private  Integer company_id;

    private String updated_at;

    private String day;

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

    public Integer getCompany_id() {
        return company_id;
    }

    public void setCompany_id(Integer company_id) {
        this.company_id = company_id;
    }

    public String getUpdated_at() {
        return updated_at;
    }

    public void setUpdated_at(String updated_at) {
        this.updated_at = updated_at;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }
}
