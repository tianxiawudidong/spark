package com.ifchange.spark.bi.bean.tob;

import java.io.Serializable;

public class IdwTobBaseIndustryIncre implements Serializable {

    private Long resume_id;

    private String wid;

    private Integer pindustry_id;

    private int depth;

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

    public String getUpdated_at() {
        return updated_at;
    }

    public void setUpdated_at(String updated_at) {
        this.updated_at = updated_at;
    }

    public Integer getPindustry_id() {
        return pindustry_id;
    }

    public void setPindustry_id(Integer pindustry_id) {
        this.pindustry_id = pindustry_id;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }
}
