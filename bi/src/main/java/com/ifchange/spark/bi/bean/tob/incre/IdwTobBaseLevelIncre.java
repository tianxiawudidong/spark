package com.ifchange.spark.bi.bean.tob.incre;

import java.io.Serializable;

public class IdwTobBaseLevelIncre implements Serializable {

    private long resume_id;

    private String wid;

    private int level;

    private String updated_at;

    private String day;

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public long getResume_id() {
        return resume_id;
    }

    public void setResume_id(long resume_id) {
        this.resume_id = resume_id;
    }

    public String getWid() {
        return wid;
    }

    public void setWid(String wid) {
        this.wid = wid;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public String getUpdated_at() {
        return updated_at;
    }

    public void setUpdated_at(String updated_at) {
        this.updated_at = updated_at;
    }
}
