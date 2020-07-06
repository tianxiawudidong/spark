package com.ifchange.spark.bi.bean.position.incre;

import java.io.Serializable;

public class IdwPositionsIndustryTempIncre implements Serializable {

    private long position_id;

    private long industry_id;

    private String day;

    public long getPosition_id() {
        return position_id;
    }

    public void setPosition_id(long position_id) {
        this.position_id = position_id;
    }

    public long getIndustry_id() {
        return industry_id;
    }

    public void setIndustry_id(long industry_id) {
        this.industry_id = industry_id;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }
}
