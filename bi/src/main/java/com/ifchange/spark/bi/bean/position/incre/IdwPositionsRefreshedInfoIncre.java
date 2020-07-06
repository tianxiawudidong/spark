package com.ifchange.spark.bi.bean.position.incre;

import java.io.Serializable;

public class IdwPositionsRefreshedInfoIncre implements Serializable {

    private long position_id;

    private String refreshed_info;

    private String day;

    public long getPosition_id() {
        return position_id;
    }

    public void setPosition_id(long position_id) {
        this.position_id = position_id;
    }

    public String getRefreshed_info() {
        return refreshed_info;
    }

    public void setRefreshed_info(String refreshed_info) {
        this.refreshed_info = refreshed_info;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }
}
