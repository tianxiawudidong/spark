package com.ifchange.spark.bi.bean.position;

import java.io.Serializable;

public class IdwPositionsWorkCity implements Serializable {

    private long position_id;

    private String city_id;

    public long getPosition_id() {
        return position_id;
    }

    public void setPosition_id(long position_id) {
        this.position_id = position_id;
    }

    public String getCity_id() {
        return city_id;
    }

    public void setCity_id(String city_id) {
        this.city_id = city_id;
    }
}
