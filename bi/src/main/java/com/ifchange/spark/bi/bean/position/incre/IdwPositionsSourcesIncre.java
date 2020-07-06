package com.ifchange.spark.bi.bean.position.incre;

import java.io.Serializable;

public class IdwPositionsSourcesIncre implements Serializable {

    private long position_id;

    private long source;

    private String source_id;

    private String day;

    public long getPosition_id() {
        return position_id;
    }

    public void setPosition_id(long position_id) {
        this.position_id = position_id;
    }

    public long getSource() {
        return source;
    }

    public void setSource(long source) {
        this.source = source;
    }

    public String getSource_id() {
        return source_id;
    }

    public void setSource_id(String source_id) {
        this.source_id = source_id;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }
}
